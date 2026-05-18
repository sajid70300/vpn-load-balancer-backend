"""
Celery tasks for VPN monitoring.
Each VPNServer row is now one unified physical server (both protocols on same row).
OpenVPN management port monitoring updates that single row.
Shadowsocks sessions are managed via the /v2/connection_feedback/ and
/v2/shadowsocks/disconnect/ endpoints — no separate Celery task needed for SS.
"""

import socket
import time
import requests
from datetime import datetime
from sqlalchemy import select, and_, update, delete
from sqlalchemy.orm import Session
from app.database import SyncSessionLocal
from app.models import VPNServer, VPNUserSession, Notification
from app.config import settings
import redis


redis_client = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)


def get_db_session():
    db = SyncSessionLocal()
    try:
        return db
    except Exception as e:
        db.close()
        raise


def create_notification_if_needed(db: Session, notif_type: str, server: VPNServer, message: str):
    """
    Insert a notification only if no unread notification of the same
    (type, server_ip) already exists — prevents duplicate alerts.
    """
    try:
        existing = db.query(Notification).filter(
            Notification.type      == notif_type,
            Notification.server_ip == server.ip_address,
            Notification.is_read   == False,
        ).first()
        if existing:
            return
        notif = Notification(
            type        = notif_type,
            server_id   = server.id,
            server_name = server.name,
            server_ip   = server.ip_address,
            app_name    = server.app_name,
            message     = message,
            is_read     = False,
        )
        db.add(notif)
    except Exception as e:
        print(f"⚠️ Failed to create notification ({notif_type}): {e}")


def get_openvpn_status(server_ip: str, port: int = 7505) -> str | None:
    """Fetch OpenVPN status with retries and validation."""
    for attempt in range(3):
        try:
            response = ""
            with socket.create_connection((server_ip, port), timeout=20) as sock:
                sock.sendall(b"status\r\n")
                chunks_without_data = 0

                while len(response) < 500000:
                    try:
                        data = sock.recv(16384).decode(errors='ignore')
                        if not data:
                            chunks_without_data += 1
                            if chunks_without_data >= 3:
                                break
                            time.sleep(0.1)
                            continue

                        chunks_without_data = 0
                        response += data

                        if "ROUTING TABLE" in response and "END" in response:
                            if len(response) > 1000:
                                break

                    except socket.timeout:
                        if len(response) > 100:
                            break
                        raise

                if not validate_openvpn_response(response):
                    print(f"⚠️ Attempt {attempt + 1}: Incomplete response from {server_ip}, retrying...")
                    if attempt < 2:
                        time.sleep(2)
                        continue
                    return None

                return response if len(response) > 50 else None

        except socket.timeout:
            if attempt < 2:
                time.sleep(2)
                continue
        except Exception as e:
            print(f"❌ Attempt {attempt + 1} failed for {server_ip}: {e}")
            if attempt < 2:
                time.sleep(5)

    return None


def validate_openvpn_response(response: str) -> bool:
    """Validate that OpenVPN response is complete."""
    if not response or len(response) < 50:
        return False
    required_sections = ["Common Name", "ROUTING TABLE", "Virtual Address", "GLOBAL STATS"]
    return all(section in response for section in required_sections)


def calculate_load_score(server: VPNServer, session_count: int) -> float:
    """Calculate load score (lower = less loaded = better)."""
    cpu_score = server.cpu_usage * 0.4
    ram_score = server.ram_usage * 0.3
    capacity_score = (session_count / server.max_capacity * 100) * 0.3 if server.max_capacity > 0 else 0
    return round(cpu_score + ram_score + capacity_score, 2)


def sync_server_sessions(db: Session, server: VPNServer, active_users: dict):
    """
    Sync OpenVPN sessions for a single server row.
    active_users: {(user_id, device_ip): {config_tag, bytes_received, bytes_sent}}
    """
    existing_sessions_result = db.query(VPNUserSession).filter(
        VPNUserSession.server_id == server.id
    ).all()

    existing_sessions = {
        (s.user_id, s.device_ip): s
        for s in existing_sessions_result
        if s.protocol == 'openvpn'
    }

    new_user_sessions = []

    for (user_id, device_ip), user_data in active_users.items():
        config_tag     = user_data.get('config_tag')
        bytes_received = user_data.get('bytes_received', 0)
        bytes_sent     = user_data.get('bytes_sent', 0)

        if (user_id, device_ip) not in existing_sessions:
            new_user_sessions.append(
                VPNUserSession(
                    server_id      = server.id,
                    user_id        = user_id,
                    device_ip      = device_ip,
                    config_tag     = config_tag,
                    bytes_received = bytes_received,
                    bytes_sent     = bytes_sent,
                    connected_time = datetime.utcnow(),
                    protocol       = 'openvpn',
                )
            )
        else:
            db.query(VPNUserSession).filter(
                VPNUserSession.server_id == server.id,
                VPNUserSession.user_id   == user_id,
                VPNUserSession.device_ip == device_ip
            ).update({
                'bytes_received': bytes_received,
                'bytes_sent':     bytes_sent,
            })

    # Handle disconnected users (only remove openvpn sessions)
    disconnected_keys = set(existing_sessions.keys()) - set(active_users.keys())
    if disconnected_keys:
        for user_id, device_ip in disconnected_keys:
            db.query(VPNUserSession).filter(
                VPNUserSession.server_id == server.id,
                VPNUserSession.user_id   == user_id,
                VPNUserSession.device_ip == device_ip,
                VPNUserSession.protocol  == 'openvpn',
            ).delete()
        print(f"   ❌ {len(disconnected_keys)} OpenVPN user(s) disconnected from {server.name}")

    if new_user_sessions:
        db.bulk_save_objects(new_user_sessions)
        print(f"   ✅ {len(new_user_sessions)} new OpenVPN user(s) added for {server.name}")

    # Update peak users (count all sessions: openvpn + shadowsocks)
    current_users = db.query(VPNUserSession).filter(
        VPNUserSession.server_id == server.id
    ).count()
    if current_users > server.peak_users:
        server.peak_users      = current_users
        server.peak_users_time = datetime.utcnow()


def process_server_group(server_ids: list, inactive_retry_seconds: int = 600):
    """
    Process a group of VPNServer rows that share the same physical machine
    (same ip:management_port).

    Connects to the OpenVPN management port ONCE, parses the response ONCE,
    then distributes users to ALL rows in the group — each row applying its
    own config_tag / cn_match filter independently.

    This correctly handles:
    - Same machine in multiple apps
    - Same machine as free + premium in the same app
    - Any combination of the above
    """
    db = get_db_session()
    try:
        # Load all server rows in this group upfront
        servers = db.query(VPNServer).filter(VPNServer.id.in_(server_ids)).all()
        if not servers:
            return

        # Use first row to get ip:port (all rows in group share the same values)
        primary    = servers[0]
        ip_address      = primary.ip_address
        management_port = primary.management_port

        print(f"\n{'='*60}")
        print(f"🔍 PROCESSING: {ip_address}:{management_port} — {len(servers)} row(s)")
        print(f"   Rows: {[s.app_name + '/' + s.server_type for s in servers]}")
        print(f"{'='*60}\n")

        # ── Fetch management port ONCE ────────────────────────────────────
        response = get_openvpn_status(ip_address, management_port)

        if response is None:
            # Set a cooldown so this group is skipped for the next 10 minutes
            cooldown_key = f"inactive_retry:{ip_address}:{management_port}"
            redis_client.set(cooldown_key, "1", ex=inactive_retry_seconds)
            # Mark ALL rows in group as inactive and clear their sessions
            for server in servers:
                if server.is_active:
                    create_notification_if_needed(
                        db, 'server_down', server,
                        f"Server '{server.name}' ({server.ip_address}) is unreachable and has been marked inactive."
                    )
                server.is_active = False
                db.query(VPNUserSession).filter(
                    VPNUserSession.server_id == server.id,
                    VPNUserSession.protocol  == 'openvpn',
                ).delete(synchronize_session=False)
            db.commit()
            print(f"❌ {ip_address}:{management_port} is DOWN — {len(servers)} row(s) marked inactive.")
            return

        lines = response.split("\n")

        # ── Phase 1: Parse CLIENT LIST for bandwidth (ONCE) ──────────────
        client_bandwidth = {}
        reading_clients = False
        for line in lines:
            if "Common Name,Real Address,Bytes Received,Bytes Sent,Connected Since" in line:
                reading_clients = True
                continue
            if "ROUTING TABLE" in line:
                break
            if reading_clients and line.strip():
                parts = line.split(",")
                if len(parts) >= 5:
                    raw_cn       = parts[0]
                    real_address = parts[1]
                    device_ip    = real_address.split(':', 1)[1].split(":")[0] if real_address.startswith(('udp4:', 'udp6:', 'tcp4:', 'tcp6:')) else real_address.split(":")[0]
                    try:
                        bytes_received = int(parts[2])
                        bytes_sent     = int(parts[3])
                    except ValueError:
                        bytes_received = bytes_sent = 0
                    client_bandwidth[(raw_cn, device_ip)] = {
                        'bytes_received': bytes_received,
                        'bytes_sent':     bytes_sent,
                    }

        print(f"📊 Phase 1: Stored bandwidth for {len(client_bandwidth)} entries")

        # ── Phase 2: Parse ROUTING TABLE (ONCE) ──────────────────────────
        all_routing_entries = []
        reading_routing = False
        for line in lines:
            if "ROUTING TABLE" in line:
                reading_routing = True
                continue
            if "GLOBAL STATS" in line or "END" in line:
                break
            if reading_routing and line.strip() and not line.startswith("Virtual Address"):
                parts = line.split(",")
                if len(parts) >= 3:
                    raw_cn       = parts[1].strip()
                    real_address = parts[2].strip()
                    if raw_cn != "UNDEF":
                        all_routing_entries.append({
                            'virtual_ip':   parts[0].strip(),
                            'raw_cn':       raw_cn,
                            'real_address': real_address,
                            'device_ip':    real_address.split(':', 1)[1].split(":")[0] if real_address.startswith(('udp4:', 'udp6:', 'tcp4:', 'tcp6:')) else real_address.split(":")[0],
                        })

        print(f"📊 Phase 2: Parsed {len(all_routing_entries)} entries from ROUTING TABLE")

        # ── Validation (based on primary row's current session count) ────
        if len(client_bandwidth) > 10 and len(all_routing_entries) == 0:
            print(f"⚠️ VALIDATION FAILED: CLIENT LIST has entries but ROUTING TABLE is empty!")
            return

        current_session_count = db.query(VPNUserSession).filter(
            VPNUserSession.server_id == primary.id,
            VPNUserSession.protocol  == 'openvpn',
        ).count()
        if current_session_count > 10 and len(all_routing_entries) < (current_session_count * 0.3):
            print(f"⚠️ VALIDATION FAILED: Suspicious drop ({current_session_count} → {len(all_routing_entries)})")
            return

        if len(client_bandwidth) > 0 and len(all_routing_entries) > 0:
            ratio = len(all_routing_entries) / len(client_bandwidth)
            if ratio < 0.5:
                print(f"⚠️ VALIDATION FAILED: CLIENT LIST / ROUTING TABLE mismatch (ratio {ratio:.2%})")
                return

        print(f"✅ Validation passed")

        # ── Phase 3: Distribute users to ALL rows in group (ONCE per row) ─
        for server in servers:
            active_users  = {}
            matched_count = 0
            skipped_count = 0

            for entry in all_routing_entries:
                raw_cn    = entry['raw_cn']
                user_id   = raw_cn
                device_ip = entry['device_ip']

                config_tag = None
                if "@" in user_id:
                    actual_user, possible_tag = user_id.rsplit("@", 1)
                    if possible_tag:
                        config_tag = possible_tag[:50]
                        user_id    = actual_user

                # Apply this row's own config_tag / cn_match filter
                if server.config_tag or server.cn_match:
                    tag_match_ok = (not server.config_tag) or (config_tag == server.config_tag)
                    cn_match_ok  = (server.cn_match and server.cn_match.lower() in raw_cn.lower())
                    if not (tag_match_ok or cn_match_ok):
                        skipped_count += 1
                        continue

                matched_count += 1
                bandwidth_key = (raw_cn, device_ip)
                bandwidth     = client_bandwidth.get(bandwidth_key, {'bytes_received': 0, 'bytes_sent': 0})

                active_users[(user_id, device_ip)] = {
                    'config_tag':     config_tag,
                    'bytes_received': bandwidth['bytes_received'],
                    'bytes_sent':     bandwidth['bytes_sent'],
                }

            server.is_active = True
            # Server recovered — clear any inactive cooldown
            cooldown_key = f"inactive_retry:{ip_address}:{management_port}"
            redis_client.delete(cooldown_key)
            sync_server_sessions(db, server, active_users)
            print(f"✅ {server.name} ({server.app_name}/{server.server_type}): "
                  f"{len(active_users)} users (matched: {matched_count}, skipped: {skipped_count})")

        db.commit()
        print(f"{'='*60}\n")

    except Exception as e:
        db.rollback()
        print(f"❌ Error processing group {server_ids}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


def fetch_server_metrics(server_id: int):
    """
    Fetch CPU/RAM/ping metrics from the monitoring API for a single server.
    Updates the VPNServer row directly.
    If the API fails (timeout, error, non-200), a cooldown key is set in Redis
    so the URL is skipped for METRICS_API_RETRY_MINUTES minutes.
    """
    db = get_db_session()
    try:
        server = db.query(VPNServer).filter(VPNServer.id == server_id).first()
        if not server or not server.monitoring_api_url:
            return

        cooldown_key  = f"metrics_retry:{server.monitoring_api_url}"
        retry_seconds = settings.METRICS_API_RETRY_MINUTES * 60

        try:
            response = requests.get(f"{server.monitoring_api_url}/metrics", timeout=5)
            if response.status_code == 200:
                data         = response.json()
                current_time = datetime.utcnow()
                cpu_usage    = data.get('cpu_percent', 0.0)
                ram_usage    = data.get('ram_percent', 0.0)
                ping_ms      = data.get('ping_ms', 0.0)

                server.cpu_usage         = cpu_usage
                server.ram_usage         = ram_usage
                server.ping_latency_ms   = ping_ms
                server.last_health_check = current_time

                if cpu_usage > server.peak_cpu:
                    server.peak_cpu      = cpu_usage
                    server.peak_cpu_time = current_time

                if ram_usage > server.peak_ram:
                    server.peak_ram      = ram_usage
                    server.peak_ram_time = current_time

                # Load score uses all sessions on this server row
                session_count = db.query(VPNUserSession).filter(
                    VPNUserSession.server_id == server.id
                ).count()
                server.load_score = calculate_load_score(server, session_count)

                if server.max_capacity > 0 and session_count >= server.max_capacity:
                    create_notification_if_needed(
                        db, 'capacity_reached', server,
                        f"Server '{server.name}' ({server.ip_address}) has reached its maximum "
                        f"capacity of {server.max_capacity} sessions ({session_count} active)."
                    )

                db.commit()
                # API responded successfully — clear any existing cooldown
                redis_client.delete(cooldown_key)
                print(f"✅ {server.name}: CPU={cpu_usage:.1f}% RAM={ram_usage:.1f}% Ping={ping_ms:.1f}ms")
            else:
                # Non-200 response — set cooldown
                redis_client.set(cooldown_key, "1", ex=retry_seconds)
                print(f"⚠️ Failed to fetch metrics for {server.name}: HTTP {response.status_code} — cooldown set for {settings.METRICS_API_RETRY_MINUTES}m")

        except requests.exceptions.RequestException as e:
            # Timeout or connection error — set cooldown
            redis_client.set(cooldown_key, "1", ex=retry_seconds)
            print(f"❌ Error fetching metrics for {server.name}: {e} — cooldown set for {settings.METRICS_API_RETRY_MINUTES}m")

    finally:
        db.close()


def monitor_vpn_status():
    """
    Main task to monitor VPN servers via OpenVPN management port.

    Groups all VPNServer rows by ip:management_port so the management port
    is only contacted ONCE per physical machine. The parsed response is then
    applied to ALL rows sharing that ip:port (e.g. same machine finalized as
    both free and premium in the same or different apps).
    """
    try:
        queue_length = redis_client.llen('celery')
        if queue_length > 50:
            print(f"⚠️ Queue backlog detected ({queue_length} tasks), skipping this cycle")
            return
    except Exception as e:
        print(f"⚠️ Could not check queue length: {e}")

    lock_id      = "monitor_vpn_status_lock"
    lock_timeout = 30

    if not redis_client.set(lock_id, "locked", nx=True, ex=lock_timeout):
        print("⚠️ Previous monitoring task still running, skipping this cycle")
        return

    try:
        db = get_db_session()
        try:
            vpn_servers = db.query(VPNServer).all()
            print(f"📊 Monitoring {len(vpn_servers)} server(s) (active + inactive for auto-recovery)...")

            # Group rows by ip:management_port — one physical machine may have
            # multiple VPNServer rows (e.g. free + premium, or multiple apps).
            # We hit the management port ONCE and sync sessions into ALL rows.
            from collections import defaultdict
            groups: dict = defaultdict(list)
            for server in vpn_servers:
                key = f"{server.ip_address}:{server.management_port}"
                groups[key].append(server.id)

            INACTIVE_RETRY_SECONDS = settings.INACTIVE_SERVER_RETRY_MINUTES * 60

            for key, server_ids in groups.items():
                cooldown_key = f"inactive_retry:{key}"
                if redis_client.exists(cooldown_key):
                    ttl = redis_client.ttl(cooldown_key)
                    print(f"⏳ Skipping {key} — inactive cooldown active ({ttl}s remaining)")
                    continue
                print(f"🔄 Processing {key} — {len(server_ids)} row(s)")
                process_server_group(server_ids, inactive_retry_seconds=INACTIVE_RETRY_SECONDS)

        finally:
            db.close()
    finally:
        redis_client.delete(lock_id)


def monitor_all_server_metrics():
    """
    Main task to fetch CPU/RAM/ping metrics from all monitoring API endpoints.
    """
    try:
        queue_length = redis_client.llen('celery')
        if queue_length > 50:
            print(f"⚠️ Queue backlog detected ({queue_length} tasks), skipping metrics cycle")
            return
    except Exception as e:
        print(f"⚠️ Could not check queue length: {e}")

    lock_id      = "monitor_metrics_lock"
    lock_timeout = 15

    if not redis_client.set(lock_id, "locked", nx=True, ex=lock_timeout):
        print("⚠️ Previous metrics task still running, skipping this cycle")
        return

    try:
        db = get_db_session()
        try:
            servers = db.query(VPNServer).filter(
                VPNServer.is_active == True,
                VPNServer.monitoring_api_url.isnot(None),
                VPNServer.monitoring_api_url != ''
            ).all()

            # Deduplicate by monitoring_api_url so each physical machine is fetched once
            seen_urls: set = set()
            skipped_urls: set = set()
            for server in servers:
                url = server.monitoring_api_url
                if url in seen_urls or url in skipped_urls:
                    continue
                cooldown_key = f"metrics_retry:{url}"
                if redis_client.exists(cooldown_key):
                    ttl = redis_client.ttl(cooldown_key)
                    print(f"⏳ Skipping metrics for {url} — cooldown active ({ttl}s remaining)")
                    skipped_urls.add(url)
                    continue
                seen_urls.add(url)
                fetch_server_metrics(server.id)

            print(f"📊 Fetched metrics for {len(seen_urls)} unique monitoring API(s), skipped {len(skipped_urls)}")
        finally:
            db.close()
    finally:
        redis_client.delete(lock_id)


def cleanup_stale_shadowsocks_sessions():
    """
    Removes Shadowsocks sessions older than 1 hour without a disconnect signal.
    Runs as a safety net for users who disconnect without hitting the endpoint.
    OpenVPN sessions are intentionally untouched.
    """
    from datetime import timedelta

    SHADOWSOCKS_SESSION_TTL_SECONDS = 3600

    db = get_db_session()
    try:
        cutoff = datetime.utcnow() - timedelta(seconds=SHADOWSOCKS_SESSION_TTL_SECONDS)

        stale_sessions = db.query(VPNUserSession).filter(
            VPNUserSession.protocol      == 'shadowsocks',
            VPNUserSession.connected_time < cutoff
        ).all()

        if stale_sessions:
            count = len(stale_sessions)
            for session in stale_sessions:
                db.delete(session)
            db.commit()
            print(f"🧹 Cleaned up {count} stale Shadowsocks session(s) older than 1 hour")
        else:
            print("✅ No stale Shadowsocks sessions to clean up")

    except Exception as e:
        db.rollback()
        print(f"❌ Error cleaning up stale Shadowsocks sessions: {e}")
    finally:
        db.close()


def update_geoip_databases():
    """
    Downloads fresh GeoLite2-Country and GeoLite2-ASN .mmdb files from MaxMind
    and replaces the existing ones in place.

    MaxMind releases database updates every Tuesday.
    This task should be scheduled to run weekly (e.g. every Tuesday at 3am UTC).

    Requires in .env:
        MAXMIND_ACCOUNT_ID   - your MaxMind account ID
        MAXMIND_LICENSE_KEY  - your MaxMind license key
        GEOIP_COUNTRY_PATH   - full path to GeoLite2-Country.mmdb
        GEOIP_ASN_PATH       - full path to GeoLite2-ASN.mmdb
    """
    import requests
    import tarfile
    import shutil
    import tempfile
    import os
    from app.config import settings

    EDITIONS = [
        {
            "edition_id": "GeoLite2-Country",
            "dest_path": settings.GEOIP_COUNTRY_PATH,
            "mmdb_filename": "GeoLite2-Country.mmdb",
        },
        {
            "edition_id": "GeoLite2-ASN",
            "dest_path": settings.GEOIP_ASN_PATH,
            "mmdb_filename": "GeoLite2-ASN.mmdb",
        },
    ]

    account_id  = getattr(settings, "MAXMIND_ACCOUNT_ID", None)
    license_key = getattr(settings, "MAXMIND_LICENSE_KEY", None)

    if not account_id or not license_key:
        print("❌ GeoIP update skipped: MAXMIND_ACCOUNT_ID or MAXMIND_LICENSE_KEY not set in .env")
        return

    print("🌍 Starting GeoIP database update...")

    for edition in EDITIONS:
        edition_id   = edition["edition_id"]
        dest_path    = edition["dest_path"]
        mmdb_filename = edition["mmdb_filename"]

        url = (
            f"https://download.maxmind.com/geoip/databases/{edition_id}/download"
            f"?suffix=tar.gz"
        )

        try:
            print(f"⬇️  Downloading {edition_id}...")
            response = requests.get(
                url,
                auth=(str(account_id), license_key),
                timeout=60,
                stream=True,
            )

            if response.status_code != 200:
                print(f"❌ Failed to download {edition_id}: HTTP {response.status_code}")
                continue

            # Write to a temp file
            with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp_file:
                tmp_path = tmp_file.name
                for chunk in response.iter_content(chunk_size=8192):
                    tmp_file.write(chunk)

            # Extract the .mmdb from the tar.gz
            with tempfile.TemporaryDirectory() as extract_dir:
                with tarfile.open(tmp_path, "r:gz") as tar:
                    tar.extractall(extract_dir)

                # Find the .mmdb file inside extracted folder
                mmdb_found = None
                for root, dirs, files in os.walk(extract_dir):
                    for f in files:
                        if f == mmdb_filename:
                            mmdb_found = os.path.join(root, f)
                            break

                if not mmdb_found:
                    print(f"❌ Could not find {mmdb_filename} in downloaded archive")
                    continue

                # Ensure destination directory exists
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)

                # Replace the existing .mmdb file
                shutil.move(mmdb_found, dest_path)
                print(f"✅ {edition_id} updated → {dest_path}")

            os.unlink(tmp_path)

        except Exception as e:
            print(f"❌ Error updating {edition_id}: {e}")

    print("🌍 GeoIP database update complete.")