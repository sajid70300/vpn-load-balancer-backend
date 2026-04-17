"""
Intelligent Decision Engine for VPN Protocol Selection

Two-phase selection:
  Phase 1 — Pick the best server row by combined load score:
             CPU, RAM, ping, and active sessions vs max_capacity.
             Servers in active cooldown for the requesting country+ASN are skipped.

  Phase 2 — On the chosen server, determine primary/fallback protocol:

    Policy-first (when enforce_country_policies or enforce_isp_policies is ON):
      1. If an active ISP policy exists for (country + ASN)  → use it directly.
         The policy's preferred protocol becomes primary, the other becomes fallback.
         A 'blocked' ISP protocol is excluded entirely.
      2. Else if an active Country policy exists for (country) → use it directly.
         preferred_protocol = primary, fallback_protocol = fallback.
      Auto scoring is SKIPPED when a policy covers the user's context.

    Auto scoring (no matching policy, or toggles are OFF):
      Score each protocol independently:
        • Success rate (country+ASN+network specific)  — 70%
        • Average connect time                         — 30%
      Higher score = primary, lower = fallback.

    ISP takes precedence over Country when both exist.
    Both protocols come from the SAME server row (same server_id).

Cooldown design (Redis-backed):
  • Triggered only when BOTH protocols fail for a server.
  • Two levels: soft → hard.
  • Scoped first to server + country + ASN.
  • If ≥ N distinct ASNs from the same country have an active cooldown on the
    same server, the entire country is blocked on that server.
  • N = GlobalSettings.cooldown_country_block_asn_threshold (default 3).

Redis key scheme:
  cooldown:asn:<server_ip>:<country>:<asn>   → level ("soft"|"hard")
  cooldown:country:<server_ip>:<country>     → level ("soft"|"hard")
  cooldown:asn_set:<server_ip>:<country>     → Redis Set of failed ASNs
"""

from typing import Optional, List, Dict
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_

from app.models import VPNServer, VPNUserSession, ProtocolMetrics, CountryPolicy, ISPPolicy, GlobalSettings
from app.schemas import BestServerDecision, ProtocolConfig
from app.cache import get_cache, set_cache, get_redis

# ── Cache keys ────────────────────────────────────────────────────────────────
SETTINGS_CACHE_KEY = "global_settings"
SETTINGS_CACHE_TTL = 3600

# Protocol tie-break: when scores are equal prefer OpenVPN
PROTOCOL_PREFERENCE_ORDER = ['openvpn', 'shadowsocks']


def _protocol_rank(protocol: str) -> int:
    try:
        return PROTOCOL_PREFERENCE_ORDER.index(protocol)
    except ValueError:
        return 99


class _DictNamespace:
    """Wraps a plain dict so its keys are accessible as attributes."""
    def __init__(self, d: dict):
        self.__dict__.update(d)


# ── Cooldown Redis key builders ───────────────────────────────────────────────

def _cd_asn_key(server_ip: str, country: str, asn: str) -> str:
    return f"cooldown:asn:{server_ip}:{country}:{asn}"


def _cd_country_key(server_ip: str, country: str) -> str:
    return f"cooldown:country:{server_ip}:{country}"


def _cd_asn_set_key(server_ip: str, country: str) -> str:
    return f"cooldown:asn_set:{server_ip}:{country}"


# ─────────────────────────────────────────────────────────────────────────────

class DecisionEngine:
    """Core decision engine — fully deterministic, no randomness."""

    WEIGHT_SUCCESS_RATE  = 0.70
    WEIGHT_CONNECT_SPEED = 0.30

    def __init__(self, db: AsyncSession):
        self.db = db

    # ------------------------------------------------------------------ #
    #  Global settings (Redis-cached)                                      #
    # ------------------------------------------------------------------ #

    async def _load_global_settings(self):
        """Redis-first; DB only on cache miss. admin_settings invalidates on PUT."""
        cached = await get_cache(SETTINGS_CACHE_KEY)
        if cached:
            cached.setdefault("cooldown_country_block_asn_threshold", 3)
            cached.setdefault("cooldown_hard_seconds", 3600)
            return _DictNamespace(cached)

        result = await self.db.execute(
            select(GlobalSettings).where(GlobalSettings.id == 1)
        )
        gs = result.scalar_one_or_none()
        if not gs:
            gs = GlobalSettings(id=1)
            self.db.add(gs)
            await self.db.flush()

        payload = {
            "protocol_mode":                       gs.protocol_mode,
            "disable_new_connections":              gs.disable_new_connections,
            "enforce_country_policies":             gs.enforce_country_policies,
            "enforce_isp_policies":                 gs.enforce_isp_policies,
            "cooldown_soft_seconds":                gs.cooldown_soft_seconds,
            "cooldown_hard_seconds":                gs.cooldown_hard_seconds,
            "failure_rate_threshold":               gs.failure_rate_threshold,
            "cooldown_country_block_asn_threshold": gs.cooldown_country_block_asn_threshold,
        }
        await set_cache(SETTINGS_CACHE_KEY, payload, ttl=SETTINGS_CACHE_TTL)
        return _DictNamespace(payload)

    # ------------------------------------------------------------------ #
    #  Public entry point                                                  #
    # ------------------------------------------------------------------ #

    async def get_best_server(
        self,
        app_name:     str,
        user_country: Optional[str] = None,
        user_asn:     Optional[str] = None,
        network_type: Optional[str] = None,
        server_type:  Optional[str] = None,
    ) -> BestServerDecision:
        """
        Two-phase selection.
        Phase 1: best server by hardware load (cooldown-aware).
        Phase 2: best protocol on that server by success rate + connect speed.
        """
        gs = await self._load_global_settings()

        if gs.disable_new_connections:
            raise ValueError("New connections are currently disabled (maintenance mode)")

        protocol_mode = gs.protocol_mode

        # ── Phase 1: load servers ─────────────────────────────────────────
        servers = await self._load_servers(app_name, server_type)

        if not servers:
            raise ValueError("No active servers available")

        # Filter servers in cooldown (cheap Redis check)
        available = []
        for srv in servers:
            if not await self._server_in_cooldown(srv["server"].ip_address, user_country, user_asn):
                available.append(srv)

        if not available:
            raise ValueError("All servers are in cooldown for your region")

        # Score and sort servers
        scored: List[dict] = []
        for srv in available:
            score = self._server_load_score(srv)
            scored.append({**srv, "load_score_computed": score})

        scored.sort(key=lambda s: (
            -s["load_score_computed"],
            not s["server"].is_priority_group,
            s["server"].load_score,
            s["server"].id,
        ))

        # ── Phase 2: score protocols on best available server ─────────────
        for srv in scored:
            result = await self._score_protocols(
                srv, protocol_mode, user_country, user_asn, network_type
            )
            if result:
                return await self._build_decision_response(result)

        raise ValueError("All servers at capacity")

    async def get_protocol_decision_for_server(
        self,
        ip_address:   str,
        server_type:  Optional[str],
        user_country: Optional[str] = None,
        user_asn:     Optional[str] = None,
        network_type: Optional[str] = None,
    ) -> Optional[BestServerDecision]:
        """
        For a specific physical server (identified by ip_address + server_type),
        score its protocols and return a primary/fallback decision.
        Used by /servers_config/ when the user manually selects a server.
        Returns None if the server has no active rows.
        """
        gs = await self._load_global_settings()
        protocol_mode = gs.protocol_mode

        conditions = [
            VPNServer.is_active  == True,
            VPNServer.ip_address == ip_address,
        ]
        if server_type:
            conditions.append(VPNServer.server_type == server_type)

        query = (
            select(VPNServer, func.count(VPNUserSession.id).label('session_count'))
            .outerjoin(VPNUserSession)
            .where(and_(*conditions))
            .group_by(VPNServer.id)
        )
        rows = (await self.db.execute(query)).all()

        if not rows:
            return None

        server, session_count = rows[0]
        srv = {
            "server":       server,
            "sessions":     session_count,
            "max_capacity": server.max_capacity,
            "cpu_usage":    server.cpu_usage,
            "ram_usage":    server.ram_usage,
            "ping_ms":      server.ping_latency_ms,
            "load_score":   server.load_score,
        }

        result = await self._score_protocols(
            srv, protocol_mode, user_country, user_asn, network_type
        )
        if result is None:
            return None

        return await self._build_decision_response(result)

    # ------------------------------------------------------------------ #
    #  Cooldown check (Redis only)                                         #
    # ------------------------------------------------------------------ #

    async def _server_in_cooldown(
        self,
        server_ip: str,
        country:   Optional[str],
        asn:       Optional[str],
    ) -> bool:
        if not country:
            return False

        redis = await get_redis()

        if await redis.exists(_cd_country_key(server_ip, country)):
            return True

        if asn and await redis.exists(_cd_asn_key(server_ip, country, asn)):
            return True

        return False

    # ------------------------------------------------------------------ #
    #  Phase 1: load servers                                               #
    # ------------------------------------------------------------------ #

    async def _load_servers(
        self,
        app_name:    str,
        server_type: Optional[str],
    ) -> List[dict]:
        """
        Query active VPNServer rows for this app.
        Returns only servers not at or over max_capacity.
        """
        conditions = [
            VPNServer.is_active == True,
            VPNServer.app_name  == app_name,
        ]
        if server_type:
            conditions.append(VPNServer.server_type == server_type)

        query = (
            select(VPNServer, func.count(VPNUserSession.id).label('session_count'))
            .outerjoin(VPNUserSession)
            .where(and_(*conditions))
            .group_by(VPNServer.id)
        )
        result = await self.db.execute(query)
        all_rows = result.all()

        servers = []
        for server, session_count in all_rows:
            if server.max_capacity > 0 and session_count >= server.max_capacity:
                continue
            servers.append({
                "server":       server,
                "sessions":     session_count,
                "max_capacity": server.max_capacity,
                "cpu_usage":    server.cpu_usage,
                "ram_usage":    server.ram_usage,
                "ping_ms":      server.ping_latency_ms,
                "load_score":   server.load_score,
            })

        # Priority servers first
        servers.sort(key=lambda s: not s["server"].is_priority_group)
        return servers

    # ------------------------------------------------------------------ #
    #  Phase 1: server load score                                          #
    # ------------------------------------------------------------------ #

    def _server_load_score(self, srv: dict) -> float:
        """Lower hardware load = higher score. Score 0–100 (100 = fully idle).
        Weights: CPU 35%, RAM 30%, Sessions 25%, Ping 10%"""
        cpu_score = max(0.0, 100.0 - srv["cpu_usage"])
        ram_score = max(0.0, 100.0 - srv["ram_usage"])

        if srv["max_capacity"] > 0:
            load_pct = (srv["sessions"] / srv["max_capacity"]) * 100.0
        else:
            load_pct = srv["cpu_usage"]
        session_score = max(0.0, 100.0 - load_pct)

        ping = srv["ping_ms"]
        if ping <= 0:
            ping_score = 80.0
        elif ping <= 50:
            ping_score = 100.0 - (ping / 50.0) * 20.0
        elif ping <= 150:
            ping_score = 80.0 - ((ping - 50.0) / 100.0) * 40.0
        else:
            ping_score = max(0.0, 40.0 - ((ping - 150.0) / 200.0) * 40.0)

        return round(
            cpu_score     * 0.35 +
            ram_score     * 0.30 +
            session_score * 0.25 +
            ping_score    * 0.10,
            4
        )

    # ------------------------------------------------------------------ #
    #  Phase 2: determine protocols on a server                            #
    # ------------------------------------------------------------------ #

    async def _score_protocols(
        self,
        srv:           dict,
        protocol_mode: str,
        user_country:  Optional[str],
        user_asn:      Optional[str],
        network_type:  Optional[str],
    ) -> Optional[dict]:
        """
        Determine primary/fallback protocol for a server.

        Priority order:
          1. Global force mode (force_openvpn / force_shadowsocks) — always wins.
          2. Policy-first: when enforce toggles are ON and a matching policy exists,
             use it directly — auto scoring is skipped entirely.
             ISP policy (country+ASN) takes precedence over Country policy.
          3. Auto scoring: 70% success rate + 30% connect speed.

        Returns a choice dict or None if nothing usable.
        """
        server = srv["server"]
        gs     = await self._load_global_settings()

        # ── 1. Global force mode ──────────────────────────────────────────
        if protocol_mode in ('force_openvpn', 'force_shadowsocks'):
            primary_proto  = 'openvpn' if protocol_mode == 'force_openvpn' else 'shadowsocks'
            fallback_proto = 'shadowsocks' if primary_proto == 'openvpn' else 'openvpn'
            return {
                "server":            server,
                "primary_protocol":  primary_proto,
                "primary_score":     100.0,
                "fallback_protocol": fallback_proto,
                "fallback_score":    0.0,
                "srv":               srv,
            }

        # ── 2. Policy-first path ──────────────────────────────────────────
        if user_country:
            policy_decision = await self._get_policy_decision(
                country                  = user_country,
                asn                      = user_asn,
                enforce_country_policies = gs.enforce_country_policies,
                enforce_isp_policies     = gs.enforce_isp_policies,
            )
            if policy_decision:
                primary_proto, fallback_proto = policy_decision
                return {
                    "server":            server,
                    "primary_protocol":  primary_proto,
                    "primary_score":     100.0,   # policy is authoritative — score is nominal
                    "fallback_protocol": fallback_proto,
                    "fallback_score":    0.0,
                    "srv":               srv,
                }

        # ── 3. Auto scoring ───────────────────────────────────────────────
        candidates = []
        for proto in ['openvpn', 'shadowsocks']:
            score = await self._calculate_protocol_score(
                server, proto, user_country, user_asn, network_type
            )
            candidates.append({"protocol": proto, "score": score})

        if not candidates:
            return None

        candidates.sort(key=lambda c: (-c["score"], _protocol_rank(c["protocol"])))
        primary  = candidates[0]
        fallback = candidates[1] if len(candidates) > 1 else candidates[0]

        return {
            "server":            server,
            "primary_protocol":  primary["protocol"],
            "primary_score":     primary["score"],
            "fallback_protocol": fallback["protocol"],
            "fallback_score":    fallback["score"],
            "srv":               srv,
        }

    # ------------------------------------------------------------------ #
    #  Policy-first decision lookup                                        #
    # ------------------------------------------------------------------ #

    async def _get_policy_decision(
        self,
        country:                  str,
        asn:                      Optional[str],
        enforce_country_policies: bool,
        enforce_isp_policies:     bool,
    ) -> Optional[tuple]:
        """
        Returns (primary_protocol, fallback_protocol) when a hard policy covers
        this user's context and the relevant toggle is enabled.
        Returns None when no policy applies → caller falls through to auto scoring.

        Precedence:
          1. ISP policy  (country + ASN)  — checked first when enforce_isp_policies ON
          2. Country policy (country)     — checked next when enforce_country_policies ON

        For ISP policies, only 'preferred' and 'blocked' statuses are decisive:
          • preferred → that protocol is primary, other is fallback
          • blocked   → that protocol is excluded; other becomes both primary + fallback
          • degraded  → not a hard policy, falls through to auto scoring

        A 'blocked' ISP policy for a protocol excludes it completely.
        If both protocols are blocked (edge case), return None → auto scoring.
        """
        BOTH = ['openvpn', 'shadowsocks']

        # ── ISP policy check ─────────────────────────────────────────────
        if enforce_isp_policies and asn:
            isp_rows = (await self.db.execute(
                select(ISPPolicy).where(and_(
                    ISPPolicy.country == country,
                    ISPPolicy.asn     == asn,
                ))
            )).scalars().all()

            # Filter out expired policies
            active_isp = [
                r for r in isp_rows
                if not r.expiry or datetime.utcnow() < r.expiry
            ]

            if active_isp:
                preferred_by_isp = [r.protocol for r in active_isp if r.status == 'preferred']
                blocked_by_isp   = [r.protocol for r in active_isp if r.status == 'blocked']

                # Determine available protocols after blocking
                available = [p for p in BOTH if p not in blocked_by_isp]

                if not available:
                    # Both blocked — cannot make a policy decision, fall through
                    return None

                if len(available) == 1:
                    # One protocol blocked → the surviving one is both primary and fallback
                    return (available[0], available[0])

                # Both available — use preferred to pick primary
                if preferred_by_isp:
                    primary  = preferred_by_isp[0]
                    fallback = next(p for p in BOTH if p != primary)
                    return (primary, fallback)

                # ISP rows exist but none are 'preferred' or 'blocked' (all 'degraded')
                # → not a hard policy, fall through to country check
                pass

        # ── Country policy check ─────────────────────────────────────────
        if enforce_country_policies:
            cp = (await self.db.execute(
                select(CountryPolicy).where(and_(
                    CountryPolicy.country   == country,
                    CountryPolicy.is_active == True,
                ))
            )).scalar_one_or_none()

            if cp and cp.preferred_protocol:
                primary  = cp.preferred_protocol
                fallback = cp.fallback_protocol or next(
                    (p for p in BOTH if p != primary), primary
                )
                return (primary, fallback)

        return None

    # ------------------------------------------------------------------ #
    #  Auto scoring: success rate 70% + connect speed 30%                 #
    # ------------------------------------------------------------------ #

    async def _calculate_protocol_score(
        self,
        server:       VPNServer,
        protocol:     str,
        user_country: Optional[str],
        user_asn:     Optional[str],
        network_type: Optional[str],
    ) -> float:
        """
        Pure performance score — only called when no policy covers the user's context.
        Base score = success_rate_score * 0.70 + connect_speed_score * 0.30
        No policy bias here — policy decisions are handled before this is called.
        """
        metrics = await self._get_protocol_metrics_cached(
            server.app_name, protocol, user_country, user_asn, network_type
        )

        # Component 1: success rate (0–100)
        if metrics and metrics.get("total_attempts", 0) > 0:
            rate = metrics["success_count"] / metrics["total_attempts"]
            success_rate_score = rate * 100.0
        else:
            success_rate_score = 50.0  # neutral when no data

        # Component 2: connect speed (0–100)
        if metrics and metrics.get("avg_connect_time_ms", 0) > 0:
            connect_speed_score = max(0.0, 100.0 - (metrics["avg_connect_time_ms"] / 50.0))
        else:
            connect_speed_score = 50.0  # neutral when no data

        return round(
            success_rate_score  * self.WEIGHT_SUCCESS_RATE +
            connect_speed_score * self.WEIGHT_CONNECT_SPEED,
            4
        )



    # ------------------------------------------------------------------ #
    #  Metrics lookup with short-lived cache                               #
    # ------------------------------------------------------------------ #

    async def _get_protocol_metrics_cached(
        self,
        app_name:     str,
        protocol:     str,
        country:      Optional[str],
        asn:          Optional[str],
        network_type: Optional[str],
    ) -> Optional[dict]:
        """
        Returns aggregated ProtocolMetrics across ALL servers and ALL apps,
        cached for 5s. 4-level specificity fallback.
        """
        cache_key = (
            f"pm:global:{protocol}"
            f":{country or '_'}:{asn or '_'}:{network_type or '_'}"
        )
        cached = await get_cache(cache_key)
        if cached is not None:
            return cached if cached else None

        agg = await self._get_protocol_metrics_db(
            app_name, protocol, country, asn, network_type
        )

        if agg:
            payload = {
                "success_count":       agg["success_count"],
                "failure_count":       agg["failure_count"],
                "total_attempts":      agg["total_attempts"],
                "avg_connect_time_ms": agg["avg_connect_time_ms"],
            }
        else:
            payload = {}

        await set_cache(cache_key, payload, ttl=5)
        return payload if payload else None

    async def _get_protocol_metrics_db(
        self,
        app_name:     str,
        protocol:     str,
        country:      Optional[str],
        asn:          Optional[str],
        network_type: Optional[str],
    ) -> Optional[dict]:
        """
        Aggregate ProtocolMetrics across ALL servers and ALL apps.
        4-level fallback (most specific → least specific).
        """
        base = [ProtocolMetrics.protocol == protocol]

        async def _agg(extra_filters: list) -> Optional[dict]:
            q = select(
                func.sum(ProtocolMetrics.success_count).label("success_count"),
                func.sum(ProtocolMetrics.failure_count).label("failure_count"),
                func.sum(ProtocolMetrics.total_attempts).label("total_attempts"),
                (
                    func.sum(ProtocolMetrics.avg_connect_time_ms * ProtocolMetrics.total_attempts)
                    / func.nullif(func.sum(ProtocolMetrics.total_attempts), 0)
                ).label("avg_connect_time_ms"),
            ).where(and_(*base, *extra_filters))

            row = (await self.db.execute(q)).one_or_none()
            if row is None or row.total_attempts is None or row.total_attempts == 0:
                return None
            return {
                "success_count":       int(row.success_count or 0),
                "failure_count":       int(row.failure_count or 0),
                "total_attempts":      int(row.total_attempts),
                "avg_connect_time_ms": float(row.avg_connect_time_ms or 0.0),
            }

        if country and asn and network_type:
            result = await _agg([
                ProtocolMetrics.country      == country,
                ProtocolMetrics.asn          == asn,
                ProtocolMetrics.network_type == network_type,
            ])
            if result:
                return result

        if country and asn:
            result = await _agg([
                ProtocolMetrics.country      == country,
                ProtocolMetrics.asn          == asn,
                ProtocolMetrics.network_type.is_(None),
            ])
            if result:
                return result

        if country:
            result = await _agg([
                ProtocolMetrics.country      == country,
                ProtocolMetrics.asn.is_(None),
                ProtocolMetrics.network_type.is_(None),
            ])
            if result:
                return result

        return await _agg([
            ProtocolMetrics.country.is_(None),
            ProtocolMetrics.asn.is_(None),
            ProtocolMetrics.network_type.is_(None),
        ])

    # ------------------------------------------------------------------ #
    #  Response builder                                                    #
    # ------------------------------------------------------------------ #

    async def _build_decision_response(self, choice: dict) -> BestServerDecision:
        server = choice["server"]
        srv    = choice["srv"]

        # Both protocols come from the same server row — same server_id
        primary_config = ProtocolConfig(
            protocol    = choice["primary_protocol"],
            server_id   = server.id,
            server_name = server.name,
            ip_address  = server.ip_address,
        )
        if choice["primary_protocol"] == 'openvpn':
            primary_config.ovpn_base64     = server.ovpn_base64
            primary_config.management_port = server.management_port
        else:
            primary_config.ss_port       = server.ss_port
            primary_config.ss_password   = server.ss_password
            primary_config.ss_encryption = server.ss_encryption

        fallback_config = ProtocolConfig(
            protocol    = choice["fallback_protocol"],
            server_id   = server.id,
            server_name = server.name,
            ip_address  = server.ip_address,
        )
        if choice["fallback_protocol"] == 'openvpn':
            fallback_config.ovpn_base64     = server.ovpn_base64
            fallback_config.management_port = server.management_port
        else:
            fallback_config.ss_port       = server.ss_port
            fallback_config.ss_password   = server.ss_password
            fallback_config.ss_encryption = server.ss_encryption

        return BestServerDecision(
            app_name          = server.app_name,
            primary_protocol  = choice["primary_protocol"],
            primary_config    = primary_config,
            primary_score     = choice["primary_score"],
            fallback_protocol = choice["fallback_protocol"],
            fallback_config   = fallback_config,
            fallback_score    = choice["fallback_score"],
            server_type       = server.server_type,
            server_city       = server.server_city,
            server_country    = server.server_country,
            flag_image_url    = server.flag_image_url,
            cpu_usage         = round(srv["cpu_usage"], 2),
            ram_usage         = round(srv["ram_usage"], 2),
            ping_ms           = round(srv["ping_ms"],   2),
            load_score        = round(srv["load_score"], 2),
            current_users     = srv["sessions"],
            max_capacity      = srv["max_capacity"],
        )

    # ------------------------------------------------------------------ #
    #  Connection feedback + cooldown management                           #
    # ------------------------------------------------------------------ #

    async def process_connection_feedback(
        self,
        server_id:                 int,
        server_ip:                 str,
        app_name:                  str,
        country:                   Optional[str],
        asn:                       Optional[str],
        network_type:              Optional[str],
        primary_protocol:          str,
        primary_success:           bool,
        primary_connect_time_ms:   Optional[float],
        secondary_protocol:        Optional[str],
        secondary_success:         Optional[bool],
        secondary_connect_time_ms: Optional[float],
    ):
        """
        Update ProtocolMetrics for each reported protocol attempt, then
        apply cooldown logic if both protocols failed.
        """
        gs = await self._load_global_settings()
        soft_ttl      = int(gs.cooldown_soft_seconds)
        hard_ttl      = int(gs.cooldown_hard_seconds)
        asn_threshold = int(gs.cooldown_country_block_asn_threshold)

        # Update metrics for primary
        await self._update_metrics(
            server_id, app_name, primary_protocol,
            country, asn, network_type,
            primary_success, primary_connect_time_ms,
        )

        # Update metrics for secondary (only if it was attempted)
        if secondary_protocol is not None and secondary_success is not None:
            await self._update_metrics(
                server_id, app_name, secondary_protocol,
                country, asn, network_type,
                secondary_success, secondary_connect_time_ms,
            )

        # Cooldown: only when BOTH protocols failed
        both_failed = (
            not primary_success
            and secondary_protocol is not None
            and secondary_success is False
        )

        if both_failed and country and asn:
            await self._apply_cooldown(
                server_ip, country, asn, soft_ttl, hard_ttl, asn_threshold
            )

        # Bust 5-second metrics cache
        await self._invalidate_metrics_cache(server_id)

    # ------------------------------------------------------------------ #
    #  Cooldown application                                                #
    # ------------------------------------------------------------------ #

    async def _apply_cooldown(
        self,
        server_ip:     str,
        country:       str,
        asn:           str,
        soft_ttl:      int,
        hard_ttl:      int,
        asn_threshold: int,
    ):
        redis       = await get_redis()
        asn_key     = _cd_asn_key(server_ip, country, asn)
        asn_set_key = _cd_asn_set_key(server_ip, country)

        current_level = await redis.get(asn_key)

        if current_level is None:
            await redis.setex(asn_key, soft_ttl, "soft")
        else:
            await redis.setex(asn_key, hard_ttl, "hard")

        await redis.sadd(asn_set_key, asn)
        await redis.expire(asn_set_key, hard_ttl)

        # Country-wide block check
        failing_asns = await redis.smembers(asn_set_key)
        active_count = 0
        for a in failing_asns:
            if await redis.exists(_cd_asn_key(server_ip, country, a)):
                active_count += 1
            else:
                await redis.srem(asn_set_key, a)

        if active_count >= asn_threshold:
            country_key = _cd_country_key(server_ip, country)
            await redis.setex(country_key, hard_ttl, "hard")

    # ------------------------------------------------------------------ #
    #  Metrics update                                                      #
    # ------------------------------------------------------------------ #

    async def _update_metrics(
        self,
        server_id:       int,
        app_name:        str,
        protocol:        str,
        country:         Optional[str],
        asn:             Optional[str],
        network_type:    Optional[str],
        success:         bool,
        connect_time_ms: Optional[float],
    ):
        """Upsert ProtocolMetrics and update success/failure counts + rolling avg connect time."""
        metrics = await self._get_or_create_metrics(
            server_id, app_name, protocol, country, asn, network_type
        )

        metrics.total_attempts += 1

        if success:
            metrics.success_count  += 1
            metrics.last_success_at = datetime.utcnow()

            if connect_time_ms and connect_time_ms > 0:
                if metrics.avg_connect_time_ms == 0:
                    metrics.avg_connect_time_ms = connect_time_ms
                else:
                    metrics.avg_connect_time_ms = (
                        metrics.avg_connect_time_ms * 0.7 + connect_time_ms * 0.3
                    )
                metrics.last_connect_time_ms = connect_time_ms
        else:
            metrics.failure_count    += 1
            metrics.last_failure_time = datetime.utcnow()

        metrics.success_rate = metrics.success_count / metrics.total_attempts
        metrics.updated_at   = datetime.utcnow()
        await self.db.commit()

    async def _get_or_create_metrics(
        self,
        server_id:    int,
        app_name:     str,
        protocol:     str,
        country:      Optional[str],
        asn:          Optional[str],
        network_type: Optional[str],
    ) -> ProtocolMetrics:
        """
        Fetch or create a ProtocolMetrics row keyed on:
          (server_id, protocol, country, asn, network_type)
        """
        q = select(ProtocolMetrics).where(and_(
            ProtocolMetrics.server_id    == server_id,
            ProtocolMetrics.protocol     == protocol,
            ProtocolMetrics.country      == country      if country      else ProtocolMetrics.country.is_(None),
            ProtocolMetrics.asn          == asn          if asn          else ProtocolMetrics.asn.is_(None),
            ProtocolMetrics.network_type == network_type if network_type else ProtocolMetrics.network_type.is_(None),
        ))
        metrics = (await self.db.execute(q)).scalar_one_or_none()

        if not metrics:
            metrics = ProtocolMetrics(
                server_id    = server_id,
                app_name     = None,
                protocol     = protocol,
                country      = country,
                asn          = asn,
                network_type = network_type,
                success_count        = 0,
                failure_count        = 0,
                total_attempts       = 0,
                success_rate         = 0.0,
                consecutive_failures = 0,
            )
            self.db.add(metrics)
            await self.db.flush()

        return metrics

    async def _invalidate_metrics_cache(self, server_id: int):
        """Bust the 5-second global metrics cache."""
        from app.cache import delete_cache
        await delete_cache("pm:global:*")