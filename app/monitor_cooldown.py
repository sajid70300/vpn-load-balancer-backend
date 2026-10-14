"""
Release the health-check "skip" cooldown of a machine.

When the background health check (app/tasks.py, process_server_group) finds a
machine unreachable it marks its rows inactive and sets the Redis key
`inactive_retry:<ip>:<management_port>`. While that key exists the health check
skips the machine entirely (for INACTIVE_SERVER_RETRY_MINUTES, default 10).

If an admin switches a server back ON during that time, the key must go too —
otherwise the machine is neither verified nor corrected until the key expires,
and a machine that is still dead keeps being served to users for up to 10
minutes. Removing the key makes the very next health-check cycle (about 35 s)
probe the machine again; if it is still dead it is simply marked inactive again.

Safety:
  * Best-effort and never raises: it runs AFTER the admin's change has been
    committed, and a Redis problem must never turn a successful save into an
    error (the key then just expires by itself, exactly as before this change).
  * Admin API only — never used on the user-facing request path.
  * Short socket timeouts so a stuck Redis cannot hang an admin request.
"""
import asyncio
from typing import Iterable, Optional, Tuple

import redis.asyncio as aioredis
from redis.backoff import NoBackoff
from redis.retry import Retry

# The key is written by app/tasks.py with its own client:
#   redis.Redis(host='localhost', port=6379, db=1)
# The delete MUST hit that same host/port/db, so these mirror it on purpose and
# do NOT follow CACHE_REDIS_URL (which could point at another database and would
# silently miss the key). tests/test_inactive_cooldown_release.py fails if the
# two ever drift apart.
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 1

# Hosts are tried in order and the first one that answers wins. The numeric
# address goes first because name resolution of "localhost" can try IPv6 first
# and, in testing on Windows, the async client then ran out of time before
# falling back to IPv4 and silently deleted nothing. "localhost" stays as the
# fallback because that is exactly what tasks.py uses.
_HOSTS = ("127.0.0.1", REDIS_HOST)

_SOCKET_TIMEOUT_SECONDS = 1.5
# Hard ceiling for the whole operation (connect + delete + close). redis-py
# retries failed connections with growing back-off by default, which turned a
# 1 s timeout into ~15 s in testing — so retries are switched off below AND the
# whole call is capped, so a broken Redis can never stall an admin request.
_TOTAL_TIMEOUT_SECONDS = 3.5


def inactive_retry_key(ip_address: str, management_port: Optional[int]) -> str:
    """Same key tasks.py builds in process_server_group()/monitor_vpn_status()."""
    return f"inactive_retry:{ip_address}:{management_port}"


def _new_client(host: str):
    return aioredis.Redis(
        host=host, port=REDIS_PORT, db=REDIS_DB,
        socket_connect_timeout=_SOCKET_TIMEOUT_SECONDS,
        socket_timeout=_SOCKET_TIMEOUT_SECONDS,
        retry=Retry(NoBackoff(), 0),          # fail fast, no hidden retries
    )


async def _delete_on(host: str, keys) -> int:
    client = None
    try:
        client = _new_client(host)
        return int(await client.delete(*keys) or 0)
    finally:
        if client is not None:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001
                pass


async def _delete(keys) -> int:
    last_error: Optional[Exception] = None
    for host in _HOSTS:
        try:
            return await _delete_on(host, keys)
        except Exception as e:  # noqa: BLE001 - try the next address
            last_error = e
    raise last_error  # type: ignore[misc]


async def clear_inactive_cooldowns(targets: Iterable[Tuple[str, Optional[int]]]) -> int:
    """
    Delete the skip-cooldown of each (ip_address, management_port) given.
    Returns how many keys were removed; 0 when none existed or on any error.
    """
    keys = sorted({inactive_retry_key(ip, port) for ip, port in targets if ip})
    if not keys:
        return 0

    try:
        return await asyncio.wait_for(_delete(keys), timeout=_TOTAL_TIMEOUT_SECONDS)
    except Exception as e:  # noqa: BLE001 - must never break the admin action
        # (asyncio.TimeoutError is an Exception on 3.11+/3.12 as well)
        print(f"⚠️  Could not clear the inactive-server cooldown ({len(keys)} key(s)): "
              f"{type(e).__name__}: {e}")
        return 0
