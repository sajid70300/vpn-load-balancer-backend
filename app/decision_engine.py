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

import time
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

# Cross-request cache for _get_policy_decision() results (Redis — the value
# is a plain [primary, fallback] pair, trivially JSON-safe). Country/ISP
# policies are admin-edited and change rarely; this TTL is a safety-net
# ceiling, not the real freshness mechanism — admin_metrics.py invalidates
# this on every policy create/update/delete, so an admin's change is picked
# up on the very next request regardless of TTL.
POLICY_DECISION_CACHE_TTL = 10

# Very short-lived, in-process cache for _load_servers(): shared across
# different requests for the SAME (app_name, server_type) arriving within a
# small window (e.g. several users of the same app a moment apart).
# In-process (module-level dict), not Redis: these are live SQLAlchemy ORM
# objects. Under expire_on_commit=False they stay safely readable after
# commit/detach — but only because the code that reads them afterward (here
# and in get_best_server/_score_protocols) only ever reads plain columns,
# never lazy-loads a relationship. They are not safely shareable across
# processes/Redis, so this cache is local to each worker process — with
# multiple uvicorn workers, each one gets this benefit independently rather
# than one shared cache across all of them, a deliberate trade-off for
# safety (no ORM (de)serialization) over maximum effect.
#
# Real-time capacity sync (an explicit client requirement) matters here: a
# server's capacity/active-state can change via the admin API at any moment,
# and that must still be reflected promptly. So this is backed by two
# things, not the TTL alone: (1) invalidate_server_list_cache() is called by
# every admin endpoint that already busts the routing caches
# (admin_servers.py, admin_machines.py, admin_apps.py) — instant on
# whichever worker process happens to handle that admin request; (2) the TTL
# bounds the worst case on the OTHER worker processes, which can't be
# reached by (1) directly since this cache isn't cross-process. Matched to
# public.py's best_server_v2/servers_config response caches (also 10s as of
# 2026-09-24) so every layer accepts the same staleness window. For OpenVPN
# specifically this adds little real risk on top of what already exists:
# session counts in the database are only refreshed every 35s by the
# monitor_vpn Celery task, so a few extra seconds of cache is a small
# addition to an existing tolerance, not a new category of staleness.
# Bounded in size: one entry per (app_name, server_type) combination this
# process has actually served — a small, fixed set for this deployment.
_SERVER_LIST_CACHE_TTL_SECONDS = 10.0
_server_list_cache: Dict[tuple, tuple] = {}


def invalidate_server_list_cache() -> None:
    """Call from any admin endpoint that changes server capacity, active
    state, or which servers exist for an app — same trigger points that
    already clear the Redis routing caches (best_server_v2:*, etc.)."""
    _server_list_cache.clear()
    _single_server_cache.clear()


# Same idea as _server_list_cache above, but for get_protocol_decision_for_server()'s
# own per-server lookup (used by /servers_config/'s loop — one call per server
# per request, previously uncached, found 2026-09-24 to be the still-remaining
# cause of connection pool exhaustion on /servers_config/ after the other four
# fixes). Same TTL, same in-process (not Redis) reasoning, same invalidation
# entry point as _server_list_cache — see invalidate_server_list_cache() above.
_single_server_cache: Dict[tuple, tuple] = {}

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
        # Per-request cache for _get_policy_decision(): the policy result
        # never depends on WHICH server is being scored, only on
        # (country, asn, enforce flags) — which are identical for every
        # server in one incoming request (e.g. every server in one
        # /servers_config/ call, looped in public.py). Without this, that
        # loop repeated the same ISP/Country policy database queries once
        # per server — found 2026-09-24 as a major source of background CPU
        # load, same root cause as the sync_server_sessions batching fix.
        # Scoped to this instance only: a fresh DecisionEngine (and empty
        # cache) is created per request in public.py, so this can never
        # serve stale data across requests or across an admin's own policy
        # edit and the next request.
        self._policy_decision_cache = {}

        # Same idea, for _get_protocol_metrics_cached(): its cache key
        # (protocol, country, asn, network_type) doesn't depend on which
        # server is being scored either, so it's identical across every
        # server in one /servers_config/ call for a given protocol. Found
        # 2026-09-25 via a live py-spy profile: even with its existing 5s
        # Redis cache, every server's lookup was still a real network
        # round-trip to Redis (up to ~60 per request for a 30-server app,
        # 2 protocols each) — this avoids all but the first one per request.
        self._metrics_cache = {}

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

        # Release the connection back to the pool now — this only ever runs on
        # a cache miss (rare, given the long TTL below), and everything needed
        # from `gs` is already read into `payload` (safe under
        # expire_on_commit=False). Callers go on to do Redis-heavy scoring
        # right after this returns, which doesn't need a DB connection held.
        await self.db.commit()

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

        # Normalize country to uppercase so 'pk' and 'PK' are always treated the same
        user_country = user_country.upper() if user_country else user_country

        protocol_mode = gs.protocol_mode

        # ── Phase 1: load servers ─────────────────────────────────────────
        servers = await self._load_servers(app_name, server_type)

        if not servers:
            raise ValueError("No active servers available")

        # Filter servers in cooldown — one batched Redis round-trip for the
        # whole server list instead of one sequential round-trip per server
        # (see _filter_out_cooldown_servers).
        available = await self._filter_out_cooldown_servers(servers, user_country, user_asn)

        if not available:
            raise ValueError("All servers are in cooldown for your region")

        # Score and sort servers
        scored: List[dict] = []
        for srv in available:
            score = self._server_load_score(srv)
            scored.append({**srv, "load_score_computed": score})

        scored.sort(key=lambda s: (
            not s["server"].is_priority_group,  # priority servers always first
            -s["load_score_computed"],           # within each tier, best load score first
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
        app_name:     Optional[str] = None,
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

        # Normalize country to uppercase so 'pk' and 'PK' are always treated the same
        user_country = user_country.upper() if user_country else user_country

        conditions = [
            VPNServer.is_active  == True,
            VPNServer.ip_address == ip_address,
        ]
        if server_type:
            conditions.append(VPNServer.server_type == server_type)
        if app_name:
            conditions.append(VPNServer.app_name == app_name)

        cache_key = (ip_address, server_type, app_name)
        cached = _single_server_cache.get(cache_key)
        now = time.monotonic()
        if cached is not None and cached[0] > now:
            rows = cached[1]
        else:
            query = (
                select(VPNServer, func.count(VPNUserSession.id).label('session_count'))
                .outerjoin(VPNUserSession)
                .where(and_(*conditions))
                .group_by(VPNServer.id)
            )
            rows = (await self.db.execute(query)).all()

            # Release the connection now (see _load_servers for why).
            # /servers_config/ calls this once per server in a loop, so
            # holding a connection through each one's Redis-heavy scoring
            # multiplies the hold time by server count. Only needed on an
            # actual DB hit — a cache hit never acquired a connection.
            await self.db.commit()

            _single_server_cache[cache_key] = (now + _SERVER_LIST_CACHE_TTL_SECONDS, rows)

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

    async def _filter_out_cooldown_servers(
        self,
        servers: List[dict],
        country: Optional[str],
        asn:     Optional[str],
    ) -> List[dict]:
        """
        Same result as calling _server_in_cooldown() once per server, but as
        ONE batched Redis round-trip for the whole list instead of one
        sequential round-trip per server. get_best_server() calls this once
        per incoming request, over every candidate server for the app — with
        many servers per app, the old one-await-per-server loop meant that
        many sequential Redis round-trips (plus their asyncio scheduling
        overhead) on every single request, independent of real user traffic.
        Found 2026-09-24 alongside the DB-side fixes in tasks.py and
        _get_policy_decision(). _server_in_cooldown() itself is untouched and
        still used directly elsewhere (e.g. get_protocol_decision_for_server's
        single-server path) — this is an additional, separate method for the
        multi-server case only.
        """
        if not country or not servers:
            # Matches _server_in_cooldown(): without a country, nothing is
            # ever considered in cooldown.
            return list(servers)

        redis = await get_redis()
        pipe = redis.pipeline()
        for srv in servers:
            ip = srv["server"].ip_address
            pipe.exists(_cd_country_key(ip, country))
            if asn:
                pipe.exists(_cd_asn_key(ip, country, asn))
        raw_results = await pipe.execute()

        if asn:
            # Two results per server, in the same order they were queued:
            # [country_0, asn_0, country_1, asn_1, ...].
            country_hits = raw_results[0::2]
            asn_hits      = raw_results[1::2]
            return [
                srv for srv, c_hit, a_hit in zip(servers, country_hits, asn_hits)
                if not (c_hit or a_hit)
            ]

        # One result per server: [country_0, country_1, ...].
        return [srv for srv, c_hit in zip(servers, raw_results) if not c_hit]

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
        cache_key = (app_name, server_type)
        cached = _server_list_cache.get(cache_key)
        now = time.monotonic()
        if cached is not None and cached[0] > now:
            return cached[1]

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

        # Release this connection back to the pool now. Everything needed from
        # these rows is already loaded into `servers` (safe under
        # expire_on_commit=False — attributes stay accessible after commit),
        # and the rest of get_best_server() does per-server Redis lookups
        # (cooldown checks, protocol scoring) that don't need this connection
        # held open. Without this, one request holds a connection for its
        # entire duration instead of just this query's — see the 2026-09-22
        # incident notes in database.py.
        await self.db.commit()

        _server_list_cache[cache_key] = (now + _SERVER_LIST_CACHE_TTL_SECONDS, servers)

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
        Cached wrapper around _compute_policy_decision() — see there for the
        actual policy rules. Caching lives here (not inside the compute
        function) because the compute function has several internal return
        points; wrapping it means the cache logic never has to touch or
        duplicate any of that branching.

        Two layers:
          1. Per-request (self._policy_decision_cache, in-memory) — never
             asks twice for the same key within one request.
          2. Cross-request (Redis, POLICY_DECISION_CACHE_TTL) — a second
             request a few seconds later for the same (country, asn, flags)
             reuses the first request's answer too. Invalidated immediately
             on any policy create/update/delete in admin_metrics.py, so the
             TTL is a safety-net ceiling, not the real freshness guarantee.
        """
        cache_key = (country, asn, enforce_country_policies, enforce_isp_policies)
        if cache_key in self._policy_decision_cache:
            return self._policy_decision_cache[cache_key]

        redis_key = f"policy_decision:{country}:{asn or '_'}:{enforce_country_policies}:{enforce_isp_policies}"
        cached = await get_cache(redis_key)
        if cached is not None:
            # [] represents a cached "no policy applies" (None); a 2-item
            # list represents an actual (primary, fallback) decision — same
            # existence-vs-value disambiguation already used for protocol
            # metrics caching below (get_cache returns None only when the
            # key is genuinely absent, never for a cached empty value).
            result = tuple(cached) if cached else None
            self._policy_decision_cache[cache_key] = result
            return result

        result = await self._compute_policy_decision(
            country, asn, enforce_country_policies, enforce_isp_policies
        )
        await set_cache(redis_key, list(result) if result else [], ttl=POLICY_DECISION_CACHE_TTL)
        self._policy_decision_cache[cache_key] = result
        return result

    async def _compute_policy_decision(
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
            # Release the connection now — the branching below is pure Python
            # over already-fetched rows (safe under expire_on_commit=False).
            await self.db.commit()

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
            # Release the connection now — same reasoning as the ISP check above.
            await self.db.commit()

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
        Cached wrapper around _compute_protocol_metrics_cached() — see there
        for the Redis/DB lookup itself. Adds a per-request layer in front of
        it: the (protocol, country, asn, network_type) key doesn't depend on
        which server is being scored, so within one request (e.g. every
        server in one /servers_config/ call) this avoids even the Redis
        round-trip after the first lookup for a given protocol, not just the
        database query the existing Redis cache already avoided.
        """
        cache_key = (protocol, country, asn, network_type)
        if cache_key in self._metrics_cache:
            return self._metrics_cache[cache_key]

        result = await self._compute_protocol_metrics_cached(
            app_name, protocol, country, asn, network_type
        )
        self._metrics_cache[cache_key] = result
        return result

    async def _compute_protocol_metrics_cached(
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
            # Release the connection now — the fallback levels below are pure
            # Python branching over an already-fetched row (safe under
            # expire_on_commit=False); the caller then does Redis work.
            await self.db.commit()
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
        # A cooldown_*_seconds of 0 (or less) means that level is disabled.
        # Redis SETEX/EXPIRE reject non-positive TTLs ("invalid expire time"),
        # so without this guard a 0 setting crashes every both-protocols-failed
        # feedback call with an unhandled ResponseError -> 500.
        if soft_ttl <= 0 and hard_ttl <= 0:
            return

        redis       = await get_redis()
        asn_key     = _cd_asn_key(server_ip, country, asn)
        asn_set_key = _cd_asn_set_key(server_ip, country)

        current_level = await redis.get(asn_key)

        if current_level is None:
            if soft_ttl > 0:
                await redis.setex(asn_key, soft_ttl, "soft")
        else:
            if hard_ttl > 0:
                await redis.setex(asn_key, hard_ttl, "hard")

        expiry_ttl = hard_ttl if hard_ttl > 0 else soft_ttl
        await redis.sadd(asn_set_key, asn)
        await redis.expire(asn_set_key, expiry_ttl)

        # Country-wide block check
        failing_asns = await redis.smembers(asn_set_key)
        active_count = 0
        for a in failing_asns:
            if await redis.exists(_cd_asn_key(server_ip, country, a)):
                active_count += 1
            else:
                await redis.srem(asn_set_key, a)

        if active_count >= asn_threshold and hard_ttl > 0:
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
        # Normalize country to uppercase so 'pk' and 'PK' are treated as the same
        country = country.upper() if country else country

        q = select(ProtocolMetrics).where(and_(
            ProtocolMetrics.server_id    == server_id,
            ProtocolMetrics.protocol     == protocol,
            ProtocolMetrics.country      == country      if country      else ProtocolMetrics.country.is_(None),
            ProtocolMetrics.asn          == asn          if asn          else ProtocolMetrics.asn.is_(None),
            ProtocolMetrics.network_type == network_type if network_type else ProtocolMetrics.network_type.is_(None),
        ))
        rows = (await self.db.execute(q)).scalars().all()

        if len(rows) > 1:
            # Duplicate rows for this exact key can exist because there is no
            # DB-level unique constraint backing it: two concurrent requests can
            # both see "no row yet" and both insert one (classic check-then-insert
            # race). scalar_one_or_none() used to crash the whole request here
            # (MultipleResultsFound -> 500) the moment this happened. Instead,
            # converge on the oldest row (lowest id) so all callers agree on the
            # same "canonical" row going forward, and log it for visibility.
            # This stops the crash but does NOT merge the split counts already
            # sitting in the other duplicate row(s), and does NOT prevent new
            # duplicates from forming — that needs a unique constraint + an
            # atomic upsert, tracked as separate follow-up work, not done here.
            print(f"⚠️  Duplicate protocol_metrics rows for server_id={server_id} protocol={protocol} "
                  f"country={country} asn={asn} network_type={network_type} — "
                  f"{len(rows)} rows (ids={sorted(r.id for r in rows)}), using the oldest.")
            metrics = min(rows, key=lambda r: r.id)
        elif rows:
            metrics = rows[0]
        else:
            metrics = None

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