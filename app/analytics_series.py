"""
Time-series support for the per-server analytics graphs — PURE logic (no database,
no FastAPI), so it can be tested exhaustively.

Two jobs:

1. resolve_window(): turn "last 24h" / a custom from-to range into ONE well-defined window:
   which summary table to read, the aligned start/end, and the graph resolution (bucket
   width). The per-server report cards and the graphs both use the same window, so the sum
   of the graph points always equals the card totals.

2. build_points(): turn the (already aggregated) database rows into the list of graph points.

Performance rules this module enforces (they are what keeps cost bounded as data grows):
  * A graph never has more than MAX_POINTS points, whatever the range: the bucket width grows
    with the range (5 min ... 1 day).
  * Fine (5-minute) data is only used while it is still retained; older ranges automatically use
    the hourly table with >= 1 hour buckets.
  * A custom range is clamped to the retained history; it can never ask the database for more
    than the retention window.

Everything is UTC. Times are integer epoch seconds inside this module.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

GRANULARITY = {"5m": 300, "hourly": 3600}            # seconds per row of each summary table
PRESET_SECONDS = {"1h": 3600, "6h": 6 * 3600, "24h": 86400, "7d": 7 * 86400, "30d": 30 * 86400}
# Which table the REPORT CARDS read for each preset (unchanged from the first release).
PRESET_REPORT_SOURCE = {"1h": "5m", "6h": "5m", "24h": "hourly", "7d": "hourly", "30d": "hourly"}
# Preferred graph bucket width per preset (seconds).
PRESET_STEP = {"1h": 300, "6h": 300, "24h": 900, "7d": 3600, "30d": 14400}

STEP_CHOICES = (300, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400)
TARGET_POINTS = 200            # automatic resolution aims for at most this many points
MAX_POINTS = 300               # hard cap, also for a user-chosen resolution
REPORT_5M_MAX_SPAN = 6 * 3600  # custom ranges up to this long read the 5-minute table for the cards
FINE_MARGIN = 3600             # fine data is "available" only if it starts this far inside retention
TOP_COUNTRIES = 5              # country graphs show the top N countries + "OTHER"
OTHER = "OTHER"

PROTOCOLS = ("openvpn", "shadowsocks")


@dataclass(frozen=True)
class Window:
    key: str                      # 1h | 6h | 24h | 7d | 30d | custom
    from_s: int                   # aligned inclusive start
    to_s: Optional[int]           # aligned EXCLUSIVE end for custom ranges; None = open-ended (presets)
    report_source: str            # '5m' | 'hourly'  (table for the report cards)
    series_source: str            # '5m' | 'hourly'  (table for the graphs)
    step: int                     # graph bucket width in seconds
    points: int                   # number of buckets in the graph grid
    now_s: int
    clamped: bool = False         # custom range was cut to the retained history / to "now"
    resolution_adjusted: bool = False   # requested resolution was coarsened (too many points / too fine)


def _floor(ts: int, step: int) -> int:
    return (ts // step) * step


def _ceil(ts: int, step: int) -> int:
    return -((-ts) // step) * step


def _points_for(span: int, step: int) -> int:
    return max(1, -((-span) // step))


def parse_instant(raw: str) -> int:
    """ISO-8601 -> epoch seconds. No timezone means UTC. Raises ValueError with a readable message."""
    text = (raw or "").strip()
    if not text:
        raise ValueError("Empty date/time")
    if text[-1] in "Zz":
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        raise ValueError(f"Invalid date/time {raw!r}; use ISO format such as 2026-10-01T12:00:00Z") from None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _pick_step(span: int, allowed: Sequence[int], resolution: Optional[int],
               preferred: Optional[int]) -> Tuple[int, bool]:
    """(step, adjusted). `preferred` is used by presets; otherwise the smallest step within TARGET_POINTS."""
    if resolution:
        candidates = [s for s in allowed if s >= resolution and _points_for(span, s) <= MAX_POINTS]
        step = candidates[0] if candidates else allowed[-1]
        return step, step != resolution
    if preferred is not None:
        return preferred, False
    for s in allowed:
        if _points_for(span, s) <= TARGET_POINTS:
            return s, False
    return allowed[-1], False


def resolve_window(
    key: str,
    from_raw: Optional[str],
    to_raw: Optional[str],
    resolution: Optional[int],
    now_s: int,
    fine_days: int,
    hourly_days: int,
) -> Window:
    """
    key: 1h|6h|24h|7d|30d|custom. For 'custom', from_raw and to_raw are required ISO timestamps.
    fine_days / hourly_days: retention of the 5-minute / hourly tables (days, >= 1).
    Raises ValueError with a user-readable message for invalid input.
    """
    fine_days = max(1, int(fine_days))
    hourly_days = max(1, int(hourly_days))
    fine_start = now_s - fine_days * 86400 + FINE_MARGIN

    # ── presets ────────────────────────────────────────────────────────────────
    if key in PRESET_SECONDS:
        secs = PRESET_SECONDS[key]
        report_source = PRESET_REPORT_SOURCE[key]
        from_s = _floor(now_s - secs, GRANULARITY[report_source])      # identical to the first release
        fine_ok = from_s >= fine_start
        allowed = [s for s in STEP_CHOICES if fine_ok or s >= 3600]
        span = now_s - from_s + 1
        preferred = PRESET_STEP[key] if (fine_ok or PRESET_STEP[key] >= 3600) else 3600
        step, adjusted = _pick_step(span, allowed, resolution, preferred)
        series_source = "5m" if (step < 3600 or from_s % 3600 != 0) else "hourly"
        return Window(
            key=key, from_s=from_s, to_s=None, report_source=report_source, series_source=series_source,
            step=step, points=(now_s - from_s) // step + 1, now_s=now_s, resolution_adjusted=adjusted,
        )

    if key != "custom":
        raise ValueError(f"Unknown period {key!r}")

    # ── custom range ───────────────────────────────────────────────────────────
    if not from_raw or not to_raw:
        raise ValueError("A custom range needs both 'from' and 'to'")
    from_in, to_in = parse_instant(from_raw), parse_instant(to_raw)
    if to_in <= from_in:
        raise ValueError("'to' must be after 'from'")

    clamped = False
    if to_in > now_s:
        to_in, clamped = now_s, True
    earliest = now_s - hourly_days * 86400
    if from_in < earliest:
        from_in, clamped = earliest, True
    if to_in <= from_in:
        raise ValueError(f"The selected range is outside the retained history ({hourly_days} days)")

    span_in = to_in - from_in
    report_source = "hourly"
    if span_in <= REPORT_5M_MAX_SPAN and _floor(from_in, 300) >= fine_start:
        report_source = "5m"
    gran = GRANULARITY[report_source]
    from_s = _floor(from_in, gran)
    to_s = _ceil(to_in, gran)

    span = to_s - from_s
    fine_ok = from_s >= fine_start
    allowed = [s for s in STEP_CHOICES if fine_ok or s >= 3600]
    step, adjusted = _pick_step(span, allowed, resolution, None)
    series_source = "5m" if (step < 3600 or from_s % 3600 != 0) else "hourly"
    end_s = min(to_s, _ceil(now_s + 1, GRANULARITY[series_source]))
    return Window(
        key="custom", from_s=from_s, to_s=to_s, report_source=report_source, series_source=series_source,
        step=step, points=_points_for(end_s - from_s, step), now_s=now_s, clamped=clamped,
        resolution_adjusted=adjusted,
    )


# ─── building the graph points ────────────────────────────────────────────────

def _rate(num: int, den: int) -> Optional[float]:
    return round(num / den * 100, 2) if den > 0 else None


def build_points(
    window: Window,
    traffic_rows: Iterable[Tuple[int, str, str, int, int, int]],
    usage_rows: Iterable[Tuple[int, int, int, int, Optional[int], int]],
    countries: Sequence[str],
    traffic_start_s: Optional[int],
) -> List[dict]:
    """
    traffic_rows: (bucket_epoch, country_group, protocol, assigned, success, failed)
                  already aggregated by (native bucket, country_group, protocol).
    usage_rows:   (snapshot_epoch, sessions, openvpn_sessions, shadowsocks_sessions,
                   capacity_or_None, active_server_rows)  one row per snapshot time.
    countries:    country groups to report (top countries, plus OTHER when it has data).
    traffic_start_s: first epoch at which traffic recording existed for these servers
                  (None = nothing recorded yet). Buckets entirely before it have no data (null),
                  which is different from "recorded, and zero".
    """
    n, step, origin = window.points, window.step, window.from_s

    traffic: List[Optional[dict]] = [None] * n
    for epoch, group, proto, assigned, success, failed in traffic_rows:
        i = (epoch - origin) // step
        if i < 0 or i >= n:
            continue
        acc = traffic[i]
        if acc is None:
            acc = traffic[i] = {"tot": [0, 0, 0], "proto": {}, "cty": {}}
        for bucket in (acc["tot"], acc["proto"].setdefault(proto, [0, 0, 0]), acc["cty"].setdefault(group, [0, 0, 0])):
            bucket[0] += assigned
            bucket[1] += success
            bucket[2] += failed

    usage: List[Optional[list]] = [None] * n
    for epoch, sessions, _ovpn, _ss, capacity, active_rows in sorted(usage_rows, key=lambda r: r[0]):
        i = (epoch - origin) // step
        if i < 0 or i >= n:
            continue
        if usage[i] is None:
            usage[i] = []
        usage[i].append((sessions, capacity, active_rows))

    out: List[dict] = []
    for i in range(n):
        start = origin + i * step
        end = start + step
        point: dict = {"t": iso(start), "partial": start <= window.now_s < end}

        if traffic_start_s is not None and end > traffic_start_s:
            acc = traffic[i] or {"tot": [0, 0, 0], "proto": {}, "cty": {}}
            a, s, f = acc["tot"]
            point["assigned"], point["success"], point["failed"] = a, s, f
            point["success_rate"] = _rate(s, s + f)
            point["protocols"] = {
                p: dict(zip(("assigned", "success", "failed"), acc["proto"].get(p, [0, 0, 0]))) for p in PROTOCOLS
            }
            point["countries"] = {
                c: dict(zip(("assigned", "success", "failed"), acc["cty"].get(c, [0, 0, 0]))) for c in countries
            }
        else:
            point["assigned"] = point["success"] = point["failed"] = point["success_rate"] = None
            point["protocols"] = point["countries"] = None

        snaps = usage[i]
        if snaps:
            sessions_list = [s for s, _c, _a in snaps]
            utils = [s / c * 100 for s, c, _a in snaps if c]
            capacity = next((c for _s, c, _a in reversed(snaps) if c), None)   # capacity in force at the end of the bucket
            point["sessions"] = {"avg": round(sum(sessions_list) / len(sessions_list), 1), "peak": max(sessions_list)}
            point["capacity"] = capacity
            point["utilization"] = (
                {"avg": round(sum(utils) / len(utils), 2), "peak": round(max(utils), 2)} if utils else None
            )
            point["server_active"] = any(active for _s, _c, active in snaps)
        else:
            point["sessions"] = point["capacity"] = point["utilization"] = point["server_active"] = None
        out.append(point)
    return out
