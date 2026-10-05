"""Pure-logic tests for app/analytics_series.py: window/resolution rules and graph point assembly."""
import random

import pytest

from app import analytics_series as sx
from app.analytics_series import (
    GRANULARITY, MAX_POINTS, STEP_CHOICES, Window, build_points, iso, parse_instant, resolve_window,
)

H = 3600
D = 86400
NOW = 1_800_000_000 + 1800            # half past an hour boundary (1.8e9 is a multiple of 3600)
FINE_DAYS, HOURLY_DAYS = 3, 60


def win(key, frm=None, to=None, resolution=None, now=NOW, fine=FINE_DAYS, hourly=HOURLY_DAYS):
    return resolve_window(key, frm, to, resolution, now, fine, hourly)


def z(epoch):
    return iso(epoch)


# ─── presets ──────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("key,secs,report_source,step,series_source", [
    ("1h",  H,      "5m",     300,   "5m"),
    ("6h",  6 * H,  "5m",     300,   "5m"),
    ("24h", D,      "hourly", 900,   "5m"),
    ("7d",  7 * D,  "hourly", 3600,  "hourly"),
    ("30d", 30 * D, "hourly", 14400, "hourly"),
])
def test_presets_keep_the_first_release_start_and_table_for_the_report(key, secs, report_source, step, series_source):
    w = win(key)
    gran = GRANULARITY[report_source]
    assert w.report_source == report_source and w.series_source == series_source and w.step == step
    assert w.from_s == (NOW - secs) // gran * gran             # identical to point 1's alignment
    assert w.to_s is None                                       # open ended, includes the current bucket
    assert w.points == (NOW - w.from_s) // step + 1
    assert w.points <= MAX_POINTS


def test_the_graph_grid_covers_exactly_from_start_to_the_bucket_containing_now():
    for key in ("1h", "6h", "24h", "7d", "30d"):
        w = win(key)
        last_start = w.from_s + (w.points - 1) * w.step
        assert last_start <= NOW < last_start + w.step


def test_a_preset_never_uses_5_minute_data_that_is_no_longer_retained():
    w = win("24h", fine=1)                       # only 1 day of fine data kept
    assert w.series_source == "hourly" and w.step == 3600 and w.from_s % 3600 == 0


def test_unknown_preset_is_rejected():
    with pytest.raises(ValueError):
        win("90d")


# ─── custom ranges ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("span_hours,expected_step,source", [
    (1, 300, "5m"), (12, 300, "5m"), (24, 900, "5m"), (60, 1800, "5m"),
])
def test_recent_custom_ranges_use_fine_data_with_an_automatic_step(span_hours, expected_step, source):
    to = NOW - 600
    w = win("custom", z(to - span_hours * H), z(to))
    assert w.step == expected_step and w.series_source == source
    assert w.points <= sx.TARGET_POINTS


@pytest.mark.parametrize("span_days,expected_step", [(7, 3600), (14, 7200), (30, 14400), (45, 21600), (60, 43200)])
def test_older_custom_ranges_fall_back_to_hourly_data_and_coarser_steps(span_days, expected_step):
    w = win("custom", z(NOW - span_days * D), z(NOW - D // 2))
    assert w.series_source == "hourly" and w.step == expected_step
    assert w.step % 3600 == 0 and w.from_s % 3600 == 0
    assert w.report_source == "hourly"


def test_custom_range_boundaries_are_aligned_outwards_to_the_report_table():
    w = win("custom", z(NOW - 9 * H + 123), z(NOW - 2 * H + 61))                   # 7 hours: over the 6h limit for 5m cards
    assert w.report_source == "hourly"
    assert w.from_s % 3600 == 0 and w.to_s % 3600 == 0
    assert w.from_s <= NOW - 9 * H + 123 and w.to_s >= NOW - 2 * H + 61           # never narrower than asked

    short = win("custom", z(NOW - 50 * 60 + 17), z(NOW - 20 * 60 + 5))
    assert short.report_source == "5m" and short.from_s % 300 == 0 and short.to_s % 300 == 0


def test_a_range_reaching_into_the_future_is_clamped_to_now():
    w = win("custom", z(NOW - 2 * H), z(NOW + 5 * D))
    assert w.clamped is True
    assert w.to_s - NOW < 3600 + 1                                  # aligned up from "now", nothing beyond
    assert w.from_s <= NOW - 2 * H


def test_a_range_older_than_the_retained_history_is_clamped():
    w = win("custom", z(NOW - 400 * D), z(NOW - 10 * D))
    assert w.clamped is True
    assert w.from_s >= NOW - HOURLY_DAYS * D - 3600


def test_a_range_entirely_outside_the_retained_history_is_rejected():
    with pytest.raises(ValueError, match="outside the retained history"):
        win("custom", z(NOW - 400 * D), z(NOW - 300 * D))


@pytest.mark.parametrize("frm,to,msg", [
    (None, None, "needs both"), ("2026-01-01T00:00:00Z", None, "needs both"),
    ("2026-01-02T00:00:00Z", "2026-01-01T00:00:00Z", "must be after"),
    ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "must be after"),
    ("not-a-date", "2026-01-01T00:00:00Z", "Invalid date"), ("", "x", "needs both"),
])
def test_invalid_custom_input_gets_a_readable_error(frm, to, msg):
    with pytest.raises(ValueError, match=msg):
        win("custom", frm, to)


def test_parse_instant_accepts_z_offsets_and_naive_as_utc():
    assert parse_instant("2026-10-01T12:00:00Z") == parse_instant("2026-10-01T12:00:00+00:00") == parse_instant("2026-10-01T12:00:00")
    assert parse_instant("2026-10-01T17:00:00+05:00") == parse_instant("2026-10-01T12:00:00Z")
    assert parse_instant("2026-10-01 12:00") == parse_instant("2026-10-01T12:00:00Z")
    with pytest.raises(ValueError):
        parse_instant("2026-13-45")


# ─── resolution override ──────────────────────────────────────────────────────

def test_a_valid_requested_resolution_is_honoured():
    w = win("24h", resolution=1800)
    assert w.step == 1800 and not w.resolution_adjusted


def test_an_unsupported_resolution_is_rounded_up_to_the_next_supported_one():
    w = win("24h", resolution=600)
    assert w.step == 900 and w.resolution_adjusted


def test_a_resolution_too_fine_for_the_range_is_coarsened_to_stay_within_the_point_cap():
    w = win("30d", resolution=300)
    assert w.step >= 3600 and w.points <= MAX_POINTS and w.resolution_adjusted


def test_a_resolution_finer_than_the_available_data_is_not_used():
    w = win("custom", z(NOW - 20 * D), z(NOW - D), resolution=300)       # 5-minute data is only kept for 3 days
    assert w.step >= 3600 and w.series_source == "hourly"


# ─── invariants over many random windows (what keeps cost bounded) ─────────────

def test_every_possible_window_respects_the_cost_bounds():
    rng = random.Random(20261005)
    for _ in range(3000):
        span = rng.randint(60, HOURLY_DAYS * D)
        end = NOW - rng.randint(0, HOURLY_DAYS * D - span) if HOURLY_DAYS * D > span else NOW
        res = rng.choice([None, None, 300, 600, 900, 3600, 7200, 86400])
        try:
            w = win("custom", z(end - span), z(end), resolution=res)
        except ValueError:
            continue
        assert 1 <= w.points <= MAX_POINTS, (span, res, w)
        assert w.step in STEP_CHOICES
        assert w.from_s % GRANULARITY[w.report_source] == 0
        if w.series_source == "hourly":
            assert w.from_s % 3600 == 0 and w.step % 3600 == 0
        else:                                                        # fine data must still be retained
            assert w.from_s >= NOW - FINE_DAYS * D
        assert w.from_s >= NOW - HOURLY_DAYS * D - 3600              # never reads beyond retention
        if res is None:
            assert w.points <= sx.TARGET_POINTS or w.step == STEP_CHOICES[-1]


# ─── building the points ──────────────────────────────────────────────────────

def _window(step=900, points=8, origin=NOW - NOW % 900 - 7 * 900, now=NOW):
    return Window("24h", origin, None, "hourly", "5m", step, points, now)


def test_rows_land_in_the_right_bucket_and_every_event_is_counted_exactly_once():
    w = _window()
    o = w.from_s
    rows = [
        (o,                "PK", "openvpn",     5, 4, 1),
        (o + 300,          "PK", "openvpn",     2, 2, 0),        # same 15-minute bucket as the row above
        (o + 900,          "US", "shadowsocks", 3, 1, 2),
        (o + 7 * 900,      "OTHER", "openvpn",  1, 1, 0),
        (o - 300,          "PK", "openvpn",     99, 99, 99),     # before the grid: ignored
        (o + 8 * 900,      "PK", "openvpn",     99, 99, 99),     # after the grid: ignored
    ]
    pts = build_points(w, rows, [], ["PK", "US", "OTHER"], traffic_start_s=o - 10 * 900)
    assert len(pts) == 8
    assert (pts[0]["assigned"], pts[0]["success"], pts[0]["failed"]) == (7, 6, 1)
    assert pts[0]["protocols"]["openvpn"] == {"assigned": 7, "success": 6, "failed": 1}
    assert pts[0]["protocols"]["shadowsocks"] == {"assigned": 0, "success": 0, "failed": 0}
    assert pts[1]["countries"]["US"] == {"assigned": 3, "success": 1, "failed": 2}
    assert pts[1]["countries"]["PK"] == {"assigned": 0, "success": 0, "failed": 0}
    assert pts[7]["countries"]["OTHER"]["assigned"] == 1
    # conservation: nothing lost, nothing double counted
    assert sum(p["assigned"] for p in pts) == 5 + 2 + 3 + 1
    assert sum(p["success"] for p in pts) == 4 + 2 + 1 + 1
    assert sum(p["failed"] for p in pts) == 1 + 0 + 2 + 0


def test_country_and_protocol_breakdowns_always_add_up_to_the_totals():
    rng = random.Random(7)
    w = _window(points=8)
    rows = []
    for i in range(8):
        for c in ("PK", "AE", "OTHER"):
            for p in ("openvpn", "shadowsocks", "other"):
                rows.append((w.from_s + i * 900 + rng.choice((0, 300, 600)), c, p,
                             rng.randint(0, 9), rng.randint(0, 9), rng.randint(0, 9)))
    pts = build_points(w, rows, [], ["PK", "AE", "OTHER"], traffic_start_s=0)
    for p in pts:
        for metric in ("assigned", "success", "failed"):
            by_country = sum(c[metric] for c in p["countries"].values())
            by_protocol = sum(v[metric] for v in p["protocols"].values())     # 'other' protocol is only in the total
            assert by_country == p[metric]
            assert by_protocol <= p[metric]


def test_success_rate_is_none_without_attempts_not_zero():
    w = _window(points=2)
    pts = build_points(w, [(w.from_s, "PK", "openvpn", 4, 0, 0)], [], ["PK"], traffic_start_s=0)
    assert pts[0]["success_rate"] is None and pts[0]["assigned"] == 4
    pts = build_points(w, [(w.from_s, "PK", "openvpn", 0, 3, 1)], [], ["PK"], traffic_start_s=0)
    assert pts[0]["success_rate"] == 75.0


def test_buckets_before_recording_began_are_null_but_recorded_zero_is_zero():
    w = _window(points=6)
    start = w.from_s + 2 * 900 + 300                      # recording began inside bucket 2
    pts = build_points(w, [], [], ["PK"], traffic_start_s=start)
    assert pts[0]["assigned"] is None and pts[0]["protocols"] is None and pts[0]["countries"] is None
    assert pts[1]["success_rate"] is None and pts[1]["assigned"] is None
    assert pts[2]["assigned"] == 0 and pts[2]["countries"]["PK"]["success"] == 0     # recorded, just quiet
    assert all(p["assigned"] == 0 for p in pts[2:])
    nothing = build_points(w, [], [], ["PK"], traffic_start_s=None)
    assert all(p["assigned"] is None for p in nothing)


def test_only_the_bucket_containing_now_is_marked_partial():
    w = _window()
    pts = build_points(w, [], [], [], traffic_start_s=None)
    partial = [i for i, p in enumerate(pts) if p["partial"]]
    assert partial == [7]
    closed = build_points(Window("custom", w.from_s, w.from_s + 8 * 900, "hourly", "5m", 900, 8, NOW + 5 * D), [], [], [], None)
    assert not any(p["partial"] for p in closed)


def test_timestamps_are_utc_iso_with_z():
    pts = build_points(_window(), [], [], [], None)
    assert all(p["t"].endswith("Z") for p in pts) and pts[1]["t"] == iso(pts[0] and _window().from_s + 900)


# usage rows: (epoch, sessions, openvpn, shadowsocks, capacity|None, active_rows)

def test_sessions_avg_peak_capacity_and_utilization_per_bucket():
    w = _window(points=3)
    o = w.from_s
    usage = [
        (o,        100, 60, 40, 500, 1),
        (o + 300,  300, 200, 100, 500, 1),
        (o + 600,  200, 100, 100, 500, 1),
        (o + 900,   50, 50, 0, 1000, 1),            # capacity was raised: the past keeps its capacity
    ]
    pts = build_points(w, [], usage, [], None)
    assert pts[0]["sessions"] == {"avg": 200.0, "peak": 300}
    assert pts[0]["capacity"] == 500
    assert pts[0]["utilization"] == {"avg": 40.0, "peak": 60.0}
    assert pts[1]["capacity"] == 1000 and pts[1]["utilization"] == {"avg": 5.0, "peak": 5.0}
    assert pts[0]["server_active"] is True


def test_capacity_change_inside_a_bucket_reports_the_capacity_in_force_at_its_end():
    w = _window(points=1)
    usage = [(w.from_s, 100, 0, 0, 500, 1), (w.from_s + 300, 100, 0, 0, 800, 1)]
    pts = build_points(w, [], usage, [], None)
    assert pts[0]["capacity"] == 800
    assert pts[0]["utilization"]["peak"] == 20.0 and pts[0]["utilization"]["avg"] == 16.25


def test_a_disabled_server_contributes_no_capacity_and_no_utilization():
    w = _window(points=2)
    usage = [(w.from_s, 0, 0, 0, None, 0), (w.from_s + 900, 40, 40, 0, 500, 1)]
    pts = build_points(w, [], usage, [], None)
    assert pts[0]["capacity"] is None and pts[0]["utilization"] is None and pts[0]["server_active"] is False
    assert pts[0]["sessions"] == {"avg": 0.0, "peak": 0}
    assert pts[1]["utilization"]["peak"] == 8.0


def test_buckets_without_a_snapshot_are_null_not_zero():
    w = _window(points=4)
    pts = build_points(w, [], [(w.from_s + 2 * 900, 10, 10, 0, 100, 1)], [], None)
    assert pts[0]["sessions"] is None and pts[0]["capacity"] is None and pts[0]["utilization"] is None
    assert pts[2]["sessions"]["peak"] == 10
    assert pts[3]["sessions"] is None


def test_zero_capacity_gives_no_utilization():
    w = _window(points=1)
    pts = build_points(w, [], [(w.from_s, 10, 10, 0, 0, 1)], [], None)
    assert pts[0]["utilization"] is None and pts[0]["capacity"] is None
