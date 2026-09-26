"""Unit tests for timestamp normalization and stable hashing."""
from __future__ import annotations

import pytest

from app.timeutil import minute_bucket, to_ns
from app.sampler import hash_score, select_by_hash, stable_hash
from app.servicetime import exclusive_by_service, _union_length
from app.concurrency import active_windows
from app.integrity import check_integrity
from app.models import SpanIn
from pydantic import ValidationError


def _row(span_id, start_ns, end_ns, *, parent=None, service="svc"):
    return {
        "span_id": span_id,
        "parent_span_id": parent,
        "service": service,
        "start_ns": start_ns,
        "end_ns": end_ns,
    }


def _by_service(rows):
    return {e["service"]: e for e in exclusive_by_service(rows)}


def test_union_length_merges_overlapping_and_abutting():
    assert _union_length([]) == 0
    assert _union_length([(10, 40)]) == 30
    assert _union_length([(10, 40), (30, 80)]) == 70
    assert _union_length([(0, 10), (10, 20)]) == 20
    assert _union_length([(0, 10), (20, 30)]) == 20
    # Identical interval counts once.
    assert _union_length([(0, 50), (0, 50)]) == 50


def test_exclusive_time_subtracts_direct_children():
    rows = [
        _row("root", 0, 100, parent=None),
        _row("child", 20, 60, parent="root"),
    ]
    svcs = _by_service(rows)
    assert svcs["svc"]["span_count"] == 2
    # root: 100 - 40 = 60; child: 40 leaf time.
    assert svcs["svc"]["exclusive_ns"] == 100


def test_overlapping_children_are_subtracted_only_once():
    rows = [
        _row("root", 0, 100),
        _row("a", 10, 40, parent="root"),
        _row("b", 30, 80, parent="root"),
    ]
    # union [10,80) = 70 -> root exclusive 30; leaves add 30 + 50 = 80.
    assert _by_service(rows)["svc"]["exclusive_ns"] == 110


def test_child_extending_beyond_parent_is_clipped_to_parent():
    rows = [
        _row("root", 10, 50),
        _row("child", 0, 100, parent="root"),
    ]
    # root exclusive 0; child exclusive 100.
    assert _by_service(rows)["svc"]["exclusive_ns"] == 100


def test_self_referencing_parent_is_ignored():
    rows = [_row("loop", 0, 30, parent="loop")]
    assert _by_service(rows)["svc"]["exclusive_ns"] == 30


def test_missing_parent_is_not_a_child_of_anything():
    # Orphan span pointing at a span that never arrived keeps full time.
    rows = [_row("orphan", 0, 40, parent="absent-parent")]
    assert _by_service(rows)["svc"]["exclusive_ns"] == 40


def test_zero_duration_span_contributes_zero():
    rows = [
        _row("root", 0, 100),
        _row("instant", 50, 50, parent="root"),
    ]
    assert _by_service(rows)["svc"]["exclusive_ns"] == 100


def test_missing_service_lands_in_null_group():
    rows = [
        _row("a", 0, 30, service=None),
        _row("b", 0, 20, service="svc"),
    ]
    entries = {e["service"]: e for e in exclusive_by_service(rows)}
    assert entries[None] == {
        "service": None, "span_count": 1, "exclusive_ns": 30
    }
    assert entries["svc"]["exclusive_ns"] == 20


def test_sorting_ns_desc_name_asc_null_last():
    rows = [
        _row("b1", 0, 20, service="zsvc"),     # exclusive 20
        _row("b2", 0, 20, service="asvc"),     # exclusive 20, name tie-break
        _row("c", 0, 100, service="msvc"),     # exclusive 100
        _row("n", 0, 20, service=None),        # null, last among the 20s
    ]
    result = exclusive_by_service(rows)
    assert [(e["service"], e["exclusive_ns"]) for e in result] == [
        ("msvc", 100), ("asvc", 20), ("zsvc", 20), (None, 20),
    ]


def test_nested_spans_exclusive_sums_to_wall_clock_length():
    rows = [
        _row("root", 0, 100, service="a"),
        _row("mid", 10, 90, parent="root", service="b"),
        _row("leaf", 20, 80, parent="mid", service="c"),
    ]
    # a:20, b:20, c:60 -> total 100, the trace length.
    result = _by_service(rows)
    assert result["a"]["exclusive_ns"] == 20
    assert result["b"]["exclusive_ns"] == 20
    assert result["c"]["exclusive_ns"] == 60


def test_active_windows_basic_overlap_and_peak():
    rows = [
        _row("a", 0, 10),
        _row("b", 2, 8),
        _row("c", 4, 6),
    ]
    # active: 1@0, 2@2, 3@4, 2@6, 1@8, 0@10 -> one window [2, 8), peak 3.
    assert active_windows(rows, 2) == [
        {"start_ns": 2, "end_ns": 8, "duration_ns": 6, "peak_active": 3}
    ]


def test_active_windows_touching_endpoints_do_not_overlap():
    # Half-open [start, end): b starts exactly where a ends.
    rows = [_row("a", 0, 10), _row("b", 10, 20)]
    assert active_windows(rows, 2) == []


def test_active_windows_adjacent_qualifying_stretches_merge():
    # a ends where c starts (no false overlap), but b keeps the count at 2
    # across the seam at t=10, so [5, 15) is one continuous window.
    rows = [
        _row("a", 0, 10),
        _row("b", 5, 15),
        _row("c", 10, 20),
    ]
    assert active_windows(rows, 2) == [
        {"start_ns": 5, "end_ns": 15, "duration_ns": 10, "peak_active": 2}
    ]


def test_active_windows_separate_windows_and_min_active_threshold():
    rows = [
        _row("a", 0, 10),
        _row("b", 2, 4),
        _row("c", 20, 30),
        _row("d", 22, 28),
        _row("e", 24, 26),
    ]
    assert active_windows(rows, 2) == [
        {"start_ns": 2, "end_ns": 4, "duration_ns": 2, "peak_active": 2},
        {"start_ns": 22, "end_ns": 28, "duration_ns": 6, "peak_active": 3},
    ]
    # With a threshold of 3 only the second stretch qualifies.
    assert active_windows(rows, 3) == [
        {"start_ns": 24, "end_ns": 26, "duration_ns": 2, "peak_active": 3}
    ]
    # Nothing ever reaches 4.
    assert active_windows(rows, 4) == []


def test_active_windows_zero_duration_spans_are_ignored():
    rows = [
        _row("a", 0, 10),
        _row("instant", 5, 5),   # empty interval: never active
        _row("b", 5, 5),
    ]
    assert active_windows(rows, 2) == []


def test_active_windows_identical_spans_and_empty_input():
    assert active_windows([], 2) == []
    rows = [_row("a", 0, 10), _row("b", 0, 10)]
    assert active_windows(rows, 2) == [
        {"start_ns": 0, "end_ns": 10, "duration_ns": 10, "peak_active": 2}
    ]


def test_active_windows_rejects_min_active_below_two():
    with pytest.raises(ValueError):
        active_windows([_row("a", 0, 10)], 1)


def _irow(span_id, start_ns, end_ns, *, parent=None):
    return {
        "span_id": span_id,
        "parent_span_id": parent,
        "start_ns": start_ns,
        "end_ns": end_ns,
    }


def _anomaly_map(result):
    return {(a["span_id"], a["type"]): a for a in result["anomalies"]}


def test_integrity_empty_and_clean_tree():
    assert check_integrity([]) == {
        "span_count": 0, "valid": True, "anomalies": []
    }
    rows = [
        _irow("root", 0, 100),
        _irow("a", 10, 90, parent="root"),
        _irow("b", 20, 30, parent="a"),
    ]
    result = check_integrity(rows)
    assert result["span_count"] == 3
    assert result["valid"] is True
    assert result["anomalies"] == []


def test_integrity_missing_parent():
    rows = [
        _irow("orphan", 0, 10, parent="absent"),
        _irow("root", 0, 10),
    ]
    result = check_integrity(rows)
    assert result["valid"] is False
    assert result["anomalies"] == [{
        "type": "missing_parent",
        "span_id": "orphan",
        "parent_span_id": "absent",
    }]


def test_integrity_self_reference_is_its_own_anomaly():
    rows = [_irow("loop", 0, 30, parent="loop")]
    result = check_integrity(rows)
    assert result["anomalies"] == [{
        "type": "self_reference", "span_id": "loop",
    }]
    # The self edge must not be re-reported as a multi-node cycle.
    assert all(a["type"] != "cycle" for a in result["anomalies"])


def test_integrity_two_node_cycle_reported_once_and_order_independent():
    rows_a = [
        _irow("a", 0, 10, parent="b"),
        _irow("b", 0, 10, parent="a"),
    ]
    rows_b = list(reversed(rows_a))
    for rows in (rows_a, rows_b):
        result = check_integrity(rows)
        assert result["valid"] is False
        assert len(result["anomalies"]) == 1
        anomaly = result["anomalies"][0]
        assert anomaly["type"] == "cycle"
        assert anomaly["span_id"] == "a"          # smallest member first
        assert anomaly["cycle"] == ["a", "b"]


def test_integrity_three_node_cycle_rotated_to_smallest_member():
    rows = [
        _irow("c", 0, 10, parent="a"),
        _irow("b", 0, 10, parent="c"),
        _irow("a", 0, 10, parent="b"),
    ]
    result = check_integrity(rows)
    assert len(result["anomalies"]) == 1
    assert result["anomalies"][0]["cycle"] == ["a", "b", "c"]


def test_integrity_distinct_cycles_are_each_reported_once():
    rows = [
        _irow("a", 0, 10, parent="b"),
        _irow("b", 0, 10, parent="a"),
        _irow("x", 0, 10, parent="y"),
        _irow("y", 0, 10, parent="x"),
    ]
    result = check_integrity(rows)
    cycles = [a for a in result["anomalies"] if a["type"] == "cycle"]
    assert len(cycles) == 2
    assert [c["cycle"] for c in cycles] == [["a", "b"], ["x", "y"]]


def test_integrity_time_bounds_detects_start_and_end_overrun():
    rows = [
        _irow("root", 10, 100),
        _irow("early", 5, 50, parent="root"),     # starts before parent
        _irow("late", 20, 120, parent="root"),    # ends after parent
        _irow("ok", 10, 100, parent="root"),      # equal endpoints are fine
    ]
    result = check_integrity(rows)
    amap = _anomaly_map(result)
    assert set(amap) == {("early", "time_out_of_bounds"),
                         ("late", "time_out_of_bounds")}
    assert amap[("early", "time_out_of_bounds")] == {
        "type": "time_out_of_bounds",
        "span_id": "early",
        "parent_span_id": "root",
        "parent_start_ns": 10,
        "parent_end_ns": 100,
        "span_start_ns": 5,
        "span_end_ns": 50,
    }


def test_integrity_cyclic_edges_skip_time_bounds():
    # Every edge violates containment, but all belong to the ring.
    rows = [
        _irow("a", 100, 200, parent="b"),
        _irow("b", 0, 50, parent="a"),
    ]
    result = check_integrity(rows)
    assert [a["type"] for a in result["anomalies"]] == ["cycle"]


def test_integrity_missing_parent_edge_skips_time_bounds():
    rows = [_irow("orphan", 0, 999, parent="ghost")]
    result = check_integrity(rows)
    assert [a["type"] for a in result["anomalies"]] == ["missing_parent"]


def test_integrity_node_outside_a_cycle_is_still_time_checked():
    # x -> a points into the ring; a exists and the x->a edge is not itself a
    # cycle edge, so its time overrun must still be reported.
    rows = [
        _irow("a", 10, 20, parent="b"),
        _irow("b", 10, 20, parent="a"),
        _irow("x", 0, 99, parent="a"),
    ]
    result = check_integrity(rows)
    amap = _anomaly_map(result)
    assert ("a", "cycle") in amap
    assert ("x", "time_out_of_bounds") in amap


def test_integrity_anomalies_sorted_by_span_id_then_type():
    rows = [
        _irow("zroot", 0, 10),
        _irow("zchild", 20, 30, parent="zroot"),  # time overrun
        _irow("morph", 0, 5, parent="nope"),      # missing parent
        _irow("aloop", 0, 5, parent="aloop"),     # self reference
    ]
    result = check_integrity(rows)
    assert [(a["span_id"], a["type"]) for a in result["anomalies"]] == [
        ("aloop", "self_reference"),
        ("morph", "missing_parent"),
        ("zchild", "time_out_of_bounds"),
    ]
    assert result["valid"] is False


def test_epoch_units_are_normalized_to_ns():
    assert to_ns(1_700_000_000) == 1_700_000_000 * 10**9
    assert to_ns(1_700_000_000_000) == 1_700_000_000 * 10**9
    assert to_ns(1_700_000_000_000_000) == 1_700_000_000 * 10**9
    assert to_ns(1_700_000_000_000_000_000) == 1_700_000_000 * 10**9
    assert to_ns(1_700_000_000.5) == 1_700_000_000 * 10**9 + 500_000_000


def test_integer_epoch_ns_round_trips_exactly():
    # Values above 2**53 are not all representable as float; integer input
    # must not be routed through float on the way out.
    odd_ns = 1_577_840_400_010_000_001
    assert to_ns(odd_ns) == odd_ns


def test_iso8601_z_and_offsets():
    assert to_ns("2023-11-14T22:13:20Z") == 1_700_000_000 * 10**9
    assert to_ns("2023-11-14T23:13:20+01:00") == 1_700_000_000 * 10**9
    # Naive timestamps are interpreted as UTC.
    assert to_ns("2023-11-14T22:13:20") == 1_700_000_000 * 10**9


@pytest.mark.parametrize("bad", ["not-a-time", "2026-13-99", None])
def test_bad_timestamps_raise(bad):
    with pytest.raises((ValueError, TypeError)):
        to_ns(bad)


def test_minute_bucket_aligns_downward():
    base = 1_700_000_040  # epoch seconds, known multiple of 60
    assert minute_bucket((base + 59) * 10**9) == base
    assert minute_bucket((base + 60) * 10**9) == base + 60


def test_hash_is_stable_and_spread():
    assert stable_hash("abc") == stable_hash("abc")
    assert stable_hash("abc") != stable_hash("abd")
    scores = [hash_score(f"trace-{i}") for i in range(2000)]
    assert min(scores) >= 0 and max(scores) < 1
    # Rough uniformity: roughly half on each side of 0.5.
    below = sum(1 for s in scores if s < 0.5)
    assert 800 < below < 1200


def test_span_invalid_timestamp_is_rejected():
    with pytest.raises(ValidationError):
        SpanIn(trace_id="t", span_id="s", start_time="nope", end_time=1)
    # end before start is schema-valid but rejected per-span by the engine.
    span = SpanIn(trace_id="t", span_id="s", start_time=10, end_time=5)
    assert span.trace_id == "t"


def test_select_by_hash_exact_counts_and_stability():
    ids = [f"t{i}" for i in range(20)]
    kept1, dropped1 = select_by_hash(ids, 7)
    kept2, dropped2 = select_by_hash(list(reversed(ids)), 7)
    assert len(kept1) == 7 and len(dropped1) == 13
    # Input order must not change which traces win.
    assert set(kept1) == set(kept2)
    assert set(dropped1) == set(dropped2)
    # Zero budget drops everyone; oversized budget keeps everyone.
    kept, dropped = select_by_hash(ids, 0)
    assert kept == [] and len(dropped) == 20
    kept, dropped = select_by_hash(ids, 100)
    assert len(kept) == 20 and dropped == []
