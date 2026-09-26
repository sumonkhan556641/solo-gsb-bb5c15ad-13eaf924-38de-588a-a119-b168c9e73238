"""Structural integrity checks for one trace.

Two invariants are validated against a snapshot of received spans:

1. **Parent references.** Every non-null ``parent_span_id`` must point at a
   span present in the same trace. A span pointing at itself is a
   ``self_reference`` anomaly; two or more distinct spans linked into a ring
   are a ``cycle`` anomaly. Edges whose parent is missing take no part in
   cycle detection or time checks.
2. **Time bounds.** For an edge whose parent exists and which is not part of a
   cycle, the child's ``[start_ns, end_ns]`` interval must be contained in the
   parent's interval; otherwise the edge is a ``time_out_of_bounds`` anomaly.

Each cycle is reported exactly once even though every span on the ring closes
it. Spans on a cycle are skipped by the time-bound check (every edge of a ring
is inside the ring). Pure functions only — no I/O, no sampling policy.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Literal

MISSING_PARENT: Literal["missing_parent"] = "missing_parent"
SELF_REFERENCE: Literal["self_reference"] = "self_reference"
CYCLE: Literal["cycle"] = "cycle"
TIME_OUT_OF_BOUNDS: Literal["time_out_of_bounds"] = "time_out_of_bounds"


def check_integrity(
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Validate parent references and parent/child time bounds.

    ``rows`` are mappings with at least ``span_id``, ``parent_span_id``,
    ``start_ns`` and ``end_ns`` keys (``sqlite3.Row`` works). Returns
    ``{"span_count", "valid", "anomalies"}`` where ``anomalies`` is sorted by
    ``span_id`` (the canonical first member for cycles) then anomaly type.
    """
    # span_id -> (parent_span_id, start_ns, end_ns); receive order is kept so
    # cycle traversal and anomaly ordering stay deterministic.
    spans: dict[str, tuple[str | None, int, int]] = {}
    order: list[str] = []
    for r in rows:
        sid = r["span_id"]
        if sid in spans:  # pragma: no cover - (trace_id, span_id) is a PK
            continue
        spans[sid] = (
            r["parent_span_id"], int(r["start_ns"]), int(r["end_ns"]),
        )
        order.append(sid)
    known = set(spans)

    anomalies: list[dict[str, Any]] = []

    # Per-edge structural problems, plus the functional parent graph that only
    # retains edges between two distinct, present spans.
    parent: dict[str, str] = {}
    for sid in order:
        pid = spans[sid][0]
        if pid is None:
            continue
        if pid == sid:
            anomalies.append({"type": SELF_REFERENCE, "span_id": sid})
        elif pid not in known:
            anomalies.append({
                "type": MISSING_PARENT,
                "span_id": sid,
                "parent_span_id": pid,
            })
        else:
            parent[sid] = pid

    # Tri-color DFS over the functional graph (each span has <= 1 parent).
    # A GRAY node reached from the current walk is the ring entry point.
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {sid: WHITE for sid in order}
    cycle_spans: set[str] = set()
    seen_cycles: set[tuple[str, ...]] = set()
    for root in order:
        if color[root] != WHITE:
            continue
        path: list[str] = []
        position: dict[str, int] = {}
        node: str | None = root
        while node is not None and color.get(node, BLACK) == WHITE:
            color[node] = GRAY
            position[node] = len(path)
            path.append(node)
            node = parent.get(node)
        if node is not None and color.get(node) == GRAY:
            ring = path[position[node]:]
            canonical = _canonical_cycle(ring)
            if canonical not in seen_cycles:
                seen_cycles.add(canonical)
                cycle_spans.update(ring)
                anomalies.append({
                    "type": CYCLE,
                    "span_id": canonical[0],
                    "cycle": list(canonical),
                })
        for n in path:
            color[n] = BLACK

    # Time bounds only for edges with an existing parent that are not cyclic.
    for sid in order:
        pid = spans[sid][0]
        if pid is None or pid == sid or pid not in known or sid in cycle_spans:
            continue
        _pid, p_start, p_end = spans[pid]
        _sid_pid, c_start, c_end = spans[sid]
        if c_start < p_start or c_end > p_end:
            anomalies.append({
                "type": TIME_OUT_OF_BOUNDS,
                "span_id": sid,
                "parent_span_id": pid,
                "parent_start_ns": p_start,
                "parent_end_ns": p_end,
                "span_start_ns": c_start,
                "span_end_ns": c_end,
            })

    anomalies.sort(key=lambda a: (a["span_id"], a["type"]))
    return {
        "span_count": len(spans),
        "valid": not anomalies,
        "anomalies": anomalies,
    }


def _canonical_cycle(ring: list[str]) -> tuple[str, ...]:
    """Rotate a ring so the lexicographically smallest member comes first."""
    cut = min(range(len(ring)), key=lambda i: ring[i])
    return tuple(ring[cut:] + ring[:cut])
