#!/usr/bin/env python3
"""Generate daily stats JSON for the actransit performance dashboard.

Reads BigQuery (trip_observations) and GCS (gtfs/current.zip) and writes a
single JSON document used by the static site at site/index.html.

Usage:
    python3 scripts/generate_stats.py [--service-date YYYY-MM-DD] [--output PATH]

Defaults to today's date in America/Los_Angeles and writes to
site/data/stats.json.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import io
import json
import subprocess
import sys
import zipfile
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ID = "transit-203605"
DATASET = "actransit"
BUCKET = "transit-203605-actransit-cache"
GTFS_ZIP_GCS = f"gs://{BUCKET}/gtfs/current.zip"
PT = ZoneInfo("America/Los_Angeles")


def run_bq(sql: str) -> list[dict]:
    """Execute a BQ query and return rows as a list of dicts."""
    proc = subprocess.run(
        ["bq", "query", "--use_legacy_sql=false", "--format=json",
         "--max_rows=10000", sql],
        capture_output=True, text=True, check=True,
    )
    return json.loads(proc.stdout) if proc.stdout.strip() else []


def fetch_gtfs_zip() -> zipfile.ZipFile:
    """Download gtfs/current.zip from GCS into an in-memory zip."""
    proc = subprocess.run(
        ["gsutil", "cat", GTFS_ZIP_GCS],
        capture_output=True, check=True,
    )
    return zipfile.ZipFile(io.BytesIO(proc.stdout))


def read_csv(zf: zipfile.ZipFile, name: str) -> list[dict]:
    """Read a CSV file out of the zip, stripping any BOM."""
    with zf.open(name) as f:
        text = io.TextIOWrapper(f, encoding="utf-8-sig")
        return list(csv.DictReader(text))


def active_service_ids(zf: zipfile.ZipFile, target_date: datetime.date) -> set[str]:
    """Return service_ids active on target_date per GTFS calendar logic.

    A service_id is active if calendar.txt has it on the right weekday in
    the right date range, modulo any calendar_dates.txt exception that
    specifically adds (type=1) or removes (type=2) it on that date.
    """
    target_yyyymmdd = target_date.strftime("%Y%m%d")
    weekday = target_date.strftime("%A").lower()  # "monday", ...

    active: set[str] = set()
    for row in read_csv(zf, "calendar.txt"):
        if row.get("start_date", "") <= target_yyyymmdd <= row.get("end_date", "9999"):
            if row.get(weekday, "0") == "1":
                active.add(row["service_id"])

    for row in read_csv(zf, "calendar_dates.txt"):
        if row.get("date") == target_yyyymmdd:
            sid = row["service_id"]
            etype = row.get("exception_type", "")
            if etype == "1":
                active.add(sid)
            elif etype == "2":
                active.discard(sid)

    return active


def scheduled_trip_ids(zf: zipfile.ZipFile, services: set[str]) -> set[str]:
    """All trip_ids whose service_id is active on the target date."""
    return {row["trip_id"]
            for row in read_csv(zf, "trips.txt")
            if row.get("service_id") in services}


def scheduled_trip_routes(zf: zipfile.ZipFile, services: set[str]) -> dict[str, str]:
    """Map trip_id -> route_id for trips scheduled on the target date."""
    return {row["trip_id"]: row["route_id"]
            for row in read_csv(zf, "trips.txt")
            if row.get("service_id") in services}


def route_colors(zf: zipfile.ZipFile) -> dict[str, dict[str, str]]:
    """Map route_id → {color, text_color} as 6-char hex (no leading '#').

    GTFS defaults: route_color = FFFFFF (white), route_text_color = 000000 (black).
    """
    out: dict[str, dict[str, str]] = {}
    for row in read_csv(zf, "routes.txt"):
        color = (row.get("route_color") or "").strip().upper() or "FFFFFF"
        text_color = (row.get("route_text_color") or "").strip().upper() or "000000"
        out[row["route_id"]] = {"color": color, "text_color": text_color}
    return out


def secs_to_min(v) -> float | None:
    """Convert seconds (string or number) to minutes rounded to 1 decimal."""
    if v is None or v == "":
        return None
    return round(float(v) / 60.0, 1)


def cast_route_row(r: dict) -> dict:
    """Convert BigQuery JSON string values to native types for a route row."""
    def i(v):
        return int(v) if v is not None and v != "" else None
    def f(v):
        return float(v) if v is not None and v != "" else None
    return {
        "route_id":          r["route_id"],
        "trips_observed":    i(r["trips_observed"]),
        "observations":      i(r["observations"]),
        "on_time_pct":       f(r["on_time_pct"]),
        "within_5min_pct":   f(r["within_5min_pct"]),
        "within_7min_pct":   f(r["within_7min_pct"]),
        "late_pct":          f(r["late_pct"]),
        "early_pct":         f(r["early_pct"]),
        "p5_delay_minutes":  secs_to_min(r["p5_delay_seconds"]),
        "p25_delay_minutes": secs_to_min(r["p25_delay_seconds"]),
        "p50_delay_minutes": secs_to_min(r["p50_delay_seconds"]),
        "p75_delay_minutes": secs_to_min(r["p75_delay_seconds"]),
        "p95_delay_minutes": secs_to_min(r["p95_delay_seconds"]),
        "avg_speed_mph":     f(r["avg_speed_mph"]),
    }


def default_service_date() -> datetime.date:
    """Default to today's service_date in PT, but fall back to yesterday if
    we're in the early-morning hours when today's service hasn't ramped up yet.
    AC Transit's service day rolls roughly at 3 AM PT; before then, yesterday's
    data is what's interesting."""
    now_pt = datetime.datetime.now(PT)
    if now_pt.hour < 4:
        return now_pt.date() - datetime.timedelta(days=1)
    return now_pt.date()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--service-date", default=str(default_service_date()),
                   help="ISO date (YYYY-MM-DD), default = today in PT (or yesterday before 4am)")
    p.add_argument("--output", default="site/data/stats.json")
    args = p.parse_args()

    service_date = datetime.date.fromisoformat(args.service_date)
    print(f"Generating stats for service_date={service_date}", file=sys.stderr)

    print("Downloading gtfs/current.zip...", file=sys.stderr)
    zf = fetch_gtfs_zip()

    services = active_service_ids(zf, service_date)
    scheduled_route_by_trip = scheduled_trip_routes(zf, services)
    scheduled = set(scheduled_route_by_trip.keys())
    colors = route_colors(zf)
    print(f"  active service_ids: {len(services)}", file=sys.stderr)
    print(f"  scheduled trips:    {len(scheduled)}", file=sys.stderr)
    print(f"  routes with colors: {len(colors)}", file=sys.stderr)

    scheduled_by_route: dict[str, int] = {}
    for rid in scheduled_route_by_trip.values():
        scheduled_by_route[rid] = scheduled_by_route.get(rid, 0) + 1

    print("Querying BQ for trips observed today...", file=sys.stderr)
    rows = run_bq(f"""
        SELECT DISTINCT trip_id
        FROM `{PROJECT_ID}.{DATASET}.trip_observations`
        WHERE service_date = "{service_date}"
          AND actual_arrival IS NOT NULL
    """)
    ran = {r["trip_id"] for r in rows}
    print(f"  trips with ≥1 stop crossing: {len(ran)}", file=sys.stderr)

    dropped = sorted(scheduled - ran)
    ran_in_schedule = scheduled & ran

    print("Querying BQ for per-route stats...", file=sys.stderr)
    routes_raw = run_bq(f"""
        WITH base AS (
          SELECT route_id, trip_id, delay_seconds, leg_avg_speed_mps
          FROM `{PROJECT_ID}.{DATASET}.trip_observations`
          WHERE service_date = "{service_date}"
            AND actual_arrival IS NOT NULL
            AND is_stale = FALSE
        )
        SELECT
          route_id,
          COUNT(DISTINCT trip_id)                                              AS trips_observed,
          COUNT(*)                                                             AS observations,
          ROUND(AVG(IF(delay_seconds BETWEEN 0 AND 180, 1, 0)) * 100, 1)        AS on_time_pct,
          ROUND(AVG(IF(delay_seconds BETWEEN 0 AND 300, 1, 0)) * 100, 1)        AS within_5min_pct,
          ROUND(AVG(IF(delay_seconds BETWEEN 0 AND 420, 1, 0)) * 100, 1)        AS within_7min_pct,
          ROUND(AVG(IF(delay_seconds < 0, 1, 0)) * 100, 1)                      AS early_pct,
          ROUND(AVG(IF(delay_seconds > 180, 1, 0)) * 100, 1)                    AS late_pct,
          APPROX_QUANTILES(delay_seconds, 100)[OFFSET(5)]                      AS p5_delay_seconds,
          APPROX_QUANTILES(delay_seconds, 100)[OFFSET(25)]                     AS p25_delay_seconds,
          APPROX_QUANTILES(delay_seconds, 100)[OFFSET(50)]                     AS p50_delay_seconds,
          APPROX_QUANTILES(delay_seconds, 100)[OFFSET(75)]                     AS p75_delay_seconds,
          APPROX_QUANTILES(delay_seconds, 100)[OFFSET(95)]                     AS p95_delay_seconds,
          ROUND(AVG(leg_avg_speed_mps) * 2.2369, 1)                             AS avg_speed_mph
        FROM base
        GROUP BY route_id
        ORDER BY trips_observed DESC, observations DESC
    """)
    routes = [cast_route_row(r) for r in routes_raw]

    # Per-route service delivery: trips that ran (≥1 stop crossing observed)
    # divided by trips scheduled today. Uses ran (which doesn't filter is_stale)
    # so we count any trip that had at least one observed arrival, even if it
    # was finalized via the stale-prune fallback path.
    ran_by_route: dict[str, int] = {}
    for tid in ran:
        rid = scheduled_route_by_trip.get(tid)
        if rid is not None:
            ran_by_route[rid] = ran_by_route.get(rid, 0) + 1

    for r in routes:
        rid = r["route_id"]
        c = colors.get(rid, {"color": "FFFFFF", "text_color": "000000"})
        r["color"] = c["color"]
        r["text_color"] = c["text_color"]
        sched = scheduled_by_route.get(rid, 0)
        ran_count = ran_by_route.get(rid, 0)
        r["scheduled_trips"] = sched
        r["ran_trips"] = ran_count
        r["service_delivered_pct"] = round(100 * ran_count / sched, 1) if sched > 0 else None

    print("Querying BQ for system stats...", file=sys.stderr)
    system_rows = run_bq(f"""
        WITH base AS (
          SELECT trip_id, vehicle_id, delay_seconds, leg_avg_speed_mps
          FROM `{PROJECT_ID}.{DATASET}.trip_observations`
          WHERE service_date = "{service_date}"
            AND actual_arrival IS NOT NULL
            AND is_stale = FALSE
        )
        SELECT
          COUNT(DISTINCT trip_id)                                              AS total_trips,
          COUNT(*)                                                             AS total_observations,
          COUNT(DISTINCT vehicle_id)                                           AS vehicles_observed,
          ROUND(AVG(IF(delay_seconds BETWEEN 0 AND 180, 1, 0)) * 100, 1)        AS on_time_pct,
          ROUND(AVG(IF(delay_seconds BETWEEN 0 AND 300, 1, 0)) * 100, 1)        AS within_5min_pct,
          ROUND(AVG(IF(delay_seconds BETWEEN 0 AND 420, 1, 0)) * 100, 1)        AS within_7min_pct,
          ROUND(AVG(IF(delay_seconds < 0, 1, 0)) * 100, 1)                      AS early_pct,
          ROUND(AVG(IF(delay_seconds > 180, 1, 0)) * 100, 1)                    AS late_pct,
          APPROX_QUANTILES(delay_seconds, 100)[OFFSET(50)]                     AS p50_delay_seconds,
          APPROX_QUANTILES(delay_seconds, 100)[OFFSET(95)]                     AS p95_delay_seconds,
          ROUND(AVG(leg_avg_speed_mps) * 2.2369, 1)                             AS avg_speed_mph
        FROM base
    """)
    s = system_rows[0] if system_rows else {}
    system = {
        "total_trips":        int(s.get("total_trips") or 0),
        "total_observations": int(s.get("total_observations") or 0),
        "vehicles_observed":  int(s.get("vehicles_observed") or 0),
        "on_time_pct":        float(s.get("on_time_pct") or 0),
        "within_5min_pct":    float(s.get("within_5min_pct") or 0),
        "within_7min_pct":    float(s.get("within_7min_pct") or 0),
        "late_pct":           float(s.get("late_pct") or 0),
        "early_pct":          float(s.get("early_pct") or 0),
        "p50_delay_minutes":  secs_to_min(s.get("p50_delay_seconds")) or 0.0,
        "p95_delay_minutes":  secs_to_min(s.get("p95_delay_seconds")) or 0.0,
        "avg_speed_mph":      float(s.get("avg_speed_mph") or 0),
    }

    print("Querying BQ for delay histogram...", file=sys.stderr)
    hist_rows = run_bq(f"""
        SELECT
          CASE
            WHEN delay_seconds < -120 THEN 'very_early'
            WHEN delay_seconds < -60  THEN 'early'
            WHEN delay_seconds <= 60  THEN 'on_time'
            WHEN delay_seconds <= 180 THEN 'slightly_late'
            WHEN delay_seconds <= 600 THEN 'late'
            ELSE                            'very_late'
          END AS bucket,
          COUNT(*) AS n
        FROM `{PROJECT_ID}.{DATASET}.trip_observations`
        WHERE service_date = "{service_date}"
          AND actual_arrival IS NOT NULL
          AND is_stale = FALSE
        GROUP BY bucket
    """)
    hist = {r["bucket"]: int(r["n"]) for r in hist_rows}
    bucket_order = ["very_early", "early", "on_time", "slightly_late", "late", "very_late"]
    bucket_labels = ["< -2 min", "-2 to -1 min", "-1 to +1 min",
                     "+1 to +3 min", "+3 to +10 min", "> +10 min"]

    print("Querying BQ for 1-minute delay histogram...", file=sys.stderr)
    minute_rows = run_bq(f"""
        SELECT
          CASE
            WHEN delay_seconds < -15*60 THEN -15
            WHEN delay_seconds >  45*60 THEN  45
            ELSE CAST(FLOOR(delay_seconds / 60.0) AS INT64)
          END AS minute,
          COUNT(*) AS n
        FROM `{PROJECT_ID}.{DATASET}.trip_observations`
        WHERE service_date = "{service_date}"
          AND actual_arrival IS NOT NULL
          AND is_stale = FALSE
        GROUP BY minute
        ORDER BY minute
    """)
    minute_histogram = [
        {"minute": int(r["minute"]), "count": int(r["n"])} for r in minute_rows
    ]

    output = {
        "service_date": str(service_date),
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "system": system,
        "schedule_compliance": {
            "scheduled_trips":         len(scheduled),
            "ran_trips":               len(ran_in_schedule),
            "dropped_trips":           len(dropped),
            "dropped_trip_ids_sample": dropped[:20],
        },
        "delay_histogram": {
            "buckets": bucket_order,
            "labels":  bucket_labels,
            "counts":  [hist.get(b, 0) for b in bucket_order],
        },
        "delay_minute_histogram": minute_histogram,
        "routes": routes,
    }

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(output, indent=2))

    print("", file=sys.stderr)
    print(f"Wrote {out}", file=sys.stderr)
    print(f"  scheduled trips: {output['schedule_compliance']['scheduled_trips']}", file=sys.stderr)
    print(f"  ran trips:       {output['schedule_compliance']['ran_trips']}", file=sys.stderr)
    print(f"  dropped trips:   {output['schedule_compliance']['dropped_trips']}", file=sys.stderr)
    print(f"  routes:          {len(routes)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
