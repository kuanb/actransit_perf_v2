#!/usr/bin/env python3
"""
Generate a synthetic route_stop_stats.json fixture from the gtfs_route.json
fixture in this directory. Values are randomized-but-realistic, seeded for
reproducibility, and spread across the full green/yellow/red color bands so
every visual state is visible in the local dev view.

Usage:
    python3 generate_sample.py
"""
import json
import math
import os
import random
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GTFS_PATH  = os.path.join(SCRIPT_DIR, "gtfs_route.json")
OUT_PATH   = os.path.join(SCRIPT_DIR, "route_stop_stats.json")
WEEK_END   = "2026-05-10"

random.seed(42)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def gauss(mu, sigma, lo=None, hi=None):
    v = random.gauss(mu, sigma)
    if lo is not None:
        v = max(lo, v)
    if hi is not None:
        v = min(hi, v)
    return v


def main():
    with open(GTFS_PATH) as f:
        gtfs = json.load(f)

    route_id = gtfs["route_id"]

    # Collect all unique stop_ids across all trips
    stop_ids = set()
    for trip in gtfs["trips"].values():
        for st in trip["stop_times"]:
            stop_ids.add(st["stop_id"])

    stop_ids = sorted(stop_ids)
    n_stops = len(stop_ids)

    stops = {}
    for i, sid in enumerate(stop_ids):
        # Spread stops across a realistic range so all color bands appear.
        # Use a mix of patterns:
        #   - ~30% green stops (on-time, p95 <= +3 min)
        #   - ~35% yellow stops (p95 in 3–7 min)
        #   - ~35% red stops (p95 > 7 min)
        roll = random.random()

        if roll < 0.30:
            # Green stop: low delay, low variance
            p50 = gauss(30, 30, lo=-90, hi=120)   # typically on time
            p95 = gauss(120, 40, lo=p50, hi=180)   # p95 within green zone
            stddev = gauss(40, 20, lo=5, hi=90)
        elif roll < 0.65:
            # Yellow stop: moderate delay
            p50 = gauss(120, 60, lo=0, hi=300)
            p95 = gauss(280, 60, lo=p50, hi=420)
            stddev = gauss(100, 40, lo=30, hi=200)
        else:
            # Red stop: significant delay or high variance
            p50 = gauss(180, 90, lo=60, hi=600)
            p95 = gauss(480, 120, lo=max(p50, 300), hi=900)
            stddev = gauss(200, 80, lo=60, hi=400)

        n = int(gauss(400, 150, lo=20, hi=1200))

        stops[sid] = {
            "n":             n,
            "p50_delay_s":   round(p50, 1),
            "p95_delay_s":   round(p95, 1),
            "stddev_delay_s": round(stddev, 1),
        }

    out = {
        "route_id": route_id,
        "week_end": WEEK_END,
        "stops": stops,
    }

    with open(OUT_PATH, "w") as f:
        json.dump(out, f, indent=2)

    print(f"Written {len(stops)} stops → {OUT_PATH}")
    # Print distribution summary
    green  = sum(1 for s in stops.values() if s["p95_delay_s"] <= 180)
    yellow = sum(1 for s in stops.values() if 180 < s["p95_delay_s"] <= 420)
    red    = sum(1 for s in stops.values() if s["p95_delay_s"] > 420)
    print(f"  p95 distribution: green={green}, yellow={yellow}, red={red}")


if __name__ == "__main__":
    main()
