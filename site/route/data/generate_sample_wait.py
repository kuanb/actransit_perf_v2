#!/usr/bin/env python3
"""
Generate a synthetic route_wait_sample.json fixture matching the shape
produced by cmd/scraper/route_wait_time.go. Values are
randomized-but-realistic: we sample headways at each (day_type, hour)
from an exponential distribution whose mean follows a plausible
diurnal pattern (frequent at peaks, sparse late at night) and then
apply the inspection-paradox transform to derive the wait-time density,
mean, and median that the frontend renders.

Usage:
    python3 generate_sample_wait.py
"""
import json
import math
import os
import random

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GTFS_PATH  = os.path.join(SCRIPT_DIR, "gtfs_route.json")
OUT_PATH   = os.path.join(SCRIPT_DIR, "route_wait_sample.json")
WEEK_END   = "2026-05-16"

# Match the Go-side constants so the JSON matches production output.
WAIT_HIST_BINS = 90
HEADWAY_MIN_MIN = 0.5   # 30 s in minutes
HEADWAY_MAX_MIN = 90.0  # 90 min cap

random.seed(7)


# Mean headway in minutes by PT hour. Weekday is the "tighter" shape
# with morning + evening peaks; weekend is sparser and lacks the peaks.
def weekday_mean_headway(hour):
    # 4–7 am morning ramp-up, 7–9 am peak, midday plateau, 4–7 pm peak,
    # taper into late evening. Round-trip through a piecewise function
    # rather than a sine so the shape resembles real schedules.
    if hour < 4:
        return 45.0  # owl service
    if 4 <= hour < 6:
        return 25.0  # ramp-up
    if 6 <= hour < 9:
        return 11.0  # morning peak
    if 9 <= hour < 15:
        return 16.0  # midday
    if 15 <= hour < 18:
        return 9.0   # evening peak
    if 18 <= hour < 21:
        return 17.0  # early evening
    return 30.0      # late evening


def weekend_mean_headway(hour):
    if hour < 6:
        return 60.0
    if 6 <= hour < 9:
        return 30.0
    if 9 <= hour < 19:
        return 22.0
    if 19 <= hour < 22:
        return 28.0
    return 45.0


def sample_headways(mean_h, n):
    """Inverse-CDF samples from Exp(1/mean_h), clipped to [0.5, 90]."""
    out = []
    for _ in range(n):
        u = random.random()
        # Avoid log(0)
        if u >= 1.0:
            u = 0.9999
        h = -math.log(1 - u) * mean_h
        if h < HEADWAY_MIN_MIN:
            h = HEADWAY_MIN_MIN
        if h > HEADWAY_MAX_MIN:
            h = HEADWAY_MAX_MIN
        out.append(h)
    return out


def bin_mass_from_headways(headways, bin_count=WAIT_HIST_BINS):
    """Pure-Python mirror of binMassFromHeadways in Go.

    Each bin [i, i+1) accumulates max(0, min(i+1, h) - i) for every
    observed headway h.
    """
    out = [0.0] * bin_count
    for h in headways:
        if h <= 0:
            continue
        full = min(int(h), bin_count)
        for i in range(full):
            out[i] += 1.0
        if full < bin_count:
            frac = h - full
            if frac > 0:
                out[full] += frac
    return out


def density_from_mass(mass):
    total = sum(mass)
    if total <= 0:
        return [0.0] * len(mass)
    return [round(v / total, 3) for v in mass]


def closed_form_mean_wait(mass):
    """Inspection-paradox mean from binned mass: Σ(i+0.5)·m_i / Σ m_i."""
    total = sum(mass)
    if total <= 0:
        return 0.0
    weighted = sum((i + 0.5) * m for i, m in enumerate(mass))
    return weighted / total


def median_from_density(density):
    """First bin where cumulative density crosses 0.5, interpolated."""
    if sum(density) <= 0:
        return None
    cum = 0.0
    for i, d in enumerate(density):
        if cum + d >= 0.5:
            if d <= 0:
                return round(float(i), 1)
            frac = (0.5 - cum) / d
            return round(i + frac, 1)
        cum += d
    return float(len(density) - 1)


def headway_p50(headways):
    if not headways:
        return None
    s = sorted(headways)
    return s[len(s) // 2]


def summary_for(headways):
    if not headways:
        return {
            "n": 0,
            "mean_headway_min": None,
            "p50_headway_min": None,
            "mean_wait_min": None,
            "median_wait_min": None,
        }
    mass = bin_mass_from_headways(headways)
    density = density_from_mass(mass)
    return {
        "n": len(headways),
        "mean_headway_min": round(sum(headways) / len(headways), 1),
        "p50_headway_min": round(headway_p50(headways), 1),
        "mean_wait_min": round(closed_form_mean_wait(mass), 1),
        "median_wait_min": median_from_density(density),
    }, mass, density


def block_for(day_type, mean_headway_fn):
    """Build one day-type block (summary + histogram + by_hour)."""
    all_headways = []
    by_hour_cells = []
    hour_masses = {}

    # Per-hour count: scale with diurnal frequency. ~30 buses/hour at
    # peak on a busy route × ~50 stops × ~7 days / day_type-count =
    # plenty of observations. We pick a fixed (smaller) sample for
    # readable histograms without flatlining at zero.
    for hour in range(24):
        mean_h = mean_headway_fn(hour)
        # Frequency-of-arrivals at a stop ≈ 1/mean_h per minute, so
        # over an hour we expect roughly 60/mean_h headways at one
        # stop. Scale by an arbitrary "stops × days" factor for a
        # large enough sample to give smooth shapes.
        n = max(8, int(60.0 / mean_h * 6.0))
        heads = sample_headways(mean_h, n)
        all_headways.extend(heads)
        s, mass, _density = summary_for(heads)
        hour_masses[hour] = mass
        by_hour_cells.append({
            "hour": hour,
            "n": s["n"],
            "p50_headway_min": s["p50_headway_min"],
            "mean_wait_min": s["mean_wait_min"],
            "median_wait_min": s["median_wait_min"],
        })

    overall_mass = bin_mass_from_headways(all_headways)
    overall_density = density_from_mass(overall_mass)

    summary = {
        "n": len(all_headways),
        "mean_headway_min": round(sum(all_headways) / len(all_headways), 1),
        "p50_headway_min": round(headway_p50(all_headways), 1),
        "mean_wait_min": round(closed_form_mean_wait(overall_mass), 1),
        "median_wait_min": median_from_density(overall_density),
    }

    histogram = {
        "bin_lo_min": list(range(WAIT_HIST_BINS)),
        "density":    overall_density,
    }

    return {
        "summary":   summary,
        "histogram": histogram,
        "by_hour":   by_hour_cells,
    }


def main():
    route_id = "51A"
    if os.path.exists(GTFS_PATH):
        with open(GTFS_PATH) as f:
            gtfs = json.load(f)
            route_id = gtfs.get("route_id", route_id)

    out = {
        "route_id": route_id,
        "week_end": WEEK_END,
        "days": {
            "weekday": block_for("weekday", weekday_mean_headway),
            "weekend": block_for("weekend", weekend_mean_headway),
        },
    }

    with open(OUT_PATH, "w") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {OUT_PATH}")
    for dt, block in out["days"].items():
        s = block["summary"]
        print(
            f"  {dt}: n={s['n']:>5d}  mean H={s['mean_headway_min']:>5.1f} min  "
            f"mean W={s['mean_wait_min']:>5.1f}  median W={s['median_wait_min']:>5.1f}"
        )


if __name__ == "__main__":
    main()
