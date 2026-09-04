#!/usr/bin/env python3

import csv
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path

import matplotlib.pyplot as plt
import requests


BASE = "https://storage.googleapis.com/transit-203605-actransit-cache"
ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"
PLOTS = ROOT.parent.parent / "site" / "presentations" / "2026-09-04" / "plots"

AGENCY = {
    "2026-01": {"service_operated_pct": 97.17, "otp_pct": 72.19, "operated_trips": 142220, "trips_not_operated": 4140},
    "2026-02": {"service_operated_pct": 97.28, "otp_pct": 74.43, "operated_trips": 129958, "trips_not_operated": 3627},
    "2026-03": {"service_operated_pct": 96.19, "otp_pct": 72.52, "operated_trips": 143331, "trips_not_operated": 5678},
    "2026-04": {"service_operated_pct": 96.74, "otp_pct": 74.75, "operated_trips": 139803, "trips_not_operated": 4713},
    "2026-05": {"service_operated_pct": 95.50, "otp_pct": 73.25, "operated_trips": 140278, "trips_not_operated": 6615},
    "2026-06": {"service_operated_pct": 94.24, "otp_pct": 74.72, "operated_trips": 133094, "trips_not_operated": 8135},
}

COLORS = {
    "agency": "#b22d42",
    "ours": "#146c78",
    "scheduled": "#74518d",
    "gold": "#a66a12",
    "ink": "#17262e",
    "muted": "#63717a",
    "paper": "#f5f0e7",
    "grid": "#d9d1c5",
}


def fetch_json(path):
    response = requests.get(f"{BASE}/{path}", timeout=30)
    response.raise_for_status()
    return response.json()


def pct(numerator, denominator):
    return round(100 * numerator / denominator, 1) if denominator else None


def round_metric(value, digits=1):
    return round(value, digits) if value is not None else None


def aggregate_daily(month, reports):
    selected = [report for report in reports if report["service_date"].startswith(month)]
    scheduled_trips = sum(report["schedule_compliance"]["scheduled_trips"] for report in selected)
    ran_trips = sum(report["schedule_compliance"]["ran_trips"] for report in selected)
    stop_total = sum(report["schedule_compliance"].get("stop_sd_n", 0) for report in selected)
    stop_delivered = sum(report["schedule_compliance"].get("stop_sd_delivered_n", 0) for report in selected)

    delay_counts = defaultdict(int)
    for report in selected:
        for bucket in report.get("delay_minute_histogram", []):
            delay_counts[int(bucket["minute"])] += int(bucket["count"])
    observed_stops = sum(delay_counts.values())
    late_10_29 = sum(count for minute, count in delay_counts.items() if 10 <= minute < 30)
    late_30_plus = sum(count for minute, count in delay_counts.items() if minute >= 30)

    bunching = [report.get("system", {}).get("bunching") for report in selected]
    bunching = [value for value in bunching if value and value.get("methodology_version") == 3]
    comparison_n = sum(value.get("comparison_n", 0) for value in bunching)
    bunched_n = sum(value.get("bunched_headway_n", 0) for value in bunching)
    long_n = sum(value.get("long_gap_n", 0) for value in bunching)
    aggregation = defaultdict(float)
    for value in bunching:
        for key, number in value.get("aggregation", {}).items():
            aggregation[key] += number

    cv_weight = aggregation["cv_weight"]
    comparable_seconds = aggregation["comparable_headway_seconds"]
    expected_wait = None
    even_wait = None
    if comparable_seconds:
        expected_wait = aggregation["comparable_headway_squared_seconds"] / (2 * comparable_seconds) / 60
        even_wait = aggregation["even_spacing_wait_area_seconds_squared"] / comparable_seconds / 60

    return {
        "month": month,
        "days_available": len(selected),
        "scheduled_trips": scheduled_trips,
        "ran_trips": ran_trips,
        "trip_delivery_pct": pct(ran_trips, scheduled_trips),
        "stop_sd_n": stop_total,
        "stop_sd_delivered_n": stop_delivered,
        "on_time_service_delivered_pct": pct(stop_delivered, stop_total),
        "observed_stop_arrivals": observed_stops,
        "late_10_29_n": late_10_29,
        "late_10_29_pct": pct(late_10_29, observed_stops),
        "late_30_plus_n": late_30_plus,
        "late_30_plus_pct": pct(late_30_plus, observed_stops),
        "headway_comparisons": comparison_n,
        "headway_cv": round_metric(aggregation["cv_weighted_sum"] / cv_weight, 2) if cv_weight else None,
        "bunched_headway_pct": pct(bunched_n, comparison_n),
        "long_gap_pct": pct(long_n, comparison_n),
        "expected_wait_min": round_metric(expected_wait),
        "even_spacing_wait_min": round_metric(even_wait),
        "spacing_penalty_min": round_metric(max(0, expected_wait - even_wait)) if expected_wait is not None else None,
    }


def configure_axes(axis):
    axis.set_facecolor("#fffdf9")
    axis.spines[["top", "right"]].set_visible(False)
    axis.spines[["left", "bottom"]].set_color(COLORS["grid"])
    axis.grid(axis="y", color=COLORS["grid"], linewidth=0.8, alpha=0.75)
    axis.tick_params(colors=COLORS["muted"], labelsize=10)


def save_figure(fig, name):
    fig.patch.set_facecolor(COLORS["paper"])
    fig.savefig(PLOTS / name, dpi=180, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)


def plot_service_operated(ours):
    months = list(AGENCY)
    labels = [datetime.strptime(month, "%Y-%m").strftime("%b") for month in months]
    agency_values = [AGENCY[month]["service_operated_pct"] for month in months]
    ours_by_month = {row["month"]: row for row in ours}
    our_values = [ours_by_month.get(month, {}).get("our_service_operated_pct") for month in months]

    fig, axis = plt.subplots(figsize=(9.3, 5.2))
    configure_axes(axis)
    axis.plot(labels, agency_values, color=COLORS["agency"], marker="o", linewidth=2.7, label="AC Transit reported")
    axis.plot(labels, our_values, color=COLORS["ours"], marker="o", linewidth=2.7, label="Independent tracker")
    axis.axhline(99.5, color=COLORS["muted"], linestyle=(0, (2, 3)), linewidth=1.4, label="District target: 99.5%")
    axis.set_ylim(89, 100.5)
    axis.set_ylabel("Percent of scheduled trips", color=COLORS["muted"])
    axis.set_title("The independent feed sees an even deeper service shortfall", loc="left", color=COLORS["ink"], fontsize=17, fontweight="bold", pad=18)
    axis.text(0, 1.02, "January–June 2026 · April tracker point covers April 18–30 only", transform=axis.transAxes, color=COLORS["muted"], fontsize=10)
    for index, value in enumerate(agency_values):
        axis.annotate(f"{value:.2f}%", (index, value), xytext=(0, 8), textcoords="offset points", ha="center", color=COLORS["agency"], fontsize=9)
    for index, value in enumerate(our_values):
        if value is not None:
            axis.annotate(f"{value:.1f}%", (index, value), xytext=(0, -16), textcoords="offset points", ha="center", color=COLORS["ours"], fontsize=9)
    axis.legend(frameon=False, loc="lower left", ncol=1)
    fig.tight_layout()
    save_figure(fig, "service-operated-comparison.png")


def plot_otp(monthly):
    months = list(AGENCY)
    labels = [datetime.strptime(month, "%Y-%m").strftime("%b") for month in months]
    agency_values = [AGENCY[month]["otp_pct"] for month in months]
    monthly_by_month = {row["month"]: row for row in monthly}
    ours_operated = [monthly_by_month.get(month, {}).get("our_otp_operated_pct") for month in months]
    ours_scheduled = [monthly_by_month.get(month, {}).get("our_otp_scheduled_pct") for month in months]

    fig, axis = plt.subplots(figsize=(9.3, 5.2))
    configure_axes(axis)
    axis.plot(labels, agency_values, color=COLORS["agency"], marker="o", linewidth=2.7, label="AC Transit departure OTP")
    axis.plot(labels, ours_operated, color=COLORS["ours"], marker="o", linewidth=2.7, label="Arrival proxy · operated trips")
    axis.plot(labels, ours_scheduled, color=COLORS["scheduled"], marker="o", linewidth=2.3, label="Arrival proxy · all scheduled trips")
    axis.axhline(75, color=COLORS["muted"], linestyle=(0, (2, 3)), linewidth=1.4, label="District target: 75%")
    axis.set_ylim(55, 78)
    axis.set_ylabel("Percent of timepoints", color=COLORS["muted"])
    axis.set_title("The tracker does not see the agency's June punctuality rebound", loc="left", color=COLORS["ink"], fontsize=17, fontweight="bold", pad=18)
    axis.text(0, 1.02, "Different underlying measures: agency departures vs. independently detected arrivals", transform=axis.transAxes, color=COLORS["muted"], fontsize=10)
    for index, value in enumerate(agency_values):
        axis.annotate(f"{value:.2f}%", (index, value), xytext=(0, 8), textcoords="offset points", ha="center", color=COLORS["agency"], fontsize=9)
    axis.legend(frameon=False, loc="lower left")
    fig.tight_layout()
    save_figure(fig, "on-time-comparison.png")


def plot_late_severity(ours):
    rows = [row for row in ours if row["month"] >= "2026-05"]
    labels = [datetime.strptime(row["month"], "%Y-%m").strftime("%b") for row in rows]
    late_10 = [row["late_10_29_pct"] for row in rows]
    late_30 = [row["late_30_plus_pct"] for row in rows]
    x = range(len(labels))

    fig, axis = plt.subplots(figsize=(9.3, 5.2))
    configure_axes(axis)
    axis.bar(x, late_10, color=COLORS["gold"], label="10–29 minutes late")
    axis.bar(x, late_30, bottom=late_10, color=COLORS["agency"], label="30+ minutes late")
    axis.set_xticks(list(x), labels)
    axis.set_ylabel("Share of observed stop arrivals", color=COLORS["muted"])
    axis.set_ylim(0, 7.2)
    fig.suptitle("One on-time percentage cannot distinguish 10 minutes late from 30", x=0.06, ha="left", color=COLORS["ink"], fontsize=17, fontweight="bold")
    fig.text(0.06, 0.91, "Independent arrivals · May–August 2026", color=COLORS["muted"], fontsize=10)
    for index, row in enumerate(rows):
        axis.text(index, late_10[index] / 2, f"{late_10[index]:.1f}%", ha="center", va="center", color="white", fontsize=10, fontweight="bold")
        axis.text(index, late_10[index] + late_30[index] / 2, f"{late_30[index]:.1f}%", ha="center", va="center", color="white", fontsize=10, fontweight="bold")
    axis.legend(frameon=False, loc="upper center", ncol=2)
    fig.tight_layout(rect=(0, 0, 1, 0.84))
    save_figure(fig, "late-severity.png")


def plot_spacing(ours):
    rows = [row for row in ours if row["month"] >= "2026-05"]
    labels = [datetime.strptime(row["month"], "%Y-%m").strftime("%b") for row in rows]

    fig, axes = plt.subplots(1, 2, figsize=(10.5, 5.2))
    for axis in axes:
        configure_axes(axis)

    axes[0].plot(labels, [row["headway_cv"] for row in rows], color=COLORS["ours"], marker="o", linewidth=2.7)
    axes[0].set_ylim(0.18, 0.25)
    axes[0].set_title("Headway variation", loc="left", color=COLORS["ink"], fontsize=14, fontweight="bold")
    axes[0].set_ylabel("Headway CV · lower is better", color=COLORS["muted"])
    for index, row in enumerate(rows):
        axes[0].annotate(f'{row["headway_cv"]:.2f}', (index, row["headway_cv"]), xytext=(0, 8), textcoords="offset points", ha="center", color=COLORS["ours"], fontsize=10)

    axes[1].plot(labels, [row["long_gap_pct"] for row in rows], color=COLORS["agency"], marker="o", linewidth=2.7, label="Long gaps")
    axes[1].plot(labels, [row["bunched_headway_pct"] for row in rows], color=COLORS["gold"], marker="o", linewidth=2.7, label="Bunched arrivals")
    axes[1].set_ylim(0, 15)
    axes[1].set_title("Too far apart or too close", loc="left", color=COLORS["ink"], fontsize=14, fontweight="bold")
    axes[1].set_ylabel("Share of comparable headways", color=COLORS["muted"])
    axes[1].legend(frameon=False, loc="upper left")

    fig.suptitle("Bus spacing was worsening after the packet's reporting window", x=0.06, ha="left", color=COLORS["ink"], fontsize=17, fontweight="bold")
    fig.text(0.06, 0.92, "Independent arrivals · May–August 2026", color=COLORS["muted"], fontsize=10)
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    save_figure(fig, "spacing-regularity.png")


def main():
    RESULTS.mkdir(parents=True, exist_ok=True)
    PLOTS.mkdir(parents=True, exist_ok=True)

    index = fetch_json("stats/_index.json")
    dates = [value for value in index["dates"] if "2026-04-01" <= value <= "2026-08-31"]
    with ThreadPoolExecutor(max_workers=12) as executor:
        reports = list(executor.map(lambda value: fetch_json(f"stats/{value}.json"), sorted(dates)))
    ours = [aggregate_daily(f"2026-{month:02d}", reports) for month in range(4, 9)]

    monthly = []
    for month in range(4, 9):
        value = f"2026-{month:02d}"
        report = fetch_json(f"stats/monthly/{value}.json")
        kpi = report["agency_kpi"]
        row = next(item for item in ours if item["month"] == value)
        row["our_scheduled_trips"] = kpi["service_operated"]["scheduled_trips"]
        row["our_operated_trips"] = kpi["service_operated"]["operated_trips"]
        row["our_service_operated_pct"] = kpi["service_operated"]["operated_pct"]
        row["our_otp_operated_pct"] = kpi["on_time_performance"]["of_operated_pct"]
        row["our_otp_scheduled_pct"] = kpi["on_time_performance"]["of_scheduled_pct"]
        monthly.append(row)

    for month, values in AGENCY.items():
        values["scheduled_trips"] = values["operated_trips"] + values["trips_not_operated"]
        if month in {"2026-05", "2026-06"}:
            independent = next(item for item in monthly if item["month"] == month)
            values["independent_operated_trips"] = independent["our_operated_trips"]
            values["independent_scheduled_trips"] = independent["our_scheduled_trips"]
            values["operated_trip_gap"] = independent["our_operated_trips"] - values["operated_trips"]

    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "source_period": {"start": "2026-01-01", "end": "2026-06-30"},
        "independent_data_period": {"start": min(dates), "end": max(dates)},
        "agency": AGENCY,
        "independent_monthly": monthly,
    }
    (RESULTS / "summary.json").write_text(json.dumps(payload, indent=2) + "\n")

    with (RESULTS / "monthly-comparison.csv").open("w", newline="") as stream:
        fieldnames = list(monthly[0])
        writer = csv.DictWriter(stream, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(monthly)

    plot_service_operated(monthly)
    plot_otp(monthly)
    plot_late_severity(monthly)
    plot_spacing(monthly)


if __name__ == "__main__":
    main()
