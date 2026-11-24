from __future__ import annotations

import csv
import io
import json
import os
import statistics
import urllib.request
from calendar import monthrange
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zipfile import ZipFile

import matplotlib.pyplot as plt
import numpy as np


ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"
PLOTS = ROOT / "site" / "presentations" / "2026-11-24-tempo" / "plots"
GTFS_DIR = Path(os.environ.get("ACTRANSIT_TEMPO_GTFS_DIR", "/tmp/actransit-tempo-gtfs-20261124"))
STATS_CACHE = Path(os.environ.get("ACTRANSIT_TEMPO_STATS_CACHE", "/tmp/actransit-tempo-stats-20261124"))
STATS_BASE = "https://storage.googleapis.com/transit-203605-actransit-cache/stats"
DAY_TYPES = ("weekday", "saturday", "sunday")
DAY_LABELS = {"weekday": "Weekday", "saturday": "Saturday", "sunday": "Sunday"}


def read_rows(archive: ZipFile, name: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.TextIOWrapper(archive.open(name), encoding="utf-8-sig", newline="")))


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def gtfs_seconds(value: str) -> int:
    hours, minutes, seconds = (int(part) for part in value.split(":"))
    return hours * 3600 + minutes * 60 + seconds


def day_type(value: date) -> str:
    if value.weekday() == 5:
        return "saturday"
    if value.weekday() == 6:
        return "sunday"
    return "weekday"


def service_sets(
    calendar: list[dict[str, str]],
    calendar_dates: list[dict[str, str]],
    first: date,
    last: date,
) -> dict[date, frozenset[str]]:
    exceptions: dict[date, list[tuple[str, str]]] = defaultdict(list)
    for row in calendar_dates:
        value = parse_date(row["date"])
        if first <= value <= last:
            exceptions[value].append((row["service_id"], row["exception_type"]))
    weekday_names = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
    result: dict[date, frozenset[str]] = {}
    current = first
    while current <= last:
        active = {
            row["service_id"]
            for row in calendar
            if parse_date(row["start_date"]) <= current <= parse_date(row["end_date"])
            and row[weekday_names[current.weekday()]] == "1"
        }
        for service_id, exception_type in exceptions.get(current, []):
            if exception_type == "1":
                active.add(service_id)
            elif exception_type == "2":
                active.discard(service_id)
        result[current] = frozenset(active)
        current += timedelta(days=1)
    return result


def standard_schedules(
    trips: list[dict[str, str]],
    first_departures: dict[str, int],
    services: dict[date, frozenset[str]],
) -> dict[str, tuple[date, tuple[tuple[int, int], ...]]]:
    by_service: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for trip in trips:
        departure = first_departures.get(trip["trip_id"])
        if departure is None:
            continue
        by_service[trip["service_id"]].append((int(trip["direction_id"]), departure))
    signatures: dict[str, Counter[tuple[tuple[int, int], ...]]] = {value: Counter() for value in DAY_TYPES}
    dates_by_signature: dict[tuple[str, tuple[tuple[int, int], ...]], list[date]] = defaultdict(list)
    for service_date, active in services.items():
        signature = tuple(sorted(item for service_id in active for item in by_service.get(service_id, [])))
        value = day_type(service_date)
        signatures[value][signature] += 1
        dates_by_signature[(value, signature)].append(service_date)
    result = {}
    for value in DAY_TYPES:
        signature, _ = signatures[value].most_common(1)[0]
        result[value] = (min(dates_by_signature[(value, signature)]), signature)
    return result


def summarize_signature(signature: tuple[tuple[int, int], ...]) -> dict:
    departures = {direction: sorted(seconds for value, seconds in signature if value == direction) for direction in (0, 1)}
    daytime_headways = []
    evening_headways = []
    for times in departures.values():
        daytime = [value for value in times if 6 * 3600 <= value < 19 * 3600]
        evening = [value for value in times if 19 * 3600 <= value < 24 * 3600]
        daytime_headways.extend((right - left) / 60 for left, right in zip(daytime, daytime[1:]))
        evening_headways.extend((right - left) / 60 for left, right in zip(evening, evening[1:]))
    return {
        "scheduled_trips": len(signature),
        "direction_0_trips": len(departures[0]),
        "direction_1_trips": len(departures[1]),
        "daytime_median_headway_min": statistics.median(daytime_headways),
        "daytime_p90_headway_min": float(np.quantile(daytime_headways, 0.9)),
        "daytime_max_headway_min": max(daytime_headways),
        "evening_median_headway_min": statistics.median(evening_headways),
        "evening_p90_headway_min": float(np.quantile(evening_headways, 0.9)),
        "evening_max_headway_min": max(evening_headways),
    }


def load_schedule_history() -> list[dict]:
    rows = []
    for path in GTFS_DIR.glob("*.zip"):
        with ZipFile(path) as archive:
            feed = read_rows(archive, "feed_info.txt")[0]
            first = parse_date(feed["feed_start_date"])
            last = parse_date(feed["feed_end_date"])
            trips = [row for row in read_rows(archive, "trips.txt") if row["route_id"] == "1T"]
            trip_ids = {row["trip_id"] for row in trips}
            first_departures: dict[str, tuple[int, int]] = {}
            for row in read_rows(archive, "stop_times.txt"):
                if row["trip_id"] not in trip_ids:
                    continue
                sequence = int(row["stop_sequence"])
                departure = gtfs_seconds(row["departure_time"])
                current = first_departures.get(row["trip_id"])
                if current is None or sequence < current[0]:
                    first_departures[row["trip_id"]] = (sequence, departure)
            departure_values = {trip_id: value[1] for trip_id, value in first_departures.items()}
            calendar_dates = read_rows(archive, "calendar_dates.txt") if "calendar_dates.txt" in archive.namelist() else []
            services = service_sets(read_rows(archive, "calendar.txt"), calendar_dates, first, last)
            schedules = standard_schedules(trips, departure_values, services)
            for value, (representative_date, signature) in schedules.items():
                rows.append(
                    {
                        "booking_id": path.stem,
                        "feed_version": feed.get("feed_version", ""),
                        "feed_start_date": first.isoformat(),
                        "feed_end_date": last.isoformat(),
                        "day_type": value,
                        "representative_date": representative_date.isoformat(),
                        **summarize_signature(signature),
                    }
                )
    return sorted(rows, key=lambda row: (row["feed_start_date"], DAY_TYPES.index(row["day_type"])))


def fetch_json(relative: str) -> dict:
    path = STATS_CACHE / relative
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(f"{STATS_BASE}/{relative}", path)
    return json.loads(path.read_text())


def load_monthly_delivery() -> tuple[list[dict], dict]:
    index = fetch_json("monthly/_index.json")
    rows = []
    october = {}
    for month in sorted(index["months"]):
        payload = fetch_json(f"monthly/{month}.json")
        if payload.get("status") != "complete" or month < "2026-05":
            continue
        route = next(value for value in payload["routes"] if value["route_id"] == "1T")
        operated = route["agency_kpi"]["service_operated"]
        weekday = route["agency_kpi_by_day_type"]["weekday"]
        weekday_scheduled = sum(value["service_operated"]["scheduled_trips"] for value in weekday.values())
        weekday_observed = sum(value["service_operated"]["operated_trips"] for value in weekday.values())
        year, month_number = (int(value) for value in month.split("-"))
        weekday_days = sum(
            1
            for day in range(1, monthrange(year, month_number)[1] + 1)
            if date(year, month_number, day).weekday() < 5
        )
        rows.append(
            {
                "month": month,
                "days_available": payload["days_available"],
                "scheduled_trips": operated["scheduled_trips"],
                "observed_running": operated["operated_trips"],
                "dropped_not_observed": operated["scheduled_trips"] - operated["operated_trips"],
                "observed_pct": operated["operated_pct"],
                "weekday_days": weekday_days,
                "weekday_scheduled_trips": weekday_scheduled,
                "weekday_observed_running": weekday_observed,
                "weekday_dropped_not_observed": weekday_scheduled - weekday_observed,
                "weekday_scheduled_per_day": round(weekday_scheduled / weekday_days, 1),
                "weekday_dropped_per_day": round((weekday_scheduled - weekday_observed) / weekday_days, 1),
                "weekday_dropped_pct": round(100 * (weekday_scheduled - weekday_observed) / weekday_scheduled, 1),
            }
        )
        if month == "2026-10":
            october = route["agency_kpi_by_day_type"]
    return rows, october


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def set_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.titlesize": 16,
            "axes.titleweight": "bold",
            "axes.labelcolor": "#40505c",
            "axes.edgecolor": "#d5d2ca",
            "axes.facecolor": "#ffffff",
            "figure.facecolor": "#f7f3ea",
            "xtick.color": "#596772",
            "ytick.color": "#596772",
            "grid.color": "#e5e1d8",
        }
    )


def plot_promise_comparison(launch: dict, current: dict) -> None:
    values = [5, launch["daytime_median_headway_min"], current["daytime_median_headway_min"], current["evening_median_headway_min"]]
    labels = ["Promised peak\nservice (2016)", "Launch GTFS\ndaytime (2020)", "Current GTFS\ndaytime (2026)", "Current GTFS\nevening (2026)"]
    colors = ["#d59638", "#6c7d87", "#782b84", "#a44f75"]
    fig, ax = plt.subplots(figsize=(10.5, 5.4))
    bars = ax.bar(np.arange(len(values)), values, color=colors, width=0.66)
    for bar, value in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, value + 0.35, f"{value:g} min", ha="center", va="bottom", fontweight="bold")
    ax.set_title("Promised and scheduled Line 1T headways", loc="left", pad=18)
    ax.set_ylabel("Median scheduled time between buses")
    ax.set_xticks(np.arange(len(labels)), labels)
    ax.set_ylim(0, 17)
    ax.grid(axis="y", alpha=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(PLOTS / "promise-vs-schedule.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_schedule_history(rows: list[dict], monthly: list[dict]) -> None:
    weekday = [row for row in rows if row["day_type"] == "weekday"]
    dates = [date.fromisoformat(row["feed_start_date"]) for row in weekday]
    headways = [row["daytime_median_headway_min"] for row in weekday]
    delivery_dates = [datetime.strptime(row["month"], "%Y-%m") for row in monthly]
    scheduled = [row["weekday_scheduled_per_day"] for row in monthly]
    dropped = [row["weekday_dropped_per_day"] for row in monthly]
    fig, axes = plt.subplots(2, 1, figsize=(11, 8.5), gridspec_kw={"height_ratios": [1, 1.18]})
    axes[0].plot(dates, headways, color="#782b84", marker="o", linewidth=2.4, markersize=5)
    axes[0].axhline(5, color="#d59638", linestyle="--", linewidth=2, label="2016 public promise")
    axes[0].set_title("Scheduled daytime headway in 25 GTFS bookings", loc="left", pad=16)
    axes[0].set_ylabel("Median headway (min)")
    axes[0].set_ylim(0, 16)
    axes[0].grid(axis="y", alpha=0.8)
    axes[0].legend(frameon=False, loc="lower right")
    axes[1].plot(delivery_dates, scheduled, color="#2f6f89", marker="o", linewidth=2.4, markersize=6, label="Scheduled")
    axes[1].plot(delivery_dates, dropped, color="#d59638", marker="o", linewidth=2.4, markersize=6, label="Dropped / not observed")
    for plotted_date, value, row in zip(delivery_dates, dropped, monthly):
        axes[1].annotate(
            f"{row['weekday_dropped_pct']:.1f}%",
            (plotted_date, value),
            xytext=(0, 9),
            textcoords="offset points",
            ha="center",
            color="#8b5a13",
            fontsize=8.5,
            fontweight="bold",
        )
    axes[1].set_title("Average scheduled and dropped / not observed trips per weekday", loc="left", pad=16)
    axes[1].set_ylabel("Trips per weekday, both directions")
    axes[1].set_ylim(0, 240)
    axes[1].set_xticks(delivery_dates, [value.strftime("%b") for value in delivery_dates])
    axes[1].grid(axis="y", alpha=0.8)
    axes[1].legend(frameon=False, ncol=2, loc="center left")
    for ax in axes:
        ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(PLOTS / "gtfs-schedule-history.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_monthly_delivery(rows: list[dict]) -> None:
    labels = [datetime.strptime(row["month"], "%Y-%m").strftime("%b") for row in rows]
    observed = np.array([row["observed_running"] for row in rows])
    dropped = np.array([row["dropped_not_observed"] for row in rows])
    x = np.arange(len(rows))
    fig, ax = plt.subplots(figsize=(10.5, 5.8))
    ax.bar(x, observed, color="#2f6f89", label="Observed running")
    ax.bar(x, dropped, bottom=observed, color="#d59638", label="Dropped / not observed")
    for index, row in enumerate(rows):
        ax.text(index, observed[index] + dropped[index] + 95, f"{dropped[index]:,}\n({100 - row['observed_pct']:.1f}%)", ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax.set_title("Monthly Line 1T trips observed in the public vehicle feed", loc="left", pad=18)
    ax.set_ylabel("Scheduled trips, both directions")
    ax.set_xticks(x, labels)
    ax.set_ylim(0, max(observed + dropped) * 1.18)
    ax.grid(axis="y", alpha=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, ncol=2, loc="upper left")
    fig.tight_layout()
    fig.savefig(PLOTS / "monthly-trip-delivery.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def october_rows(payload: dict) -> list[dict]:
    rows = []
    for day_value in ("weekday", "weekend"):
        for period in ("morning", "midday", "afternoon", "evening", "owl"):
            service = payload[day_value][period]["service_operated"]
            rows.append(
                {
                    "day_type": day_value,
                    "period": period,
                    "scheduled_trips": service["scheduled_trips"],
                    "observed_running": service["operated_trips"],
                    "dropped_not_observed": service["scheduled_trips"] - service["operated_trips"],
                    "observed_pct": service["operated_pct"],
                }
            )
    return rows


def main() -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    PLOTS.mkdir(parents=True, exist_ok=True)
    set_style()
    schedule = load_schedule_history()
    monthly, october_payload = load_monthly_delivery()
    october = october_rows(october_payload)
    write_csv(RESULTS / "schedule_by_booking.csv", schedule)
    write_csv(RESULTS / "monthly_delivery.csv", monthly)
    write_csv(RESULTS / "october_by_daypart.csv", october)
    launch = next(row for row in schedule if row["booking_id"] == "2008FA" and row["day_type"] == "weekday")
    current = next(row for row in schedule if row["booking_id"] == "2608FA" and row["day_type"] == "weekday")
    summary = {
        "launch_weekday": launch,
        "current_weekday": current,
        "current_saturday": next(row for row in schedule if row["booking_id"] == "2608FA" and row["day_type"] == "saturday"),
        "current_sunday": next(row for row in schedule if row["booking_id"] == "2608FA" and row["day_type"] == "sunday"),
        "monthly_delivery": monthly,
        "october_by_daypart": october,
    }
    (RESULTS / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    plot_promise_comparison(launch, current)
    plot_schedule_history(schedule, monthly)
    plot_monthly_delivery(monthly)


if __name__ == "__main__":
    main()
