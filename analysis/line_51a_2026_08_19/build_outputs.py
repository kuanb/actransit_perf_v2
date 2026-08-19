from __future__ import annotations

import csv
import io
import json
import math
import os
import re
import subprocess
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zipfile import ZipFile

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"
PLOTS = ROOT / "site" / "presentations" / "2026-08-19-line-51a" / "plots"
OBSERVATION_CACHE = Path(os.environ.get("ACTRANSIT_51A_OBSERVATION_CACHE", "/tmp/actransit-line51a-20260819-observations.csv"))
PACIFIC = "America/Los_Angeles"
START_DATE = date(2026, 6, 1)
END_DATE = date(2026, 7, 31)
EXCLUDED_DATES = {date(2026, 6, 14)}
HOLIDAYS = {date(2026, 6, 19), date(2026, 7, 4)}

GTFS_SOURCES = [
    {
        "name": "S1000251",
        "start": date(2026, 6, 1),
        "end": date(2026, 6, 13),
        "path": Path(os.environ.get("ACTRANSIT_GTFS_BASELINE", "/tmp/actransit-reliability-20260819/gtfs_versions/20260426T045642Z.zip")),
        "url": "https://storage.googleapis.com/transit-203605-actransit-cache/gtfs/20260426T045642Z.zip",
    },
    {
        "name": "S1000252",
        "start": date(2026, 6, 15),
        "end": date(2026, 7, 31),
        "path": Path(os.environ.get("ACTRANSIT_GTFS_SUMMER", "/tmp/actransit-reliability-20260819/gtfs_versions/20260615T050002Z.zip")),
        "url": "https://storage.googleapis.com/transit-203605-actransit-cache/gtfs/20260615T050002Z.zip",
    },
]

DIRECTION_LABELS = {0: "Fruitvale-bound", 1: "Rockridge-bound"}
DIRECTION_SHORT = {0: "To Fruitvale", 1: "To Rockridge"}
TIME_BANDS = ["Early AM", "AM peak", "Midday", "PM peak", "Evening", "Late night"]
TIME_COLORS = {
    "Early AM": "#80939b",
    "AM peak": "#d59638",
    "Midday": "#4d879b",
    "PM peak": "#a5213a",
    "Evening": "#5d4d7e",
    "Late night": "#2f3941",
}
DAY_TYPES = ["Weekday", "Saturday", "Sunday / holiday"]
TARGET_SEGMENTS = {
    (1, "6036", "6200"): "11th → 15th",
    (1, "2489", "2507"): "20th → Grand",
    (0, "5224", "2476"): "17th → 12th",
    (0, "2511", "2487"): "Grand → 19th",
}
SCREEN_STOPS = {0: "5224", 1: "6200"}


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


def ensure_gtfs(source: dict) -> Path:
    path = source["path"]
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(source["url"], path)
    return path


def read_zip_rows(archive: ZipFile, name: str):
    return csv.DictReader(io.TextIOWrapper(archive.open(name), encoding="utf-8-sig", newline=""))


def gtfs_seconds(value: str) -> int:
    hours, minutes, seconds = (int(part) for part in value.split(":"))
    return hours * 3600 + minutes * 60 + seconds


def service_dates_for_feed(archive: ZipFile, first: date, last: date) -> dict[date, set[str]]:
    calendar = list(read_zip_rows(archive, "calendar.txt"))
    exceptions: dict[date, list[tuple[str, str]]] = defaultdict(list)
    for row in read_zip_rows(archive, "calendar_dates.txt"):
        value = datetime.strptime(row["date"], "%Y%m%d").date()
        if first <= value <= last:
            exceptions[value].append((row["service_id"], row["exception_type"]))
    weekday_names = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    active: dict[date, set[str]] = {}
    current = first
    while current <= last:
        values = {
            row["service_id"]
            for row in calendar
            if datetime.strptime(row["start_date"], "%Y%m%d").date() <= current <= datetime.strptime(row["end_date"], "%Y%m%d").date()
            and row[weekday_names[current.weekday()]] == "1"
        }
        for service_id, exception_type in exceptions.get(current, []):
            if exception_type == "1":
                values.add(service_id)
            elif exception_type == "2":
                values.discard(service_id)
        active[current] = values
        current += timedelta(days=1)
    return active


def load_schedule() -> tuple[pd.DataFrame, pd.DataFrame]:
    schedule_rows: list[dict] = []
    stop_records: dict[tuple[str, str], dict] = {}
    for source in GTFS_SOURCES:
        path = ensure_gtfs(source)
        with ZipFile(path) as archive:
            stops = {row["stop_id"]: row for row in read_zip_rows(archive, "stops.txt")}
            trips = [row for row in read_zip_rows(archive, "trips.txt") if row["route_id"] == "51A"]
            trips_by_id = {row["trip_id"]: row for row in trips}
            stop_times: dict[str, list[dict]] = defaultdict(list)
            for row in read_zip_rows(archive, "stop_times.txt"):
                if row["trip_id"] in trips_by_id:
                    stop_times[row["trip_id"]].append(row)
            active_services = service_dates_for_feed(archive, source["start"], source["end"])
            for service_date, service_ids in active_services.items():
                if service_date in EXCLUDED_DATES:
                    continue
                for trip_id, trip in trips_by_id.items():
                    if trip["service_id"] not in service_ids:
                        continue
                    direction_id = int(trip["direction_id"])
                    for stop_time in stop_times[trip_id]:
                        stop = stops[stop_time["stop_id"]]
                        stop_records[(source["name"], stop["stop_id"])] = {
                            "feed": source["name"],
                            "stop_id": stop["stop_id"],
                            "stop_name": stop["stop_name"],
                            "lat": float(stop["stop_lat"]),
                            "lon": float(stop["stop_lon"]),
                        }
                        schedule_rows.append(
                            {
                                "feed": source["name"],
                                "service_date": service_date.isoformat(),
                                "trip_id": trip_id,
                                "direction_id": direction_id,
                                "direction": DIRECTION_LABELS[direction_id],
                                "headsign": trip.get("trip_headsign", ""),
                                "stop_sequence": int(stop_time["stop_sequence"]),
                                "stop_id": stop["stop_id"],
                                "stop_name": stop["stop_name"],
                                "stop_lat": float(stop["stop_lat"]),
                                "stop_lon": float(stop["stop_lon"]),
                                "scheduled_seconds": gtfs_seconds(stop_time["arrival_time"]),
                                "shape_dist_m": float(stop_time["shape_dist_traveled"]),
                            }
                        )
    schedule = pd.DataFrame(schedule_rows)
    schedule["service_date_value"] = pd.to_datetime(schedule["service_date"])
    schedule["day_type"] = schedule["service_date_value"].map(day_type)
    schedule["scheduled_hour"] = schedule["scheduled_seconds"] / 3600
    schedule["scheduled_band"] = schedule["scheduled_hour"].map(time_band)
    return schedule, pd.DataFrame(stop_records.values())


def day_type(value: pd.Timestamp | datetime | date) -> str:
    if isinstance(value, pd.Timestamp):
        value = value.date()
    elif isinstance(value, datetime):
        value = value.date()
    if value in HOLIDAYS or value.weekday() == 6:
        return "Sunday / holiday"
    if value.weekday() == 5:
        return "Saturday"
    return "Weekday"


def time_band(hour: float) -> str:
    hour = hour % 24
    if 5 <= hour < 6:
        return "Early AM"
    if 6 <= hour < 9:
        return "AM peak"
    if 9 <= hour < 15:
        return "Midday"
    if 15 <= hour < 19:
        return "PM peak"
    if 19 <= hour < 24:
        return "Evening"
    return "Late night"


def load_observations() -> pd.DataFrame:
    if OBSERVATION_CACHE.exists():
        payload = OBSERVATION_CACHE.read_text()
    else:
        sql = (HERE / "sql" / "observations.sql").read_text()
        command = [
            "bq",
            "--quiet",
            "query",
            "--use_legacy_sql=false",
            "--format=csv",
            "--max_rows=1000000",
            sql,
        ]
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        payload = result.stdout
        OBSERVATION_CACHE.write_text(payload)
    obs = pd.read_csv(
        io.StringIO(payload),
        dtype={"service_date": str, "trip_id": str, "vehicle_id": str, "stop_id": str},
    )
    obs["stop_sequence"] = obs["stop_sequence"].astype(int)
    obs["actual_arrival"] = pd.to_datetime(obs["actual_arrival"], utc=True, errors="coerce")
    obs["actual_local"] = obs["actual_arrival"].dt.tz_convert(PACIFIC)
    return obs


def short_stop_name(name: str) -> str:
    if "(20th St)" in name:
        return "20th"
    name = re.sub(r" \([^)]*\)", "", name)
    name = name.replace("Broadway & ", "").replace("8th St & Broadway", "8th")
    name = name.replace("College Av & Broadway", "College")
    name = name.replace("W MacArthur Blvd", "MacArthur")
    name = name.replace("W Grand Av", "Grand Av")
    name = name.replace(" St", "").replace(" Av", "")
    return name


def prepare_merged(schedule: pd.DataFrame, obs: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    merged = schedule.merge(
        obs,
        on=["service_date", "trip_id", "stop_sequence", "stop_id"],
        how="left",
        validate="one_to_one",
    )
    observed_keys = obs[["service_date", "trip_id"]].drop_duplicates()
    scheduled_keys = schedule[["service_date", "trip_id"]].drop_duplicates()
    unmatched = observed_keys.merge(scheduled_keys, on=["service_date", "trip_id"], how="left", indicator=True)
    merged["trip_observed_any"] = merged.groupby(["service_date", "trip_id"])["actual_arrival"].transform("count").gt(0)
    audit = {
        "scheduled_rows": int(len(schedule)),
        "deduped_observation_rows": int(len(obs)),
        "raw_observation_rows": int(obs["finalization_copies"].sum()),
        "actual_arrivals": int(obs["actual_arrival"].notna().sum()),
        "scheduled_trips": int(schedule[["service_date", "trip_id"]].drop_duplicates().shape[0]),
        "observed_trips": int(merged.loc[merged["trip_observed_any"], ["service_date", "trip_id"]].drop_duplicates().shape[0]),
        "unmatched_observed_trips": int((unmatched["_merge"] == "left_only").sum()),
        "service_days": int(schedule["service_date"].nunique()),
    }
    audit["duplicate_pct"] = 100 * (audit["raw_observation_rows"] - audit["deduped_observation_rows"]) / audit["raw_observation_rows"]
    return merged, audit


def prepare_segments(merged: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    values = merged.sort_values(["service_date", "trip_id", "stop_sequence"]).copy()
    groups = values.groupby(["service_date", "trip_id"], sort=False)
    for column in ["stop_sequence", "stop_id", "stop_name", "stop_lat", "stop_lon", "shape_dist_m", "scheduled_seconds", "actual_local"]:
        values[f"next_{column}"] = groups[column].shift(-1)
    values = values[values["next_stop_sequence"] == values["stop_sequence"] + 1].copy()
    values["segment_id"] = values["stop_id"] + "-" + values["next_stop_id"].astype(str)
    values["segment_label"] = values["stop_name"].map(short_stop_name) + " → " + values["next_stop_name"].map(short_stop_name)
    values["segment_distance_m"] = values["next_shape_dist_m"] - values["shape_dist_m"]
    values["scheduled_runtime_s"] = values["next_scheduled_seconds"] - values["scheduled_seconds"]
    values["runtime_s"] = (values["next_actual_local"] - values["actual_local"]).dt.total_seconds()
    values["actual_hour"] = values["actual_local"].dt.hour + values["actual_local"].dt.minute / 60 + values["actual_local"].dt.second / 3600
    values["actual_band"] = values["actual_hour"].map(time_band)
    values["speed_mph"] = values["segment_distance_m"] / values["runtime_s"] * 2.236936
    values["is_target"] = values.apply(
        lambda row: (int(row["direction_id"]), str(row["stop_id"]), str(row["next_stop_id"])) in TARGET_SEGMENTS,
        axis=1,
    )
    values["target_label"] = values.apply(
        lambda row: TARGET_SEGMENTS.get((int(row["direction_id"]), str(row["stop_id"]), str(row["next_stop_id"])), ""),
        axis=1,
    )
    corridor = values[
        values["stop_name"].str.contains("Broadway", na=False)
        & values["next_stop_name"].str.contains("Broadway", na=False)
        & (values["stop_lat"] >= 37.798)
        & (values["next_stop_lat"] >= 37.798)
    ].copy()
    valid = corridor[
        corridor["runtime_s"].notna()
        & corridor["segment_distance_m"].gt(0)
        & corridor["runtime_s"].between(10, 1800)
    ].copy()
    return corridor, valid


def quantile(series: pd.Series, probability: float) -> float:
    return float(series.quantile(probability)) if len(series) else math.nan


def bootstrap_daily_ci(frame: pd.DataFrame, value: str = "runtime_s") -> tuple[float, float]:
    daily = frame.groupby("service_date")[value].median().dropna().to_numpy()
    if len(daily) < 5:
        return math.nan, math.nan
    rng = np.random.default_rng(51)
    samples = rng.choice(daily, size=(2000, len(daily)), replace=True)
    estimates = np.median(samples, axis=1)
    return tuple(np.quantile(estimates, [0.025, 0.975]))


def baseline_for_segment(frame: pd.DataFrame) -> tuple[float, str, int]:
    early = frame[(frame["day_type"] == "Weekday") & frame["actual_hour"].between(5, 7, inclusive="left")]
    if len(early) >= 20:
        return float(early["runtime_s"].median()), "weekday 5–7 a.m. median", len(early)
    return quantile(frame["runtime_s"], 0.15), "15th percentile", len(frame)


def segment_summary(valid: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (direction_id, segment_id), segment in valid.groupby(["direction_id", "segment_id"]):
        baseline, baseline_method, baseline_n = baseline_for_segment(segment)
        for (day, band), frame in segment.groupby(["day_type", "actual_band"]):
            low, high = bootstrap_daily_ci(frame)
            rows.append(
                {
                    "direction_id": direction_id,
                    "direction": DIRECTION_LABELS[int(direction_id)],
                    "segment_id": segment_id,
                    "segment_label": frame["segment_label"].iloc[0],
                    "upstream_stop_id": frame["stop_id"].iloc[0],
                    "downstream_stop_id": frame["next_stop_id"].iloc[0],
                    "day_type": day,
                    "time_band": band,
                    "n": len(frame),
                    "service_days": frame["service_date"].nunique(),
                    "distance_m": frame["segment_distance_m"].median(),
                    "scheduled_runtime_s": frame["scheduled_runtime_s"].median(),
                    "median_s": frame["runtime_s"].median(),
                    "p25_s": quantile(frame["runtime_s"], 0.25),
                    "p75_s": quantile(frame["runtime_s"], 0.75),
                    "p90_s": quantile(frame["runtime_s"], 0.90),
                    "p95_s": quantile(frame["runtime_s"], 0.95),
                    "max_s": frame["runtime_s"].max(),
                    "median_speed_mph": frame["speed_mph"].median(),
                    "baseline_s": baseline,
                    "baseline_method": baseline_method,
                    "baseline_n": baseline_n,
                    "slowdown_pct": 100 * (frame["runtime_s"].median() / baseline - 1),
                    "schedule_overrun_pct": 100 * (frame["runtime_s"].median() / frame["scheduled_runtime_s"].median() - 1),
                    "median_ci_low_s": low,
                    "median_ci_high_s": high,
                    "is_target": bool(frame["is_target"].iloc[0]),
                    "target_label": frame["target_label"].iloc[0],
                }
            )
    return pd.DataFrame(rows)


def coverage_summary(merged: pd.DataFrame) -> pd.DataFrame:
    trip = merged.groupby(["service_date", "trip_id"], as_index=False).agg(
        trip_observed=("actual_arrival", lambda values: values.notna().any())
    )
    daily_trips = trip.groupby("service_date", as_index=False).agg(
        scheduled_trips=("trip_id", "size"), observed_trips=("trip_observed", "sum")
    )
    daily_stops = merged.groupby("service_date", as_index=False).agg(
        scheduled_stops=("stop_id", "size"), observed_arrivals=("actual_arrival", lambda values: values.notna().sum())
    )
    result = daily_trips.merge(daily_stops, on="service_date")
    result["trip_delivery_pct"] = 100 * result["observed_trips"] / result["scheduled_trips"]
    result["stop_observation_pct"] = 100 * result["observed_arrivals"] / result["scheduled_stops"]
    result["day_type"] = pd.to_datetime(result["service_date"]).map(day_type)
    return result


def prepare_screenline(merged: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    screen = merged[merged.apply(lambda row: str(row["stop_id"]) == SCREEN_STOPS[int(row["direction_id"])], axis=1)].copy()
    screen = screen.sort_values(["service_date", "direction_id", "scheduled_seconds"])
    screen["schedule_index"] = screen.groupby(["service_date", "direction_id"]).cumcount()
    screen["scheduled_headway_min"] = screen.groupby(["service_date", "direction_id"])["scheduled_seconds"].diff() / 60
    screen["status"] = np.select(
        [screen["actual_arrival"].notna(), screen["trip_observed_any"]],
        ["Observed at screenline", "Observed elsewhere only"],
        default="No route observation",
    )
    screen["scheduled_hour_int"] = np.floor(screen["scheduled_hour"] % 24).astype(int)

    actual = screen[screen["actual_arrival"].notna()].sort_values(["service_date", "direction_id", "actual_arrival"]).copy()
    actual["headway_min"] = actual.groupby(["service_date", "direction_id"])["actual_arrival"].diff().dt.total_seconds() / 60
    actual["previous_schedule_index"] = actual.groupby(["service_date", "direction_id"])["schedule_index"].shift(1)
    actual["missed_slots"] = actual["schedule_index"] - actual["previous_schedule_index"] - 1
    actual["actual_hour"] = actual["actual_local"].dt.hour + actual["actual_local"].dt.minute / 60
    actual["actual_band"] = actual["actual_hour"].map(time_band)
    actual = actual[actual["headway_min"].between(1, 180)].copy()

    scheduled_reference = (
        screen[screen["scheduled_headway_min"].between(1, 180)]
        .groupby(["direction_id", "day_type", "scheduled_band"], as_index=False)["scheduled_headway_min"]
        .median()
        .rename(columns={"scheduled_headway_min": "reference_scheduled_headway_min", "scheduled_band": "actual_band"})
    )
    actual = actual.merge(scheduled_reference, on=["direction_id", "day_type", "actual_band"], how="left")
    actual["headway_ratio"] = actual["headway_min"] / actual["reference_scheduled_headway_min"]
    return screen, actual, scheduled_reference


def headway_summary(actual: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for keys, frame in actual.groupby(["direction_id", "day_type", "actual_band"]):
        direction_id, day, band = keys
        values = frame["headway_min"]
        schedule = frame["reference_scheduled_headway_min"].median()
        rows.append(
            {
                "direction_id": direction_id,
                "direction": DIRECTION_LABELS[int(direction_id)],
                "day_type": day,
                "time_band": band,
                "n": len(values),
                "service_days": frame["service_date"].nunique(),
                "scheduled_headway_min": schedule,
                "p25_min": quantile(values, 0.25),
                "median_min": values.median(),
                "p75_min": quantile(values, 0.75),
                "p90_min": quantile(values, 0.90),
                "p95_min": quantile(values, 0.95),
                "p99_min": quantile(values, 0.99),
                "max_min": values.max(),
                "mean_min": values.mean(),
                "cv": values.std() / values.mean(),
                "mean_rider_wait_min": (values.pow(2).sum() / (2 * values.sum())),
                "bunching_pct": 100 * (values < 0.5 * schedule).mean(),
                "gap_1_5x_pct": 100 * (values > 1.5 * schedule).mean(),
                "gap_2x_pct": 100 * (values > 2 * schedule).mean(),
                "intervals_with_missing_slots_pct": 100 * frame["missed_slots"].gt(0).mean(),
            }
        )
    return pd.DataFrame(rows)


def delivery_summary(screen: pd.DataFrame) -> pd.DataFrame:
    result = screen.groupby(["direction_id", "direction", "day_type", "scheduled_band", "status"], as_index=False).size()
    totals = result.groupby(["direction_id", "direction", "day_type", "scheduled_band"])["size"].transform("sum")
    result["pct"] = 100 * result["size"] / totals
    return result.rename(columns={"scheduled_band": "time_band", "size": "scheduled_trips"})


def feed_comparison(valid: pd.DataFrame, screen: pd.DataFrame) -> pd.DataFrame:
    runtime = (
        valid[(valid["is_target"]) & (valid["day_type"] == "Weekday")]
        .groupby(["feed", "direction_id", "direction", "target_label", "actual_band"], as_index=False)
        .agg(runtime_n=("runtime_s", "size"), median_runtime_s=("runtime_s", "median"), p90_runtime_s=("runtime_s", lambda values: values.quantile(0.9)))
        .rename(columns={"actual_band": "time_band"})
    )
    delivery = (
        screen[(screen["day_type"] == "Weekday")]
        .groupby(["feed", "direction_id", "direction", "scheduled_band"], as_index=False)
        .agg(scheduled_trips=("trip_id", "size"), observed_at_screenline=("actual_arrival", lambda values: values.notna().sum()))
        .rename(columns={"scheduled_band": "time_band"})
    )
    delivery["screenline_observation_pct"] = 100 * delivery["observed_at_screenline"] / delivery["scheduled_trips"]
    return runtime.merge(delivery, on=["feed", "direction_id", "direction", "time_band"], how="left")


def day_type_summary(merged: pd.DataFrame, screen: pd.DataFrame, actual: pd.DataFrame) -> pd.DataFrame:
    trip = (
        merged.groupby(["day_type", "service_date", "trip_id"], as_index=False)
        .agg(observed=("actual_arrival", lambda values: values.notna().any()))
        .groupby("day_type", as_index=False)
        .agg(service_days=("service_date", "nunique"), scheduled_trips=("trip_id", "size"), observed_trips=("observed", "sum"))
    )
    trip["trip_observation_pct"] = 100 * trip["observed_trips"] / trip["scheduled_trips"]
    evening_delivery = (
        screen[screen["scheduled_band"] == "Evening"]
        .groupby("day_type", as_index=False)
        .agg(evening_scheduled=("trip_id", "size"), evening_observed=("actual_arrival", lambda values: values.notna().sum()))
    )
    evening_delivery["evening_screenline_observation_pct"] = 100 * evening_delivery["evening_observed"] / evening_delivery["evening_scheduled"]
    evening_gaps = (
        actual[actual["actual_band"] == "Evening"]
        .groupby("day_type", as_index=False)
        .agg(evening_headways=("headway_min", "size"), median_evening_headway_min=("headway_min", "median"), p90_evening_headway_min=("headway_min", lambda values: values.quantile(0.9)), max_evening_headway_min=("headway_min", "max"))
    )
    return trip.merge(evening_delivery, on="day_type").merge(evening_gaps, on="day_type")


def worst_gaps(actual: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "service_date",
        "direction",
        "stop_name",
        "trip_id",
        "actual_local",
        "headway_min",
        "reference_scheduled_headway_min",
        "missed_slots",
        "day_type",
        "actual_band",
    ]
    result = actual.nlargest(25, "headway_min")[columns].copy()
    result["actual_local"] = result["actual_local"].astype(str)
    return result.rename(columns={"actual_band": "time_band"})


def plot_coverage(coverage: pd.DataFrame, path: Path) -> None:
    data = coverage.copy()
    data["service_date"] = pd.to_datetime(data["service_date"])
    fig, ax = plt.subplots(figsize=(11.5, 5.2))
    ax.plot(data["service_date"], data["trip_delivery_pct"], color="#a5213a", marker="o", markersize=2.5, linewidth=1.5, label="Trip observed anywhere")
    ax.plot(data["service_date"], data["stop_observation_pct"], color="#2f6f89", marker="o", markersize=2.5, linewidth=1.5, label="Scheduled stops with arrivals")
    ax.axvline(pd.Timestamp("2026-06-14"), color="#967335", linestyle="--", linewidth=1)
    ax.text(pd.Timestamp("2026-06-15"), 67, "June 14 excluded\nfeed-coverage gap", color="#806b43", fontsize=9)
    ax.set_title("Sixty complete service days support the 51A study", loc="left", pad=16)
    ax.set_ylabel("Share of scheduled service (%)")
    ax.set_ylim(60, 102)
    ax.grid(axis="y")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, ncol=2, loc="lower right")
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def ordered_segments(valid: pd.DataFrame, direction_id: int) -> list[str]:
    order = (
        valid[valid["direction_id"] == direction_id]
        .groupby("segment_id")["stop_sequence"]
        .median()
        .sort_values()
    )
    return list(order.index)


def plot_corridor_map(valid: pd.DataFrame, path: Path) -> None:
    data = valid[valid["direction_id"] == 1].sort_values("stop_sequence").drop_duplicates("stop_id")
    last = valid[valid["direction_id"] == 1].sort_values("stop_sequence").iloc[-1]
    stops = list(zip(data["stop_id"], data["stop_name"], data["stop_lat"])) + [(last["next_stop_id"], last["next_stop_name"], last["next_stop_lat"])]
    stops = sorted({str(stop_id): (str(name), float(lat)) for stop_id, name, lat in stops}.items(), key=lambda item: item[1][1])
    fig, ax = plt.subplots(figsize=(8.2, 9.8))
    y = np.arange(len(stops))
    ax.plot(np.zeros_like(y), y, color="#2f6f89", linewidth=6, solid_capstyle="round")
    for index, (stop_id, (name, _)) in enumerate(stops):
        target_stop = stop_id in {"6036", "6200", "2489", "2507"}
        ax.scatter(0, index, s=95 if target_stop else 38, color="#a5213a" if target_stop else "white", edgecolor="#2f6f89", linewidth=1.5, zorder=3)
        ax.text(0.12 if index % 2 == 0 else -0.12, index, short_stop_name(name), ha="left" if index % 2 == 0 else "right", va="center", fontsize=9, fontweight="bold" if target_stop else "normal")
    index_by_id = {stop_id: index for index, (stop_id, _) in enumerate(stops)}
    for start, end, label in [("6036", "6200", "crosses 14th"), ("2489", "2507", "20th → Grand")]:
        low, high = sorted([index_by_id[start], index_by_id[end]])
        ax.axhspan(low, high, xmin=0.47, xmax=0.53, color="#d59638", alpha=0.45, zorder=1)
        ax.text(0.62, (low + high) / 2, label, color="#806026", va="center", fontsize=9)
    ax.set_xlim(-1.25, 1.25)
    ax.set_ylim(-1, len(stops))
    ax.axis("off")
    ax.set_title("Oakland Broadway corridor studied", loc="left", pad=18)
    ax.text(-1.24, len(stops) - 0.2, "Rockridge-bound stop pattern · 8th Street to College Avenue", color="#596772", fontsize=10)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_segment_heatmap(valid: pd.DataFrame, path: Path) -> None:
    weekday = valid[(valid["day_type"] == "Weekday") & valid["actual_hour"].between(5, 24, inclusive="left")].copy()
    weekday["hour"] = np.floor(weekday["actual_hour"]).astype(int)
    fig, axes = plt.subplots(2, 1, figsize=(13, 12), constrained_layout=True)
    norm = mcolors.Normalize(vmin=35, vmax=180)
    for ax, direction_id in zip(axes, [1, 0]):
        order = ordered_segments(weekday, direction_id)
        labels = weekday.drop_duplicates("segment_id").set_index("segment_id")["segment_label"]
        pivot = weekday[weekday["direction_id"] == direction_id].pivot_table(index="segment_id", columns="hour", values="runtime_s", aggfunc="median").reindex(index=order, columns=range(5, 24))
        image = ax.imshow(pivot, aspect="auto", cmap="YlOrRd", norm=norm)
        ax.set_xticks(range(19), [f"{hour}:00" for hour in range(5, 24)], rotation=45, ha="right", fontsize=8)
        ax.set_yticks(range(len(order)), [labels[item] for item in order], fontsize=8)
        ax.set_title(DIRECTION_LABELS[direction_id], loc="left", fontsize=13)
        ax.set_xlabel("Segment-entry hour, Pacific Time")
        for row, segment_id in enumerate(order):
            if segment_id in {"6036-6200", "2489-2507", "5224-2476", "2511-2487"}:
                ax.get_yticklabels()[row].set_color("#a5213a")
                ax.get_yticklabels()[row].set_fontweight("bold")
    cbar = fig.colorbar(image, ax=axes, shrink=0.82, pad=0.02)
    cbar.set_label("Median stop-to-stop elapsed time (seconds)")
    fig.suptitle("Delay concentrates on specific Broadway blocks", x=0.04, ha="left", fontsize=18, fontweight="bold")
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_hotspots(summary: pd.DataFrame, path: Path) -> None:
    data = summary[(summary["day_type"] == "Weekday") & (summary["time_band"] == "PM peak") & (summary["n"] >= 30)].copy()
    data = data.sort_values("slowdown_pct")
    colors = np.where(data["is_target"], "#a5213a", "#78909b")
    fig, ax = plt.subplots(figsize=(10.5, 8.4))
    y = np.arange(len(data))
    ax.barh(y, data["slowdown_pct"], color=colors)
    ax.set_yticks(y, [f"{row.segment_label} · {DIRECTION_SHORT[int(row.direction_id)]}" for row in data.itertuples()], fontsize=8)
    ax.axvline(0, color="#596772", linewidth=1)
    ax.set_xlabel("Median slowdown versus low-traffic reference (%)")
    ax.set_title("PM delay is localized—not uniform along Broadway", loc="left", pad=18)
    ax.grid(axis="x")
    ax.spines[["top", "right", "left"]].set_visible(False)
    for index, value in enumerate(data["slowdown_pct"]):
        ax.text(value + (2 if value >= 0 else -2), index, f"{value:.0f}%", va="center", ha="left" if value >= 0 else "right", fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_target_distributions(valid: pd.DataFrame, path: Path) -> None:
    data = valid[(valid["is_target"]) & (valid["day_type"] == "Weekday") & valid["actual_band"].isin(["AM peak", "Midday", "PM peak", "Evening"])].copy()
    targets = list(dict.fromkeys(data.sort_values(["direction_id", "stop_sequence"])["segment_label"]))
    fig, axes = plt.subplots(2, 2, figsize=(12, 9), sharey=True)
    for ax, label in zip(axes.flat, targets):
        frame = data[data["segment_label"] == label]
        values = [frame[frame["actual_band"] == band]["runtime_s"] / 60 for band in ["AM peak", "Midday", "PM peak", "Evening"]]
        plot = ax.boxplot(values, tick_labels=["AM", "Midday", "PM", "Evening"], showfliers=False, patch_artist=True, widths=0.65)
        for patch, band in zip(plot["boxes"], ["AM peak", "Midday", "PM peak", "Evening"]):
            patch.set_facecolor(TIME_COLORS[band])
            patch.set_alpha(0.82)
        ax.set_title(f"{label}\n{frame['direction'].iloc[0]}", fontsize=11)
        ax.grid(axis="y")
        ax.spines[["top", "right"]].set_visible(False)
    axes[0, 0].set_ylabel("Stop-to-stop minutes")
    axes[1, 0].set_ylabel("Stop-to-stop minutes")
    fig.suptitle("The named crossings have distinct daily profiles", x=0.06, ha="left", fontsize=17, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_target_hourly(valid: pd.DataFrame, path: Path) -> None:
    data = valid[(valid["is_target"]) & (valid["day_type"] == "Weekday") & valid["actual_hour"].between(5, 24, inclusive="left")].copy()
    data["hour"] = np.floor(data["actual_hour"]).astype(int)
    grouped = data.groupby(["direction_id", "segment_label", "hour"])["runtime_s"].agg(median="median", p90=lambda values: values.quantile(0.9)).reset_index()
    fig, axes = plt.subplots(2, 1, figsize=(11.5, 8.2), sharex=True)
    for ax, direction_id in zip(axes, [1, 0]):
        for index, (label, frame) in enumerate(grouped[grouped["direction_id"] == direction_id].groupby("segment_label")):
            color = ["#a5213a", "#2f6f89"][index % 2]
            ax.plot(frame["hour"], frame["median"] / 60, marker="o", color=color, linewidth=2, label=f"{label} median")
            ax.plot(frame["hour"], frame["p90"] / 60, color=color, linestyle="--", alpha=0.6, label=f"{label} p90")
        ax.axvspan(19, 23.8, color="#ded8e9", alpha=0.55)
        ax.set_title(DIRECTION_LABELS[direction_id], loc="left", fontsize=12)
        ax.set_ylabel("Minutes")
        ax.grid(axis="y")
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(frameon=False, ncol=2, fontsize=8)
    axes[-1].set_xticks(range(5, 24), [f"{hour}:00" for hour in range(5, 24)], rotation=45, ha="right", fontsize=8)
    fig.suptitle("Street delay recedes at night—but not every reliability problem does", x=0.06, ha="left", fontsize=17, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_weekend(valid: pd.DataFrame, path: Path) -> None:
    data = valid[valid["actual_band"].isin(["AM peak", "Midday", "PM peak", "Evening"])].copy()
    grouped = data.groupby(["direction_id", "day_type", "actual_band"], as_index=False).agg(median_s=("runtime_s", "median"), n=("runtime_s", "size"))
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.4), sharey=True)
    x = np.arange(4)
    width = 0.24
    for ax, direction_id in zip(axes, [1, 0]):
        direction = grouped[grouped["direction_id"] == direction_id]
        for index, day in enumerate(DAY_TYPES):
            lookup = direction[direction["day_type"] == day].set_index("actual_band")["median_s"]
            values = [lookup.get(band, np.nan) for band in ["AM peak", "Midday", "PM peak", "Evening"]]
            ax.bar(x + (index - 1) * width, np.array(values) / 60, width, label=day, color=["#2f6f89", "#d59638", "#76578d"][index])
        ax.set_xticks(x, ["AM", "Midday", "PM", "Evening"])
        ax.set_title(DIRECTION_LABELS[direction_id], fontsize=12)
        ax.grid(axis="y")
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].set_ylabel("Median Broadway segment time (minutes)")
    axes[1].legend(frameon=False, fontsize=8)
    fig.suptitle("Weekend conditions change the shape of corridor delay", x=0.06, ha="left", fontsize=17, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_delivery_by_hour(screen: pd.DataFrame, path: Path) -> None:
    weekday = screen[screen["day_type"] == "Weekday"].copy()
    grouped = weekday.groupby(["direction_id", "scheduled_hour_int", "status"]).size().unstack(fill_value=0)
    for status in ["Observed at screenline", "Observed elsewhere only", "No route observation"]:
        if status not in grouped.columns:
            grouped[status] = 0
    grouped["total"] = grouped[["Observed at screenline", "Observed elsewhere only", "No route observation"]].sum(axis=1)
    for status in ["Observed at screenline", "Observed elsewhere only", "No route observation"]:
        grouped[f"{status}_pct"] = 100 * grouped[status] / grouped["total"]
    fig, axes = plt.subplots(2, 1, figsize=(11.5, 8.2), sharex=True)
    colors = ["#2f6f89", "#d59638", "#a5213a"]
    for ax, direction_id in zip(axes, [1, 0]):
        frame = grouped.loc[direction_id].reindex(range(5, 24)).fillna(0)
        bottom = np.zeros(len(frame))
        for status, color in zip(["Observed at screenline", "Observed elsewhere only", "No route observation"], colors):
            values = frame[f"{status}_pct"].to_numpy()
            ax.bar(frame.index, values, bottom=bottom, color=color, label=status)
            bottom += values
        ax.set_title(DIRECTION_LABELS[direction_id], loc="left", fontsize=12)
        ax.set_ylim(75, 100)
        ax.set_ylabel("Scheduled trips (%)")
        ax.grid(axis="y")
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].legend(frameon=False, ncol=3, fontsize=8, loc="lower left")
    axes[-1].set_xticks(range(5, 24), [f"{hour}:00" for hour in range(5, 24)], rotation=45, ha="right", fontsize=8)
    fig.suptitle("Evening gaps include scheduled service not observed on central Broadway", x=0.06, ha="left", fontsize=17, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_headway_box(actual: pd.DataFrame, path: Path) -> None:
    data = actual[(actual["day_type"] == "Weekday") & actual["actual_band"].isin(["AM peak", "Midday", "PM peak", "Evening"])].copy()
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.6), sharey=True)
    for ax, direction_id in zip(axes, [1, 0]):
        frame = data[data["direction_id"] == direction_id]
        values = [frame[frame["actual_band"] == band]["headway_min"] for band in ["AM peak", "Midday", "PM peak", "Evening"]]
        plot = ax.boxplot(values, tick_labels=["AM", "Midday", "PM", "Evening"], whis=1.5, showfliers=True, flierprops={"markersize": 2, "alpha": 0.3}, patch_artist=True)
        for patch, band in zip(plot["boxes"], ["AM peak", "Midday", "PM peak", "Evening"]):
            patch.set_facecolor(TIME_COLORS[band])
            patch.set_alpha(0.82)
        ax.set_title(DIRECTION_LABELS[direction_id], fontsize=12)
        ax.grid(axis="y")
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].set_ylabel("Actual arrival interval (minutes)")
    axes[0].set_ylim(0, 62)
    fig.suptitle("Evening headways have a long, rider-visible tail", x=0.06, ha="left", fontsize=17, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_headway_survival(actual: pd.DataFrame, path: Path) -> None:
    data = actual[(actual["day_type"] == "Weekday") & actual["actual_band"].isin(["Midday", "PM peak", "Evening"])].copy()
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.4), sharey=True)
    for ax, direction_id in zip(axes, [1, 0]):
        for band in ["Midday", "PM peak", "Evening"]:
            values = np.sort(data[(data["direction_id"] == direction_id) & (data["actual_band"] == band)]["headway_min"].to_numpy())
            if not len(values):
                continue
            survival = 100 * (1 - np.arange(1, len(values) + 1) / len(values))
            ax.step(values, survival, where="post", color=TIME_COLORS[band], linewidth=2, label=band)
        ax.set_xlim(0, 60)
        ax.set_ylim(0, 100)
        ax.set_title(DIRECTION_LABELS[direction_id], fontsize=12)
        ax.set_xlabel("Arrival interval exceeded (minutes)")
        ax.grid()
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].set_ylabel("Intervals longer than threshold (%)")
    axes[1].legend(frameon=False)
    fig.suptitle("The probability of a long gap rises after 7 p.m.", x=0.06, ha="left", fontsize=17, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_gap_calendar(actual: pd.DataFrame, path: Path) -> None:
    evening = actual[actual["actual_band"] == "Evening"].copy()
    daily = evening.groupby(["service_date", "direction_id"])["headway_min"].max().unstack()
    full_dates = pd.date_range(START_DATE, END_DATE, freq="D").difference(pd.DatetimeIndex(["2026-06-14"]))
    fig, axes = plt.subplots(2, 1, figsize=(12, 3.8), constrained_layout=True)
    for ax, direction_id in zip(axes, [1, 0]):
        values = daily.get(direction_id, pd.Series(dtype=float)).reindex([item.date().isoformat() for item in full_dates]).to_numpy()[None, :]
        image = ax.imshow(values, aspect="auto", cmap="YlOrRd", vmin=10, vmax=60)
        ax.set_yticks([0], [DIRECTION_LABELS[direction_id]])
        ticks = [index for index, item in enumerate(full_dates) if item.day in {1, 8, 15, 22, 29}]
        ax.set_xticks(ticks, [full_dates[index].strftime("%b %-d") for index in ticks], fontsize=8)
    cbar = fig.colorbar(image, ax=axes, shrink=0.85, pad=0.02)
    cbar.set_label("Longest evening gap (minutes)")
    fig.suptitle("Long evening gaps recur across the two-month window", x=0.03, ha="left", fontsize=17, fontweight="bold")
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_traffic_operations(valid: pd.DataFrame, screen: pd.DataFrame, actual: pd.DataFrame, path: Path) -> None:
    weekday = valid[(valid["day_type"] == "Weekday") & valid["actual_hour"].between(5, 24, inclusive="left")].copy()
    weekday["hour"] = np.floor(weekday["actual_hour"]).astype(int)
    baseline = weekday.groupby(["direction_id", "segment_id"])["runtime_s"].quantile(0.15).rename("baseline_s")
    weekday = weekday.join(baseline, on=["direction_id", "segment_id"])
    weekday["slowdown_pct"] = 100 * (weekday["runtime_s"] / weekday["baseline_s"] - 1)
    traffic = weekday.groupby(["direction_id", "hour"], as_index=False)["slowdown_pct"].median()
    gaps = actual[actual["day_type"] == "Weekday"].copy()
    gaps["hour"] = np.floor(gaps["actual_hour"]).astype(int)
    reliability = gaps.groupby(["direction_id", "hour"], as_index=False).agg(headway_cv=("headway_min", lambda values: values.std() / values.mean()))
    scheduled = screen[screen["day_type"] == "Weekday"].groupby(["direction_id", "scheduled_hour_int"])["actual_arrival"].agg(total="size", observed=lambda values: values.notna().sum()).reset_index().rename(columns={"scheduled_hour_int": "hour"})
    scheduled["missing_pct"] = 100 * (1 - scheduled["observed"] / scheduled["total"])
    data = traffic.merge(reliability, on=["direction_id", "hour"]).merge(scheduled, on=["direction_id", "hour"])
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.5), sharex=True, sharey=True)
    for ax, direction_id in zip(axes, [1, 0]):
        frame = data[data["direction_id"] == direction_id]
        colors = ["#76578d" if hour >= 19 else "#2f6f89" for hour in frame["hour"]]
        ax.scatter(frame["slowdown_pct"], frame["headway_cv"], s=30 + frame["missing_pct"] * 10, c=colors, alpha=0.8, edgecolor="white", linewidth=0.6)
        for row in frame.itertuples():
            ax.text(row.slowdown_pct, row.headway_cv, str(row.hour), fontsize=7, ha="center", va="center", color="white")
        ax.set_title(DIRECTION_LABELS[direction_id], fontsize=12)
        ax.set_xlabel("Median segment slowdown (%)")
        ax.grid()
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].set_ylabel("Headway coefficient of variation")
    fig.suptitle("Traffic delay and service-spacing instability are different dimensions", x=0.06, ha="left", fontsize=17, fontweight="bold")
    fig.text(0.06, 0.01, "Labels are hours; purple is evening. Marker size increases with scheduled trips missing at the screenline.", color="#596772")
    fig.tight_layout(rect=(0, 0.04, 1, 0.94))
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def summarize_findings(
    audit: dict,
    corridor: pd.DataFrame,
    valid: pd.DataFrame,
    segments: pd.DataFrame,
    screen: pd.DataFrame,
    actual: pd.DataFrame,
    headways: pd.DataFrame,
) -> dict:
    weekday_targets = segments[(segments["day_type"] == "Weekday") & segments["is_target"]].copy()
    target_rows = []
    for target in sorted(weekday_targets["target_label"].unique()):
        frame = weekday_targets[weekday_targets["target_label"] == target]
        record = {"target": target, "direction": frame["direction"].iloc[0], "bands": {}}
        for row in frame.itertuples():
            record["bands"][row.time_band] = {
                "n": int(row.n),
                "median_min": row.median_s / 60,
                "p25_min": row.p25_s / 60,
                "p75_min": row.p75_s / 60,
                "p90_min": row.p90_s / 60,
                "p95_min": row.p95_s / 60,
                "slowdown_pct": row.slowdown_pct,
                "scheduled_min": row.scheduled_runtime_s / 60,
            }
        target_rows.append(record)
    evening = headways[(headways["day_type"] == "Weekday") & (headways["time_band"] == "Evening")].copy()
    day = headways[(headways["day_type"] == "Weekday") & (headways["time_band"].isin(["AM peak", "Midday", "PM peak"]))].copy()
    delivery = screen[(screen["day_type"] == "Weekday") & (screen["scheduled_band"] == "Evening")]
    delivery_status = delivery["status"].value_counts().to_dict()
    return {
        "audit": audit,
        "corridor_scheduled_legs": int(len(corridor)),
        "corridor_valid_legs": int(len(valid)),
        "corridor_invalid_or_missing_legs": int(len(corridor) - len(valid)),
        "targets": target_rows,
        "weekday_evening_headways": evening.to_dict(orient="records"),
        "weekday_daytime_headways": day.to_dict(orient="records"),
        "weekday_evening_delivery_status": {key: int(value) for key, value in delivery_status.items()},
        "weekday_evening_scheduled_trips": int(len(delivery)),
        "weekday_evening_screen_observation_pct": 100 * delivery["actual_arrival"].notna().mean(),
        "max_headway_min": float(actual["headway_min"].max()),
        "max_evening_headway_min": float(actual.loc[actual["actual_band"] == "Evening", "headway_min"].max()),
        "screen_stops": {
            DIRECTION_LABELS[direction]: screen.loc[screen["direction_id"] == direction, "stop_name"].iloc[0]
            for direction in SCREEN_STOPS
        },
    }


def main() -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    PLOTS.mkdir(parents=True, exist_ok=True)
    set_style()
    schedule, stops = load_schedule()
    obs = load_observations()
    merged, audit = prepare_merged(schedule, obs)
    corridor, valid = prepare_segments(merged)
    segments = segment_summary(valid)
    coverage = coverage_summary(merged)
    screen, actual, scheduled_reference = prepare_screenline(merged)
    headways = headway_summary(actual)
    delivery = delivery_summary(screen)
    feeds = feed_comparison(valid, screen)
    day_types = day_type_summary(merged, screen, actual)
    gaps = worst_gaps(actual)

    stops.to_csv(RESULTS / "stops.csv", index=False)
    coverage.to_csv(RESULTS / "coverage_daily.csv", index=False)
    segments.to_csv(RESULTS / "segment_summary.csv", index=False)
    headways.to_csv(RESULTS / "headway_summary.csv", index=False)
    delivery.to_csv(RESULTS / "delivery_summary.csv", index=False)
    feeds.to_csv(RESULTS / "feed_comparison.csv", index=False)
    day_types.to_csv(RESULTS / "day_type_summary.csv", index=False)
    gaps.to_csv(RESULTS / "worst_gaps.csv", index=False)
    scheduled_reference.to_csv(RESULTS / "scheduled_headway_reference.csv", index=False)

    plot_coverage(coverage, PLOTS / "coverage.png")
    plot_corridor_map(valid, PLOTS / "corridor-map.png")
    plot_segment_heatmap(valid, PLOTS / "segment-hour-heatmap.png")
    plot_hotspots(segments, PLOTS / "pm-hotspots.png")
    plot_target_distributions(valid, PLOTS / "target-distributions.png")
    plot_target_hourly(valid, PLOTS / "target-hourly.png")
    plot_weekend(valid, PLOTS / "weekend.png")
    plot_delivery_by_hour(screen, PLOTS / "delivery-by-hour.png")
    plot_headway_box(actual, PLOTS / "headway-box.png")
    plot_headway_survival(actual, PLOTS / "headway-survival.png")
    plot_gap_calendar(actual, PLOTS / "gap-calendar.png")
    plot_traffic_operations(valid, screen, actual, PLOTS / "traffic-operations.png")

    summary = summarize_findings(audit, corridor, valid, segments, screen, actual, headways)
    (RESULTS / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
