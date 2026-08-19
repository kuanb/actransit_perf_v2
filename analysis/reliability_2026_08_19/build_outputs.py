from __future__ import annotations

import csv
import hashlib
import io
import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zipfile import ZipFile
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
RESULTS = Path(__file__).resolve().parent / "results"
PLOTS = ROOT / "site" / "presentations" / "2026-08-19" / "plots"
STATS_DIR = Path(os.environ.get("ACTRANSIT_STATS_DIR", "/tmp/actransit-reliability-20260819/stats"))
GTFS_ZIP = Path(os.environ.get("ACTRANSIT_GTFS_ZIP", "/tmp/actransit-reliability-20260819/current.zip"))
GTFS_VERSIONS_DIR = Path(os.environ.get("ACTRANSIT_GTFS_VERSIONS_DIR", "/tmp/actransit-reliability-20260819/gtfs_versions"))

GROUPS = ["Local / other", "Transbay", "All Nighter", "School", "Early Bird"]
PERIODS = ["Baseline", "Summer", "Post-shift"]
COLORS = {
    "Local / other": "#2f6f89",
    "Transbay": "#a5213a",
    "All Nighter": "#76578d",
    "School": "#c48b2b",
    "Early Bird": "#31755b",
}
PERIOD_COLORS = {"Baseline": "#aeb8bd", "Summer": "#2f6f89", "Post-shift": "#a5213a"}
EXCLUDED_DATES = {"2026-06-14", "2026-08-09"}


def route_group(route_id: str) -> str:
    if route_id in {"701", "702", "703"}:
        return "Early Bird"
    if route_id in {"800", "801", "802", "805", "840", "851"}:
        return "All Nighter"
    if route_id.isdigit() and 600 <= int(route_id) <= 699:
        return "School"
    if route_id in {"E", "F", "FS", "G", "J", "L", "NL", "NX", "NX3", "O", "P", "U", "V", "W"}:
        return "Transbay"
    return "Local / other"


def study_period(service_date: str) -> str | None:
    value = date.fromisoformat(service_date)
    if date(2026, 5, 17) <= value <= date(2026, 6, 13):
        return "Baseline"
    if date(2026, 6, 14) <= value <= date(2026, 8, 8):
        return "Summer"
    if date(2026, 8, 9) <= value <= date(2026, 8, 18):
        return "Post-shift"
    return None


def sunday(value: str) -> str:
    parsed = date.fromisoformat(value)
    return str(parsed - timedelta(days=(parsed.weekday() + 1) % 7))


def load_route_names() -> dict[str, str]:
    with ZipFile(GTFS_ZIP) as archive:
        with archive.open("routes.txt") as handle:
            rows = csv.DictReader(line.decode("utf-8-sig") for line in handle)
            return {row["route_id"]: row["route_long_name"] for row in rows}


def load_gtfs_timeline() -> pd.DataFrame:
    rows = []
    previous_hashes: dict[str, str] | None = None
    pacific = ZoneInfo("America/Los_Angeles")
    for path in sorted(GTFS_VERSIONS_DIR.glob("20*.zip")):
        archive_utc = datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        with ZipFile(path) as archive:
            feed_info = next(csv.DictReader(io.TextIOWrapper(archive.open("feed_info.txt"), encoding="utf-8-sig", newline="")))
            file_hashes = {name: hashlib.sha256(archive.read(name)).hexdigest() for name in archive.namelist()}
        if previous_hashes is None:
            change_scope = "Initial archived snapshot"
        else:
            changed = sorted(name for name in set(previous_hashes) | set(file_hashes) if previous_hashes.get(name) != file_hashes.get(name))
            if not changed:
                change_scope = "Identical snapshot"
            elif set(changed) <= {"fare_attributes.txt", "fare_rules.txt"}:
                change_scope = "Fare tables only"
            else:
                change_scope = "Schedule and route files"
        rows.append(
            {
                "archive_name": path.name,
                "archive_utc": archive_utc.isoformat(),
                "archive_pt": archive_utc.astimezone(pacific).isoformat(),
                "feed_version": feed_info["feed_version"],
                "feed_start_date": datetime.strptime(feed_info["feed_start_date"], "%Y%m%d").date().isoformat(),
                "feed_end_date": datetime.strptime(feed_info["feed_end_date"], "%Y%m%d").date().isoformat(),
                "change_scope": change_scope,
                "content_sha256": hashlib.sha256("".join(f"{name}:{file_hashes[name]}" for name in sorted(file_hashes)).encode()).hexdigest(),
            }
        )
        previous_hashes = file_hashes
    return pd.DataFrame(rows)


def load_schedule_data() -> pd.DataFrame:
    rows = []
    for path in sorted(STATS_DIR.glob("2026-*.json")):
        payload = json.loads(path.read_text())
        service_date = payload["service_date"]
        period = study_period(service_date)
        if period is None or service_date in EXCLUDED_DATES:
            continue
        for route in payload["routes"]:
            route_id = str(route["route_id"])
            gap_value = route.get("two_bus_gap_windows")
            rows.append(
                {
                    "service_date": service_date,
                    "week_start": sunday(service_date),
                    "period": period,
                    "route_id": route_id,
                    "route_group": route_group(route_id),
                    "scheduled_trips": route.get("scheduled_trips", 0),
                    "ran_trips": route.get("ran_trips", 0),
                    "two_bus_gap_windows": gap_value,
                    "possible_gap_windows": max(route.get("scheduled_trips", 0) - 1, 0) if gap_value is not None else np.nan,
                }
            )
    return pd.DataFrame(rows)


def aggregate_schedule(schedule: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    grouped = schedule.groupby(keys, as_index=False).agg(
        service_days=("service_date", "nunique"),
        scheduled_trips=("scheduled_trips", "sum"),
        ran_trips=("ran_trips", "sum"),
        two_bus_gap_windows=("two_bus_gap_windows", lambda values: values.sum(min_count=1)),
        possible_gap_windows=("possible_gap_windows", lambda values: values.sum(min_count=1)),
    )
    grouped["trip_delivery_pct"] = 100 * grouped["ran_trips"] / grouped["scheduled_trips"]
    grouped["two_bus_gap_rate_pct"] = 100 * grouped["two_bus_gap_windows"] / grouped["possible_gap_windows"]
    return grouped


def weighted_quantile(values: np.ndarray, weights: np.ndarray, probability: float) -> float:
    order = np.argsort(values)
    values = values[order]
    weights = weights[order]
    index = np.searchsorted(np.cumsum(weights), probability * weights.sum())
    return float(values[min(index, len(values) - 1)])


def rider_wait_quantile(headways: np.ndarray, counts: np.ndarray, probability: float) -> float:
    denominator = np.sum(counts * headways)
    low, high = 0, 7200
    while low < high:
        candidate = (low + high) // 2
        cdf = np.sum(counts * np.minimum(candidate, headways)) / denominator
        if cdf >= probability:
            high = candidate
        else:
            low = candidate + 1
    return low / 60


def summarize_headways(frame: pd.DataFrame) -> pd.Series:
    headways = frame["headway_bin_seconds"].to_numpy(dtype=float)
    counts = frame["n"].to_numpy(dtype=float)
    denominator = np.sum(counts * headways)
    return pd.Series(
        {
            "headways": int(counts.sum()),
            "p50_headway_min": weighted_quantile(headways, counts, 0.5) / 60,
            "p90_headway_min": weighted_quantile(headways, counts, 0.9) / 60,
            "mean_rider_wait_min": np.sum(counts * headways * headways) / (2 * denominator) / 60,
            "p50_rider_wait_min": rider_wait_quantile(headways, counts, 0.5),
            "p90_rider_wait_min": rider_wait_quantile(headways, counts, 0.9),
        }
    )


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


def grouped_bars(
    frame: pd.DataFrame,
    value: str,
    title: str,
    ylabel: str,
    path: Path,
    na_school_summer: bool = False,
    ymax: float = 105,
    label_offset: float = 0.8,
) -> None:
    fig, ax = plt.subplots(figsize=(10.5, 5.6))
    x = np.arange(len(GROUPS))
    width = 0.23
    for index, period in enumerate(PERIODS):
        lookup = frame[frame["period"] == period].set_index("route_group")[value]
        values = np.array([lookup.get(group, np.nan) for group in GROUPS], dtype=float)
        if na_school_summer and period == "Summer":
            values[GROUPS.index("School")] = np.nan
        bars = ax.bar(x + (index - 1) * width, values, width, label=period, color=PERIOD_COLORS[period])
        for bar, amount in zip(bars, values):
            if np.isfinite(amount):
                ax.text(bar.get_x() + bar.get_width() / 2, amount + label_offset, f"{amount:.1f}", ha="center", va="bottom", fontsize=8)
    if na_school_summer:
        ax.text(x[GROUPS.index("School")], ymax * 0.08, "summer\nsuspension", ha="center", color="#806b43", fontsize=8)
    ax.set_title(title, loc="left", pad=18)
    ax.set_ylabel(ylabel)
    ax.set_xticks(x, [group.replace(" / other", "") for group in GROUPS])
    ax.set_ylim(0, ymax)
    ax.grid(axis="y", alpha=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, ncol=3, loc="upper right")
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_punctuality(period_route: pd.DataFrame, path: Path) -> None:
    group = period_route[period_route["grain"] == "group"].copy()
    group["on_time_pct"] = 100 * group["on_time_arrivals"] / group["arrivals"]
    fig, axes = plt.subplots(1, 3, figsize=(11.5, 5.2), sharey=True)
    y = np.arange(len(GROUPS))
    for ax, period in zip(axes, PERIODS):
        data = group[group["period"] == period].set_index("route_group")
        values = [data["on_time_pct"].get(item, np.nan) for item in GROUPS]
        if period == "Summer":
            values[GROUPS.index("School")] = np.nan
        ax.barh(y, values, color=[COLORS[item] for item in GROUPS])
        for row, value in enumerate(values):
            if np.isfinite(value):
                ax.text(value + 0.8, row, f"{value:.1f}%", va="center", fontsize=8)
        ax.set_title(period, fontsize=12)
        ax.set_xlim(0, 58)
        ax.grid(axis="x")
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.tick_params(axis="y", length=0)
    axes[0].set_yticks(y, [group.replace(" / other", "") for group in GROUPS])
    axes[0].invert_yaxis()
    fig.suptitle("Strict on-time arrivals remain a minority", x=0.06, ha="left", fontsize=16, fontweight="bold")
    fig.text(0.06, 0.02, "Share of actual stop arrivals from scheduled time through 3 minutes late", color="#596772")
    fig.tight_layout(rect=(0, 0.05, 1, 0.93))
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_completion(completion: pd.DataFrame, path: Path) -> None:
    data = completion[completion["grain"] == "period"].copy()
    data["severe_pct"] = 100 * data["severely_truncated_trips"] / data["ran_trips"]
    grouped_bars(
        data,
        "severe_pct",
        "Severe truncation is uncommon, but concentrated",
        "Trips observed before 80% of route (%)",
        path,
        True,
        ymax=5.5,
        label_offset=0.12,
    )


def plot_waits(headways: pd.DataFrame, path: Path) -> None:
    data = headways[(headways["grain"] == "period_group") & (headways["headways"] >= 500)].copy()
    groups = ["Local / other", "Transbay", "All Nighter", "School"]
    fig, ax = plt.subplots(figsize=(10.5, 5.4))
    x = np.arange(len(groups))
    width = 0.23
    for index, period in enumerate(PERIODS):
        lookup = data[data["period"] == period].set_index("route_group")["mean_rider_wait_min"]
        values = np.array([lookup.get(group, np.nan) for group in groups], dtype=float)
        if period == "Summer":
            values[groups.index("School")] = np.nan
        bars = ax.bar(x + (index - 1) * width, values, width, label=period, color=PERIOD_COLORS[period])
        for bar, amount in zip(bars, values):
            if np.isfinite(amount):
                ax.text(bar.get_x() + bar.get_width() / 2, amount + 0.35, f"{amount:.1f}", ha="center", fontsize=8)
    ax.set_title("Random-arrival riders wait longest on low-frequency services", loc="left", pad=18)
    ax.set_ylabel("Estimated mean wait (minutes)")
    ax.set_xticks(x, [group.replace(" / other", "") for group in groups])
    ax.set_ylim(0, 32)
    ax.grid(axis="y")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, ncol=3)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_weekly(weekly: pd.DataFrame, path: Path) -> None:
    weekly = weekly.copy()
    weekly["week_start"] = pd.to_datetime(weekly["week_start"])
    weekly["stop_delivery_pct"] = 100 * weekly["delivered_stop_rows"] / weekly["scheduled_stop_rows"]
    fig, ax = plt.subplots(figsize=(11, 5.4))
    for group in ["Local / other", "Transbay", "All Nighter"]:
        data = weekly[weekly["route_group"] == group]
        ax.plot(data["week_start"], data["stop_delivery_pct"], marker="o", linewidth=2, markersize=4, color=COLORS[group], label=group.replace(" / other", ""))
    ax.axvspan(pd.Timestamp("2026-06-14"), pd.Timestamp("2026-08-09"), color="#eadfc7", alpha=0.55, zorder=0)
    ax.axvline(pd.Timestamp("2026-06-14"), color="#9d7b3d", linewidth=1)
    ax.axvline(pd.Timestamp("2026-08-09"), color="#9d7b3d", linewidth=1)
    ax.text(pd.Timestamp("2026-06-16"), 84.5, "Summer schedule", color="#7a6334", fontsize=9)
    ax.text(pd.Timestamp("2026-08-10"), 84.5, "Fall post-shift", color="#7a6334", fontsize=9)
    ax.set_title("Stop-level delivery was stable through summer, then softened", loc="left", pad=18)
    ax.set_ylabel("Stops reached from 1 min early to 7 min late (%)")
    ax.set_ylim(55, 86)
    ax.grid(axis="y")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, ncol=3, loc="lower left")
    fig.autofmt_xdate(rotation=0)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_coverage(audit: pd.DataFrame, path: Path) -> None:
    data = audit.copy()
    data["service_date"] = pd.to_datetime(data["service_date"])
    fig, ax = plt.subplots(figsize=(11, 4.4))
    ax.plot(data["service_date"], data["arrivals"] / 1000, color="#2f6f89", linewidth=1.6)
    for excluded in sorted(EXCLUDED_DATES):
        value = pd.Timestamp(excluded)
        ax.axvline(value, color="#a5213a", linewidth=1)
        ax.scatter([value], data.loc[data["service_date"] == value, "arrivals"] / 1000, color="#a5213a", zorder=3)
    ax.set_title("The archive is continuous; two schedule-shift dates are partial", loc="left", pad=16)
    ax.set_ylabel("Deduplicated arrivals (thousands)")
    ax.grid(axis="y")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_school(period_route: pd.DataFrame, schedule_period: pd.DataFrame, completion: pd.DataFrame, path: Path) -> None:
    bq = period_route[(period_route["grain"] == "group") & (period_route["route_group"] == "School")].set_index("period")
    schedule = schedule_period[schedule_period["route_group"] == "School"].set_index("period")
    comp = completion[(completion["grain"] == "period") & (completion["route_group"] == "School")].set_index("period")
    metrics = {
        "Trips delivered": [schedule.loc[p, "trip_delivery_pct"] for p in ["Baseline", "Post-shift"]],
        "Stops delivered": [100 * bq.loc[p, "delivered_stop_rows"] / bq.loc[p, "scheduled_stop_rows"] for p in ["Baseline", "Post-shift"]],
        "Strictly on time": [100 * bq.loc[p, "on_time_arrivals"] / bq.loc[p, "arrivals"] for p in ["Baseline", "Post-shift"]],
        "Not severely truncated": [100 - 100 * comp.loc[p, "severely_truncated_trips"] / comp.loc[p, "ran_trips"] for p in ["Baseline", "Post-shift"]],
    }
    fig, ax = plt.subplots(figsize=(10.5, 5.2))
    y = np.arange(len(metrics))
    for row, (label, values) in enumerate(metrics.items()):
        ax.plot(values, [row, row], color="#c7c4bd", linewidth=3, zorder=1)
        ax.scatter(values[0], row, color=PERIOD_COLORS["Baseline"], s=85, zorder=2)
        ax.scatter(values[1], row, color=PERIOD_COLORS["Post-shift"], s=85, zorder=2)
        ax.text(np.mean(values), row - 0.19, f"{values[0]:.1f} → {values[1]:.1f}", ha="center", fontsize=8)
    ax.set_yticks(y, metrics.keys())
    ax.invert_yaxis()
    ax.set_xlim(20, 104)
    ax.set_xlabel("Percent")
    ax.set_title("School service returned near baseline—with more late arrivals", loc="left", pad=18)
    ax.grid(axis="x")
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.scatter([], [], color=PERIOD_COLORS["Baseline"], label="Baseline")
    ax.scatter([], [], color=PERIOD_COLORS["Post-shift"], label="Post-shift")
    ax.legend(frameon=False, ncol=2, loc="upper right")
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_route_changes(comparison: pd.DataFrame, path: Path) -> None:
    eligible = comparison[(comparison["observed_trips_baseline"] >= 100) & (comparison["observed_trips_post_shift"] >= 50)].copy()
    selected = pd.concat([eligible.nsmallest(6, "post_shift_stop_delta"), eligible.nlargest(6, "post_shift_stop_delta")]).drop_duplicates("route_id")
    selected = selected.sort_values("post_shift_stop_delta")
    fig, ax = plt.subplots(figsize=(10.5, 6.3))
    colors = [COLORS[group] for group in selected["route_group"]]
    ax.barh(selected["route_id"], selected["post_shift_stop_delta"], color=colors)
    ax.axvline(0, color="#596772", linewidth=1)
    for row, value in enumerate(selected["post_shift_stop_delta"]):
        ax.text(value + (0.5 if value >= 0 else -0.5), row, f"{value:+.1f}", va="center", ha="left" if value >= 0 else "right", fontsize=8)
    ax.set_title("The post-shift period did not move every route in the same direction", loc="left", pad=18)
    ax.set_xlabel("Change in stop delivery vs. baseline (percentage points)")
    ax.grid(axis="x")
    ax.spines[["top", "right", "left"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    PLOTS.mkdir(parents=True, exist_ok=True)
    set_style()

    audit = pd.read_csv(RESULTS / "audit_daily.csv")
    weekly = pd.read_csv(RESULTS / "weekly_group.csv")
    period_route = pd.read_csv(RESULTS / "period_route.csv")
    completion = pd.read_csv(RESULTS / "completion.csv")
    headway_bins = pd.read_csv(RESULTS / "headway_bins.csv")
    schedule = load_schedule_data()
    route_names = load_route_names()
    gtfs_timeline = load_gtfs_timeline()

    schedule_daily = aggregate_schedule(schedule, ["service_date", "week_start", "period", "route_group"])
    schedule_weekly = aggregate_schedule(schedule, ["week_start", "period", "route_group"])
    schedule_period = aggregate_schedule(schedule, ["period", "route_group"])
    schedule_route_period = aggregate_schedule(schedule, ["period", "route_group", "route_id"])
    schedule.to_csv(RESULTS / "schedule_route_daily.csv", index=False)
    schedule_daily.to_csv(RESULTS / "schedule_daily_group.csv", index=False)
    schedule_weekly.to_csv(RESULTS / "schedule_weekly_group.csv", index=False)
    schedule_period.to_csv(RESULTS / "schedule_period_group.csv", index=False)
    schedule_route_period.to_csv(RESULTS / "schedule_period_route.csv", index=False)
    gtfs_timeline.to_csv(RESULTS / "gtfs_version_timeline.csv", index=False)

    summary_keys = ["grain", "period", "week_start", "route_group", "route_id"]
    headways = (
        headway_bins.groupby(summary_keys, dropna=False)
        .apply(summarize_headways, include_groups=False)
        .reset_index()
    )
    headways.to_csv(RESULTS / "headway_summary.csv", index=False)

    routes = period_route[period_route["grain"] == "route"].copy()
    routes["stop_delivery_pct"] = 100 * routes["delivered_stop_rows"] / routes["scheduled_stop_rows"]
    routes["on_time_pct"] = 100 * routes["on_time_arrivals"] / routes["arrivals"]
    pivots = []
    for period in PERIODS:
        subset = routes[routes["period"] == period][["route_id", "route_group", "observed_trips", "stop_delivery_pct", "on_time_pct"]].copy()
        period_key = period.lower().replace("-", "_")
        subset = subset.rename(columns={column: f"{column}_{period_key}" for column in subset.columns if column not in {"route_id", "route_group"}})
        pivots.append(subset)
    comparison = pivots[0]
    for subset in pivots[1:]:
        comparison = comparison.merge(subset, on=["route_id", "route_group"], how="outer")
    comparison["route_name"] = comparison["route_id"].map(route_names)
    comparison["summer_stop_delta"] = comparison["stop_delivery_pct_summer"] - comparison["stop_delivery_pct_baseline"]
    comparison["post_shift_stop_delta"] = comparison["stop_delivery_pct_post_shift"] - comparison["stop_delivery_pct_baseline"]
    comparison["summer_ontime_delta"] = comparison["on_time_pct_summer"] - comparison["on_time_pct_baseline"]
    comparison["post_shift_ontime_delta"] = comparison["on_time_pct_post_shift"] - comparison["on_time_pct_baseline"]
    comparison.to_csv(RESULTS / "route_comparison.csv", index=False)

    group_metrics = period_route[period_route["grain"] == "group"].copy()
    group_metrics["stop_delivery_pct"] = 100 * group_metrics["delivered_stop_rows"] / group_metrics["scheduled_stop_rows"]
    group_metrics["on_time_pct"] = 100 * group_metrics["on_time_arrivals"] / group_metrics["arrivals"]
    group_metrics["early_pct"] = 100 * group_metrics["early_arrivals"] / group_metrics["arrivals"]
    group_metrics["very_late_pct"] = 100 * group_metrics["very_late_arrivals"] / group_metrics["arrivals"]
    merged_groups = group_metrics.merge(schedule_period, on=["period", "route_group"], suffixes=("", "_schedule"))
    completion_period = completion[completion["grain"] == "period"].copy()
    completion_period["severe_pct"] = 100 * completion_period["severely_truncated_trips"] / completion_period["ran_trips"]
    merged_groups = merged_groups.merge(completion_period[["period", "route_group", "severe_pct"]], on=["period", "route_group"], how="left")

    summary = {
        "coverage": {
            "archive_days": int(len(audit)),
            "min_date": str(audit["service_date"].min()),
            "max_date": str(audit["service_date"].max()),
            "raw_rows": int(audit["raw_rows"].sum()),
            "deduped_rows": int(audit["deduped_rows"].sum()),
            "duplicate_rows": int(audit["duplicate_rows"].sum()),
            "duplicate_pct": round(100 * audit["duplicate_rows"].sum() / audit["raw_rows"].sum(), 1),
            "excluded_dates": sorted(EXCLUDED_DATES),
        },
        "groups": json.loads(merged_groups.to_json(orient="records")),
        "headways": json.loads(headways[headways["grain"] == "period_group"].to_json(orient="records")),
        "route_changes": json.loads(
            comparison[(comparison["observed_trips_baseline"] >= 100) & (comparison["observed_trips_post_shift"] >= 50)]
            .sort_values("post_shift_stop_delta")
            .to_json(orient="records")
        ),
    }
    (RESULTS / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    plot_coverage(audit, PLOTS / "coverage.png")
    grouped_bars(schedule_period, "trip_delivery_pct", "Most scheduled trips ran; specialized services improved", "Scheduled trips observed (%)", PLOTS / "trip-delivery.png", True)
    grouped_bars(group_metrics, "stop_delivery_pct", "A running trip still does not guarantee a reliable stop arrival", "Stops reached from 1 min early to 7 min late (%)", PLOTS / "stop-delivery.png", True)
    plot_punctuality(period_route, PLOTS / "punctuality.png")
    plot_completion(completion, PLOTS / "completion.png")
    plot_waits(headways, PLOTS / "rider-wait.png")
    plot_weekly(weekly, PLOTS / "weekly-trend.png")
    plot_school(period_route, schedule_period, completion, PLOTS / "school-post-shift.png")
    plot_route_changes(comparison, PLOTS / "route-changes.png")

    print(merged_groups[["period", "route_group", "trip_delivery_pct", "stop_delivery_pct", "on_time_pct", "very_late_pct", "severe_pct"]].round(1).to_string(index=False))
    print(f"\nWrote {len(list(PLOTS.glob('*.png')))} plots and {len(schedule):,} route-day schedule rows.")


if __name__ == "__main__":
    main()
