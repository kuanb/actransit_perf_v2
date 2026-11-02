#!/usr/bin/env python3

import argparse
import json
import re
import shutil
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import shapefile
from matplotlib.lines import Line2D
from pyproj import Transformer
from shapely.geometry import MultiPoint, Point, mapping, shape
from shapely.ops import transform, unary_union
from shapely.strtree import STRtree


ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent.parent
RESULTS = ROOT / "results"
PLOTS = REPO / "site" / "presentations" / "2026-11-02-title-vi" / "plots"
DATA = REPO / "site" / "presentations" / "2026-11-02-title-vi" / "data"
POC_THRESHOLD = 0.74
MILES_TO_METERS = 1609.344
COLORS = {
    "minority": "#b3364a",
    "nonminority": "#167383",
    "gold": "#b57a1c",
    "ink": "#17262e",
    "muted": "#63717a",
    "paper": "#f5f0e7",
    "grid": "#d9d1c5",
    "white": "#fffdf9",
}
FLEET_SERIES = [
    (1641, 1690, 2022), (7031, 7050, 2022), (8008, 8028, 2022),
    (8006, 8007, 2021), (6301, 6335, 2021), (1591, 1591, 2019),
    (8001, 8005, 2019), (7017, 7026, 2018), (1601, 1635, 2018),
    (6201, 6215, 2018), (1581, 1590, 2017), (2301, 2327, 2017),
    (2224, 2252, 2017), (1501, 1555, 2016), (1556, 1580, 2016),
    (1401, 1468, 2014), (3505, 3510, 2014), (6101, 6154, 2014),
    (2201, 2223, 2013), (1301, 1365, 2012), (2191, 2199, 2009),
    (5105, 5139, 2009), (1201, 1227, 2008),
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path("/tmp/actransit-titlevi-20261102"),
        help="Directory containing the downloaded Census, AC Transit GIS, GTFS, and daily stats inputs",
    )
    return parser.parse_args()


def pct(numerator, denominator):
    return 100 * numerator / denominator if denominator else None


def round_or_none(value, digits=2):
    return round(float(value), digits) if value is not None and pd.notna(value) else None


def day_type(value):
    weekday = value.weekday()
    if weekday == 5:
        return "saturday"
    if weekday == 6:
        return "sunday"
    return "weekday"


def configure_axis(axis):
    axis.set_facecolor(COLORS["white"])
    axis.spines[["top", "right"]].set_visible(False)
    axis.spines[["left", "bottom"]].set_color(COLORS["grid"])
    axis.grid(axis="y", color=COLORS["grid"], linewidth=0.8, alpha=0.8)
    axis.tick_params(colors=COLORS["muted"], labelsize=9)


def save_figure(fig, filename):
    fig.patch.set_facecolor(COLORS["paper"])
    fig.savefig(PLOTS / filename, dpi=190, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)


def find_shapefile(directory):
    matches = sorted(directory.glob("*.shp"))
    if len(matches) != 1:
        raise RuntimeError(f"expected one shapefile in {directory}, found {len(matches)}")
    return matches[0]


def load_block_groups(source_root):
    acs = pd.read_csv(
        source_root / "acsdt5y2024-b03002.dat",
        sep="|",
        dtype={"GEO_ID": str},
        usecols=["GEO_ID", "B03002_E001", "B03002_E003"],
    )
    acs = acs[acs["GEO_ID"].str.startswith("1500000US06", na=False)].copy()
    acs["geoid"] = acs["GEO_ID"].str.replace("1500000US", "", regex=False)
    acs["population"] = pd.to_numeric(acs["B03002_E001"], errors="coerce")
    acs["white_non_hispanic"] = pd.to_numeric(acs["B03002_E003"], errors="coerce")
    acs["poc_share"] = 1 - acs["white_non_hispanic"] / acs["population"]
    demographics = acs.set_index("geoid")[["population", "poc_share"]].to_dict("index")

    reader = shapefile.Reader(str(find_shapefile(source_root / "bg")))
    fields = [field[0] for field in reader.fields[1:]]
    project = Transformer.from_crs("EPSG:4269", "EPSG:3310", always_xy=True).transform
    rows = []
    for record, geometry in zip(reader.iterRecords(), reader.iterShapes()):
        values = dict(zip(fields, record))
        if values["COUNTYFP"] not in {"001", "013"}:
            continue
        geoid = values["GEOID"]
        demographic = demographics.get(geoid)
        if not demographic or not demographic["population"]:
            continue
        rows.append({
            "geoid": geoid,
            "county": values["COUNTYFP"],
            "population": float(demographic["population"]),
            "poc_share": float(demographic["poc_share"]),
            "minority": float(demographic["poc_share"]) >= POC_THRESHOLD,
            "geometry": transform(project, shape(geometry.__geo_interface__)),
        })
    return rows


def load_routes(source_root):
    reader = shapefile.Reader(str(find_shapefile(source_root / "routes")))
    fields = [field[0] for field in reader.fields[1:]]
    project = Transformer.from_crs("EPSG:4326", "EPSG:3310", always_xy=True).transform
    geometries = defaultdict(list)
    for record, geometry in zip(reader.iterRecords(), reader.iterShapes()):
        values = dict(zip(fields, record))
        route_id = str(values["PUB_RTE"]).strip()
        geometries[route_id].append(transform(project, shape(geometry.__geo_interface__)))
    return {route_id: unary_union(parts) for route_id, parts in geometries.items()}


def classify_routes(route_geometries, block_groups):
    rows = []
    for route_id, geometry in sorted(route_geometries.items()):
        total_meters = 0.0
        minority_meters = 0.0
        for block_group in block_groups:
            if not geometry.intersects(block_group["geometry"]):
                continue
            length = geometry.intersection(block_group["geometry"]).length
            total_meters += length
            if block_group["minority"]:
                minority_meters += length
        share = minority_meters / total_meters if total_meters else 0
        rows.append({
            "route_id": route_id,
            "route_miles_in_alameda_contra_costa": total_meters / MILES_TO_METERS,
            "minority_block_group_miles": minority_meters / MILES_TO_METERS,
            "minority_mileage_share": share,
            "provisional_minority_route": share >= 1 / 3,
        })
    return pd.DataFrame(rows)


def read_gtfs(source_root):
    gtfs = source_root / "gtfs"
    return {
        name: pd.read_csv(gtfs / f"{name}.txt", dtype=str)
        for name in ["routes", "trips", "stop_times", "calendar", "calendar_dates", "stops"]
    }


def active_services(gtfs, service_date):
    calendar = gtfs["calendar"].copy()
    yyyymmdd = service_date.strftime("%Y%m%d")
    weekday = service_date.strftime("%A").lower()
    active = set(calendar.loc[
        (calendar["start_date"] <= yyyymmdd)
        & (calendar["end_date"] >= yyyymmdd)
        & (calendar[weekday] == "1"),
        "service_id",
    ])
    exceptions = gtfs["calendar_dates"]
    for row in exceptions.loc[exceptions["date"] == yyyymmdd].itertuples():
        if row.exception_type == "1":
            active.add(row.service_id)
        elif row.exception_type == "2":
            active.discard(row.service_id)
    return active


def gtfs_seconds(value):
    hours, minutes, seconds = map(int, value.split(":"))
    return hours * 3600 + minutes * 60 + seconds


def calculate_scheduled_headways(gtfs, classifications):
    route_names = gtfs["routes"][["route_id", "route_short_name"]]
    trips = gtfs["trips"].merge(route_names, on="route_id", how="left")
    stop_times = gtfs["stop_times"].copy()
    stop_times["stop_sequence_number"] = pd.to_numeric(stop_times["stop_sequence"], errors="coerce")
    first_stops = stop_times.loc[stop_times.groupby("trip_id")["stop_sequence_number"].idxmin(), ["trip_id", "departure_time"]]
    trip_starts = trips.merge(first_stops, on="trip_id", how="inner")
    trip_starts["departure_seconds"] = trip_starts["departure_time"].map(gtfs_seconds)
    class_lookup = classifications.set_index("route_id")["provisional_minority_route"].to_dict()
    representatives = {
        "weekday": date(2026, 10, 14),
        "saturday": date(2026, 10, 10),
        "sunday": date(2026, 10, 11),
    }
    detail_rows = []
    for service_day, representative in representatives.items():
        active = active_services(gtfs, representative)
        selected = trip_starts[trip_starts["service_id"].isin(active)].copy()
        selected = selected[~selected["route_short_name"].str.match(r"^[67]\d\d", na=False)]
        for (route_id, direction_id), group in selected.groupby(["route_short_name", "direction_id"], dropna=False):
            departures = sorted(group["departure_seconds"].tolist())
            for earlier, later in zip(departures, departures[1:]):
                headway = (later - earlier) / 60
                if headway < 0:
                    continue
                hour = (later % 86400) / 3600
                period = "peak" if 6 <= hour < 10 or 15 <= hour < 19 else "offpeak"
                detail_rows.append({
                    "route_id": route_id,
                    "day_type": service_day,
                    "period": period,
                    "headway_minutes": headway,
                    "provisional_minority_route": class_lookup.get(route_id),
                })
    return pd.DataFrame(detail_rows)


def calculate_access(gtfs, block_groups):
    project = Transformer.from_crs("EPSG:4326", "EPSG:3310", always_xy=True).transform
    stops = gtfs["stops"].copy()
    stops["point"] = [
        transform(project, Point(float(lon), float(lat)))
        for lon, lat in zip(stops["stop_lon"], stops["stop_lat"])
    ]
    all_stop_points = MultiPoint(stops["point"].tolist())
    stop_times = gtfs["stop_times"]
    trips = gtfs["trips"][["trip_id", "service_id"]]
    representatives = {
        "weekday": date(2026, 10, 14),
        "saturday": date(2026, 10, 10),
        "sunday": date(2026, 10, 11),
    }
    qualifying = {}
    for service_day, representative in representatives.items():
        active = active_services(gtfs, representative)
        active_trips = set(trips.loc[trips["service_id"].isin(active), "trip_id"])
        counts = stop_times.loc[stop_times["trip_id"].isin(active_trips)].groupby("stop_id")["trip_id"].nunique()
        qualifying_ids = set(counts[counts >= 14].index)
        qualifying[service_day] = MultiPoint(stops.loc[stops["stop_id"].isin(qualifying_ids), "point"].tolist())

    rows = []
    for block_group in block_groups:
        centroid = block_group["geometry"].centroid
        distance_to_any = centroid.distance(all_stop_points)
        if distance_to_any > MILES_TO_METERS:
            continue
        row = {
            "geoid": block_group["geoid"],
            "population": block_group["population"],
            "poc_share": block_group["poc_share"],
            "minority": block_group["minority"],
            "distance_to_nearest_stop_miles": distance_to_any / MILES_TO_METERS,
        }
        for service_day, points in qualifying.items():
            distance = centroid.distance(points) if not points.is_empty else float("inf")
            row[f"{service_day}_access"] = distance <= 0.25 * MILES_TO_METERS
        rows.append(row)
    return pd.DataFrame(rows)


def calculate_stop_distribution(gtfs, block_groups):
    project = Transformer.from_crs("EPSG:4326", "EPSG:3310", always_xy=True).transform
    geometries = [block_group["geometry"] for block_group in block_groups]
    tree = STRtree(geometries)
    rows = []
    for stop in gtfs["stops"].itertuples():
        point = transform(project, Point(float(stop.stop_lon), float(stop.stop_lat)))
        matches = tree.query(point, predicate="within")
        if not len(matches):
            continue
        block_group = block_groups[int(matches[0])]
        rows.append({
            "stop_id": stop.stop_id,
            "geoid": block_group["geoid"],
            "group": "minority" if block_group["minority"] else "nonminority",
        })
    detail = pd.DataFrame(rows)
    summary = detail.groupby("group").size().rename("stop_n").reset_index()
    summary["share_of_stops_pct"] = 100 * summary["stop_n"] / summary["stop_n"].sum()
    return detail, summary


def empty_performance_bucket():
    return defaultdict(float)


def add_performance(bucket, route):
    kpi = route.get("agency_kpi", {})
    operated = kpi.get("service_operated", {})
    otp = kpi.get("on_time_performance", {})
    bucket["scheduled_trips"] += operated.get("scheduled_trips", 0)
    bucket["operated_trips"] += operated.get("operated_trips", 0)
    bucket["partial_trips"] += operated.get("partial_trips", 0)
    bucket["on_time_timepoints"] += otp.get("on_time_timepoints", 0)
    bucket["operated_timepoints"] += otp.get("operated_timepoints", 0)
    bucket["scheduled_timepoints"] += otp.get("scheduled_timepoints", 0)
    bucket["stop_sd_delivered_n"] += route.get("stop_sd_delivered_n", 0)
    bucket["stop_sd_n"] += route.get("stop_sd_n", 0)
    bunching = route.get("bunching") or {}
    if bunching.get("methodology_version") == 3:
        for key in ["headway_n", "comparison_n", "bunched_headway_n", "long_gap_n"]:
            bucket[key] += bunching.get(key, 0)
        for key, value in bunching.get("aggregation", {}).items():
            bucket[f"aggregation_{key}"] += value


def summarize_performance(bucket):
    comparable_seconds = bucket["aggregation_comparable_headway_seconds"]
    scheduled_seconds = bucket["aggregation_scheduled_headway_seconds"]
    expected_wait = (
        bucket["aggregation_comparable_headway_squared_seconds"] / (2 * comparable_seconds) / 60
        if comparable_seconds else None
    )
    scheduled_wait = (
        bucket["aggregation_scheduled_headway_squared_seconds"] / (2 * scheduled_seconds) / 60
        if scheduled_seconds else None
    )
    headway_n = bucket["headway_n"]
    mean_headway = bucket["aggregation_observed_headway_seconds"] / headway_n / 60 if headway_n else None
    return {
        "scheduled_trips": int(bucket["scheduled_trips"]),
        "operated_trips": int(bucket["operated_trips"]),
        "service_operated_pct": pct(bucket["operated_trips"], bucket["scheduled_trips"]),
        "partial_of_operated_pct": pct(bucket["partial_trips"], bucket["operated_trips"]),
        "otp_operated_pct": pct(bucket["on_time_timepoints"], bucket["operated_timepoints"]),
        "otp_scheduled_pct": pct(bucket["on_time_timepoints"], bucket["scheduled_timepoints"]),
        "on_time_service_delivered_pct": pct(bucket["stop_sd_delivered_n"], bucket["stop_sd_n"]),
        "headway_comparison_n": int(bucket["comparison_n"]),
        "bunched_headway_pct": pct(bucket["bunched_headway_n"], bucket["comparison_n"]),
        "long_gap_pct": pct(bucket["long_gap_n"], bucket["comparison_n"]),
        "mean_actual_headway_min": mean_headway,
        "expected_wait_min": expected_wait,
        "scheduled_expected_wait_min": scheduled_wait,
        "spacing_penalty_min": expected_wait - scheduled_wait if expected_wait is not None and scheduled_wait is not None else None,
    }


def aggregate_performance(source_root, classifications):
    class_lookup = classifications.set_index("route_id")["provisional_minority_route"].to_dict()
    route_buckets = defaultdict(empty_performance_bucket)
    group_buckets = defaultdict(empty_performance_bucket)
    stats_dir = source_root / "stats"
    for path in sorted(stats_dir.glob("2026-10-*.json")):
        report = json.loads(path.read_text())
        service_day = day_type(datetime.strptime(report["service_date"], "%Y-%m-%d").date())
        for route in report["routes"]:
            route_id = str(route["route_id"])
            if route_id not in class_lookup:
                continue
            group = "minority" if class_lookup[route_id] else "nonminority"
            add_performance(route_buckets[(route_id, service_day)], route)
            add_performance(route_buckets[(route_id, "all")], route)
            add_performance(group_buckets[(group, service_day)], route)
            add_performance(group_buckets[(group, "all")], route)

    route_rows = []
    for (route_id, service_day), bucket in sorted(route_buckets.items()):
        row = {"route_id": route_id, "day_type": service_day}
        row.update(summarize_performance(bucket))
        route_rows.append(row)
    group_rows = []
    for (group, service_day), bucket in sorted(group_buckets.items()):
        row = {"group": group, "day_type": service_day}
        row.update(summarize_performance(bucket))
        group_rows.append(row)
    return pd.DataFrame(route_rows), pd.DataFrame(group_rows)


def add_ridership_metrics(results, classifications):
    path = RESULTS / "ridership_by_route_day_type.csv"
    if not path.exists():
        return pd.DataFrame(), pd.DataFrame()
    detail = pd.read_csv(path, dtype={"route_id": str})
    class_lookup = classifications.set_index("route_id")["provisional_minority_route"].to_dict()
    detail["group"] = detail["route_id"].map(lambda value: "minority" if class_lookup.get(value) else "nonminority")
    rows = []
    for (group, service_day), values in detail.groupby(["group", "day_type"]):
        snapshot_n = values["snapshot_n"].sum()
        trip_n = values["trip_n"].sum()
        rows.append({
            "group": group,
            "day_type": service_day,
            "snapshot_n": int(snapshot_n),
            "avg_estimated_load": (values["avg_estimated_load"] * values["snapshot_n"]).sum() / snapshot_n,
            "trip_n": int(trip_n),
            "avg_trip_max_estimated_load": (values["avg_trip_max_estimated_load"] * values["trip_n"]).sum() / trip_n,
            "direct_count_n": int(values["direct_count_n"].sum()),
            "status_estimate_n": int(values["status_estimate_n"].sum()),
        })
    return detail, pd.DataFrame(rows)


def vehicle_year(vehicle_id):
    match = re.search(r"\d+", str(vehicle_id))
    if not match:
        return None
    number = int(match.group())
    for start, end, year in FLEET_SERIES:
        if start <= number <= end:
            return year
    return None


def calculate_fleet_age(classifications):
    path = RESULTS / "vehicle_assignments.csv"
    if not path.exists():
        return pd.DataFrame()
    assignments = pd.read_csv(path, dtype={"route_id": str, "vehicle_id": str})
    class_lookup = classifications.set_index("route_id")["provisional_minority_route"].to_dict()
    assignments["group"] = assignments["route_id"].map(lambda value: "minority" if class_lookup.get(value) else "nonminority")
    assignments["model_year"] = assignments["vehicle_id"].map(vehicle_year)
    assignments["vehicle_age"] = 2026 - assignments["model_year"]
    rows = []
    for group, values in assignments.groupby("group"):
        covered = values[values["vehicle_age"].notna()]
        rows.append({
            "group": group,
            "snapshot_n": int(values["snapshot_n"].sum()),
            "covered_snapshot_n": int(covered["snapshot_n"].sum()),
            "coverage_pct": pct(covered["snapshot_n"].sum(), values["snapshot_n"].sum()),
            "snapshot_weighted_vehicle_age": (
                (covered["vehicle_age"] * covered["snapshot_n"]).sum() / covered["snapshot_n"].sum()
                if covered["snapshot_n"].sum() else None
            ),
        })
    return pd.DataFrame(rows)


def scheduled_group_rows(headways):
    rows = []
    selected = headways[headways["provisional_minority_route"].notna()].copy()
    selected["group"] = selected["provisional_minority_route"].map({True: "minority", False: "nonminority"})
    for (group, service_day, period), values in selected.groupby(["group", "day_type", "period"]):
        rows.append({
            "group": group,
            "day_type": service_day,
            "period": period,
            "headway_n": len(values),
            "average_scheduled_headway_min": values["headway_minutes"].mean(),
        })
    for (group, service_day), values in selected.groupby(["group", "day_type"]):
        rows.append({
            "group": group,
            "day_type": service_day,
            "period": "all",
            "headway_n": len(values),
            "average_scheduled_headway_min": values["headway_minutes"].mean(),
        })
    return pd.DataFrame(rows)


def scheduled_route_rows(headways):
    rows = []
    for (route_id, service_day, period), values in headways.groupby(["route_id", "day_type", "period"]):
        rows.append({
            "route_id": route_id,
            "day_type": service_day,
            "period": period,
            "headway_n": len(values),
            "average_scheduled_headway_min": values["headway_minutes"].mean(),
        })
    for (route_id, service_day), values in headways.groupby(["route_id", "day_type"]):
        rows.append({
            "route_id": route_id,
            "day_type": service_day,
            "period": "all",
            "headway_n": len(values),
            "average_scheduled_headway_min": values["headway_minutes"].mean(),
        })
    return pd.DataFrame(rows)


def access_group_rows(access):
    rows = []
    for minority, values in access.groupby("minority"):
        group = "minority" if minority else "nonminority"
        for service_day in ["weekday", "saturday", "sunday"]:
            rows.append({
                "group": group,
                "day_type": service_day,
                "block_group_n": len(values),
                "accessible_block_group_n": int(values[f"{service_day}_access"].sum()),
                "accessible_block_group_pct": 100 * values[f"{service_day}_access"].mean(),
            })
    return pd.DataFrame(rows)


def plot_route_map(route_geometries, block_groups, classifications, service_geoids):
    class_lookup = classifications.set_index("route_id")["provisional_minority_route"].to_dict()
    county_geometry = unary_union([block_group["geometry"] for block_group in block_groups])
    fig, axis = plt.subplots(figsize=(8.2, 9.2))
    for block_group in block_groups:
        if block_group["geoid"] not in service_geoids:
            continue
        x, y = block_group["geometry"].exterior.xy
        fill = "#f1d7da" if block_group["minority"] else "#d8eaec"
        axis.fill(x, y, color=fill, linewidth=0)
    for route_id, geometry in route_geometries.items():
        color = COLORS["minority"] if class_lookup.get(route_id) else COLORS["nonminority"]
        geometry = geometry.intersection(county_geometry)
        parts = geometry.geoms if hasattr(geometry, "geoms") else [geometry]
        for part in parts:
            if not hasattr(part, "xy"):
                continue
            x, y = part.xy
            axis.plot(x, y, color=color, linewidth=1.25, alpha=0.78)
    axis.set_aspect("equal")
    axis.set_axis_off()
    axis.set_title("A current route-level Title VI screen", loc="left", color=COLORS["ink"], fontsize=18, fontweight="bold", pad=16)
    axis.text(0, 1.01, "Fall 2026 routes · 2020–2024 ACS · provisional 74% POC block-group threshold", transform=axis.transAxes, color=COLORS["muted"], fontsize=10)
    axis.legend(handles=[
        Line2D([0], [0], color=COLORS["minority"], lw=3, label="Provisional minority route"),
        Line2D([0], [0], color=COLORS["nonminority"], lw=3, label="Other route"),
    ], frameon=False, loc="lower left")
    save_figure(fig, "route-classification-map.png")


def write_map_geojson(route_geometries, block_groups, classifications, service_geoids):
    to_wgs84 = Transformer.from_crs("EPSG:3310", "EPSG:4326", always_xy=True).transform
    county_geometry = unary_union([block_group["geometry"] for block_group in block_groups])
    class_lookup = classifications.set_index("route_id").to_dict("index")
    features = []

    for block_group in block_groups:
        if block_group["geoid"] not in service_geoids:
            continue
        geometry = transform(to_wgs84, block_group["geometry"].simplify(12, preserve_topology=True))
        features.append({
            "type": "Feature",
            "properties": {
                "kind": "block_group",
                "geoid": block_group["geoid"],
                "population": int(block_group["population"]),
                "poc_share_pct": round(100 * block_group["poc_share"], 1),
                "minority": block_group["minority"],
            },
            "geometry": mapping(geometry),
        })

    for route_id, route_geometry in route_geometries.items():
        geometry = route_geometry.intersection(county_geometry).simplify(5, preserve_topology=True)
        if geometry.is_empty:
            continue
        classification = class_lookup[route_id]
        features.append({
            "type": "Feature",
            "properties": {
                "kind": "route",
                "route_id": route_id,
                "minority_route": classification["provisional_minority_route"],
                "minority_mileage_share_pct": round(100 * classification["minority_mileage_share"], 1),
            },
            "geometry": mapping(transform(to_wgs84, geometry)),
        })

    collection = {"type": "FeatureCollection", "features": features}
    (DATA / "map-data.geojson").write_text(json.dumps(collection, separators=(",", ":")) + "\n")


def plot_monitoring(performance, scheduled, access):
    groups = ["minority", "nonminority"]
    colors = [COLORS["minority"], COLORS["nonminority"]]
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.4))
    for axis in axes.flat:
        configure_axis(axis)

    all_day = performance[performance["day_type"] == "all"].set_index("group")
    metrics = ["service_operated_pct", "otp_operated_pct", "on_time_service_delivered_pct"]
    labels = ["Trip delivery", "Departure-window\narrival proxy", "On Time Service\nDelivered"]
    x = range(len(metrics))
    for offset, group, color in zip([-0.18, 0.18], groups, colors):
        values = [all_day.loc[group, metric] for metric in metrics]
        axes[0, 0].bar([value + offset for value in x], values, width=0.34, color=color, label=group.replace("nonminority", "other").title())
    axes[0, 0].set_xticks(list(x), labels)
    axes[0, 0].set_ylim(50, 100)
    axes[0, 0].set_ylabel("October 2026 percent")
    axes[0, 0].set_title("Observed service outcomes", loc="left", fontweight="bold", color=COLORS["ink"])
    axes[0, 0].legend(frameon=False, fontsize=9)

    schedule_labels = [("weekday", "peak", "Weekday\npeak"), ("weekday", "offpeak", "Weekday\noff-peak"), ("saturday", "all", "Saturday"), ("sunday", "all", "Sunday")]
    for offset, group, color in zip([-0.18, 0.18], groups, colors):
        values = []
        for service_day, period, _ in schedule_labels:
            subset = scheduled[(scheduled["group"] == group) & (scheduled["day_type"] == service_day)]
            subset = subset[subset["period"] == period]
            values.append(subset["average_scheduled_headway_min"].mean())
        axes[0, 1].bar([value + offset for value in range(4)], values, width=0.34, color=color)
    axes[0, 1].set_xticks(range(4), [label for _, _, label in schedule_labels])
    axes[0, 1].set_ylabel("Average scheduled headway (min)")
    axes[0, 1].set_title("Scheduled frequency", loc="left", fontweight="bold", color=COLORS["ink"])

    wait_metrics = ["expected_wait_min", "scheduled_expected_wait_min"]
    wait_labels = ["Actual expected\nwait", "Scheduled expected\nwait"]
    for offset, group, color in zip([-0.18, 0.18], groups, colors):
        values = [all_day.loc[group, metric] for metric in wait_metrics]
        axes[1, 0].bar([value + offset for value in range(2)], values, width=0.34, color=color)
    axes[1, 0].set_xticks(range(2), wait_labels)
    axes[1, 0].set_ylabel("Minutes")
    axes[1, 0].set_title("Wait implied by bus spacing", loc="left", fontweight="bold", color=COLORS["ink"])

    access_pivot = access.pivot(index="day_type", columns="group", values="accessible_block_group_pct")
    access_order = ["weekday", "saturday", "sunday"]
    for offset, group, color in zip([-0.18, 0.18], groups, colors):
        axes[1, 1].bar([value + offset for value in range(3)], [access_pivot.loc[value, group] for value in access_order], width=0.34, color=color)
    axes[1, 1].set_xticks(range(3), ["Weekday", "Saturday", "Sunday"])
    axes[1, 1].set_ylim(0, 100)
    axes[1, 1].set_ylabel("Share of proxy service-area block groups")
    axes[1, 1].set_title("Access to 14+ daily trips", loc="left", fontweight="bold", color=COLORS["ink"])

    fig.suptitle("The same screen, with current and rider-facing measures", x=0.06, ha="left", color=COLORS["ink"], fontsize=19, fontweight="bold")
    fig.text(0.06, 0.94, "Provisional classification · October 2026 observed data · representative Fall 2026 schedules", color=COLORS["muted"], fontsize=10)
    fig.tight_layout(rect=(0, 0, 1, 0.91), h_pad=2.5, w_pad=2.2)
    save_figure(fig, "monitoring-comparison.png")


def plot_route_detail(route_metrics, classifications):
    detail = route_metrics[route_metrics["day_type"] == "all"].merge(classifications[["route_id", "provisional_minority_route"]], on="route_id")
    detail = detail[
        (detail["scheduled_trips"] >= 100)
        & detail["on_time_service_delivered_pct"].notna()
        & (detail["on_time_service_delivered_pct"] >= 20)
    ].nsmallest(18, "on_time_service_delivered_pct").sort_values("on_time_service_delivered_pct")
    colors = [COLORS["minority"] if value else COLORS["nonminority"] for value in detail["provisional_minority_route"]]
    fig, axis = plt.subplots(figsize=(9, 7))
    configure_axis(axis)
    axis.barh(detail["route_id"], detail["on_time_service_delivered_pct"], color=colors)
    axis.set_xlim(40, 82)
    axis.set_xlabel("On Time Service Delivered (%)")
    axis.set_title("Where the rider window fails most often", loc="left", color=COLORS["ink"], fontsize=18, fontweight="bold", pad=16)
    axis.text(0, 1.01, "Lowest October results among routes with at least 100 scheduled trips", transform=axis.transAxes, color=COLORS["muted"], fontsize=10)
    for index, value in enumerate(detail["on_time_service_delivered_pct"]):
        axis.text(value + 0.35, index, f"{value:.1f}%", va="center", fontsize=9, color=COLORS["ink"])
    axis.legend(handles=[
        Line2D([0], [0], color=COLORS["minority"], lw=6, label="Provisional minority route"),
        Line2D([0], [0], color=COLORS["nonminority"], lw=6, label="Other route"),
    ], frameon=False, loc="lower right")
    fig.tight_layout()
    save_figure(fig, "route-detail.png")


def write_csv(frame, filename):
    output = frame.copy()
    for column in output.select_dtypes(include="number").columns:
        output[column] = output[column].round(4)
    output.to_csv(RESULTS / filename, index=False)


def main():
    args = parse_args()
    RESULTS.mkdir(parents=True, exist_ok=True)
    PLOTS.mkdir(parents=True, exist_ok=True)
    DATA.mkdir(parents=True, exist_ok=True)

    block_groups = load_block_groups(args.source_root)
    route_geometries = load_routes(args.source_root)
    classifications = classify_routes(route_geometries, block_groups)
    gtfs = read_gtfs(args.source_root)
    headways = calculate_scheduled_headways(gtfs, classifications)
    scheduled = scheduled_group_rows(headways)
    scheduled_routes = scheduled_route_rows(headways)
    access_detail = calculate_access(gtfs, block_groups)
    access = access_group_rows(access_detail)
    _, stop_distribution = calculate_stop_distribution(gtfs, block_groups)
    route_metrics, performance = aggregate_performance(args.source_root, classifications)
    ridership_detail, ridership_groups = add_ridership_metrics(RESULTS, classifications)
    fleet_age = calculate_fleet_age(classifications)

    write_csv(classifications, "route_classifications.csv")
    write_csv(route_metrics, "route_metrics.csv")
    write_csv(performance, "performance_by_group.csv")
    write_csv(scheduled, "scheduled_headways_by_group.csv")
    write_csv(scheduled_routes, "scheduled_headways_by_route.csv")
    write_csv(access, "access_by_group.csv")
    write_csv(access_detail, "access_block_groups.csv")
    write_csv(stop_distribution, "stops_by_group.csv")
    if not ridership_detail.empty:
        write_csv(ridership_groups, "ridership_by_group.csv")
    if not fleet_age.empty:
        write_csv(fleet_age, "fleet_age_by_group.csv")

    plot_route_map(route_geometries, block_groups, classifications, set(access_detail["geoid"]))
    write_map_geojson(route_geometries, block_groups, classifications, set(access_detail["geoid"]))
    plot_monitoring(performance, scheduled, access)
    plot_route_detail(route_metrics, classifications)

    all_day = performance[performance["day_type"] == "all"].set_index("group")
    summary = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "analysis_period": "2026-10-01/2026-10-31",
        "acs_vintage": "2020-2024 ACS 5-year",
        "gtfs_signup": "Fall 2026",
        "poc_threshold": POC_THRESHOLD,
        "route_count": len(classifications),
        "provisional_minority_route_count": int(classifications["provisional_minority_route"].sum()),
        "proxy_service_area_block_group_count": len(access_detail),
        "stop_distribution": stop_distribution.to_dict("records"),
        "groups": {
            group: {key: round_or_none(all_day.loc[group, key]) for key in [
                "service_operated_pct", "otp_operated_pct", "otp_scheduled_pct",
                "on_time_service_delivered_pct", "expected_wait_min",
                "scheduled_expected_wait_min", "bunched_headway_pct", "long_gap_pct",
            ]}
            for group in ["minority", "nonminority"]
        },
    }
    if not ridership_groups.empty:
        summary["ridership"] = ridership_groups.to_dict("records")
    if not fleet_age.empty:
        summary["fleet_age"] = fleet_age.to_dict("records")
    (RESULTS / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    public_routes = classifications.merge(
        route_metrics[route_metrics["day_type"] == "all"],
        on="route_id",
        how="left",
    ).sort_values("route_id")
    (DATA / "route-metrics.json").write_text(public_routes.to_json(orient="records", indent=2) + "\n")
    for filename in [
        "route_classifications.csv", "route_metrics.csv", "scheduled_headways_by_route.csv",
        "scheduled_headways_by_group.csv", "performance_by_group.csv", "access_by_group.csv",
        "access_block_groups.csv", "ridership_by_group.csv", "ridership_by_route_day_type.csv",
        "fleet_age_by_group.csv", "stops_by_group.csv", "summary.json",
    ]:
        shutil.copy2(RESULTS / filename, DATA / filename)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
