#!/usr/bin/env python3
"""
Calculate NDVI time series and statistics from Sentinel-2 imagery.
"""

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Tuple

import ee
import numpy as np
import yaml  # type: ignore


def init_ee():
    """Initialize Earth Engine."""
    try:
        ee.Initialize(project="fluent-optics-344414")
    except Exception:
        ee.Authenticate()
        ee.Initialize(project="fluent-optics-344414")


def load_config() -> Dict:
    """Load configuration from YAML."""
    import yaml

    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


def load_all_sites() -> Dict:
    """Load all sites from GeoJSON."""
    with open("all_sites.geojson", "r") as f:
        return json.load(f)


def calculate_gaps(dates: List[str], threshold_days: int = 5) -> Tuple[int, int, float]:
    """Calculate gap statistics for scene dates."""
    if len(dates) < 2:
        return 0, 0, 0.0

    # Convert string dates to datetime objects and sort
    unique_dates = sorted(set(datetime.fromisoformat(date) for date in dates))

    if len(unique_dates) < 2:
        return 0, 0, 0.0

    gaps = [
        (unique_dates[i] - unique_dates[i - 1]).days
        for i in range(1, len(unique_dates))
    ]
    gaps = [g for g in gaps if g > threshold_days]

    return (
        max(gaps, default=0),
        len(gaps),
        sum(g * g for g in gaps) / (len(unique_dates) - 1) if gaps else 0.0,
    )


def detect_outliers_upper_envelope(
    ndvi_data: List[Dict],
    window_days: int = 30,
    percentile: int = 80,
    threshold_below: float = 0.15,
) -> List[bool]:
    """
    Detect outliers based on deviation from upper envelope.

    Args:
        ndvi_data: List of dicts with 'date' and 'ndvi' keys (ndvi can be None)
        window_days: Size of rolling window in days
        percentile: Which percentile to use for upper envelope (e.g., 80th)
        threshold_below: How far below envelope to consider outlier

    Returns:
        List of boolean values indicating if each point is an outlier
    """
    from datetime import datetime, timedelta

    # Filter out None values and prepare data
    valid_data = [
        (datetime.fromisoformat(d["date"]), d["ndvi"])
        for d in ndvi_data
        if d["ndvi"] is not None
    ]

    if not valid_data:
        return [False] * len(ndvi_data)

    # Sort by date
    valid_data.sort(key=lambda x: x[0])

    # Calculate upper envelope for each point
    upper_envelopes = {}
    for target_date, target_ndvi in valid_data:
        # Get values within window
        window_start = target_date - timedelta(days=window_days // 2)
        window_end = target_date + timedelta(days=window_days // 2)

        window_values = [
            ndvi
            for date, ndvi in valid_data
            if window_start <= date <= window_end
            and ndvi >= 0.1  # Exclude obviously bad values
        ]

        if window_values:
            # Calculate percentile for this window
            envelope_value = np.percentile(window_values, percentile)
            upper_envelopes[target_date.date().isoformat()] = envelope_value

    # Determine outliers
    outliers = []
    for d in ndvi_data:
        if d["ndvi"] is None:
            outliers.append(False)
        else:
            ndvi_val = d["ndvi"]
            date_str = d["date"]

            # Simple absolute threshold for obvious outliers
            if ndvi_val < 0.1:
                outliers.append(True)
            elif date_str in upper_envelopes:
                # Check if value is too far below envelope
                diff = upper_envelopes[date_str] - ndvi_val
                is_outlier = bool(diff > threshold_below)
                outliers.append(is_outlier)
            else:
                # If we couldn't calculate envelope, don't mark as outlier
                outliers.append(False)

    return outliers


def fetch_ndvi_time_series(lat, lon, start_date, end_date):
    """Fetch NDVI time series from GEE for a given location and date range."""
    from collections import defaultdict

    import ee

    point = ee.Geometry.Point([lon, lat])
    region = point.buffer(100)
    all_collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(region)
        .filterDate(start_date, end_date)
    )
    all_dates = (
        all_collection.map(
            lambda img: ee.Feature(None, {"date": img.date().format("YYYY-MM-dd")})
        )
        .distinct("date")
        .getInfo()["features"]
    )
    scene_dates = sorted(set(f["properties"]["date"] for f in all_dates))

    def mask_clouds(image):
        qa = image.select("QA60")
        cloud_mask = qa.bitwiseAnd(1 << 10).eq(0).And(qa.bitwiseAnd(1 << 11).eq(0))
        return image.updateMask(cloud_mask)

    def add_ndvi(image):
        ndvi = image.normalizedDifference(["B8", "B4"]).rename("NDVI")
        return image.addBands(ndvi).set("date", image.date().format("YYYY-MM-dd"))

    ndvi_collection = all_collection.map(mask_clouds).map(add_ndvi)

    def extract_ndvi(image):
        ndvi_value = image.select("NDVI").reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=region,
            scale=10,
            maxPixels=1e9,
        )
        return ee.Feature(
            None,
            {
                "date": image.get("date"),
                "ndvi": ndvi_value.get("NDVI"),
            },
        )

    time_series = ndvi_collection.map(extract_ndvi)
    ts_list = time_series.getInfo()["features"]

    date_ndvi = defaultdict(list)
    for feature in ts_list:
        ndvi_val = feature["properties"].get("ndvi")
        if ndvi_val is not None:
            date = feature["properties"]["date"]
            date_ndvi[date].append(ndvi_val)

    ndvi_series = []
    for date in scene_dates:
        if date in date_ndvi:
            values = date_ndvi[date]
            avg_ndvi = sum(values) / len(values)
            ndvi_series.append({"date": date, "ndvi": round(avg_ndvi, 4)})
        else:
            ndvi_series.append({"date": date, "ndvi": None})

    return ndvi_series


def calculate_ndvi_from_series(ndvi_series):
    """Calculate NDVI statistics, outliers, and gaps from a pre-fetched NDVI time series."""
    config = load_config()
    window_days = config.get("envelope_window_days", 30)
    percentile = config.get("envelope_percentile", 80)
    threshold_below = config.get("envelope_threshold_below", 0.15)

    outlier_flags = detect_outliers_upper_envelope(
        ndvi_series,
        window_days=window_days,
        percentile=percentile,
        threshold_below=threshold_below,
    )

    ndvi_values = []
    used_dates = []

    for entry, is_outlier in zip(ndvi_series, outlier_flags):
        if entry["ndvi"] is not None:
            entry["outlier"] = is_outlier
            if not is_outlier:
                ndvi_values.append(entry["ndvi"])
                used_dates.append(entry["date"])

    gap_stats = calculate_gaps(used_dates, threshold_days=3)

    stats = {
        "ndvi_time_series": ndvi_series,
        "ndvi_observations": len(ndvi_values),
        "ndvi_mean": round(sum(ndvi_values) / len(ndvi_values), 4)
        if ndvi_values
        else 0.0,
        "ndvi_min": round(min(ndvi_values), 4) if ndvi_values else 0.0,
        "ndvi_max": round(max(ndvi_values), 4) if ndvi_values else 0.0,
        "ndvi_max_s2_gap_days": gap_stats[0],
        "ndvi_s2_gap_count": gap_stats[1],
        "ndvi_s2_weighted_gap_score": round(gap_stats[2], 2),
        "ndvi_range": round(max(ndvi_values) - min(ndvi_values), 4)
        if ndvi_values
        else 0.0,
    }
    return stats


def calculate_ndvi(lat: float, lon: float, start_date: str, end_date: str) -> Dict:
    """Calculate NDVI time series for all S2 dates, with null for cloudy/invalid."""
    ndvi_series = fetch_ndvi_time_series(lat, lon, start_date, end_date)
    return calculate_ndvi_from_series(ndvi_series)


def process_site_season(
    site_id: str,
    year: str,
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
) -> Tuple[str, str, Dict]:
    """Process NDVI for a single site-season combination."""
    # Initialize EE for this thread
    ee.Initialize(project="fluent-optics-344414")

    try:
        ndvi_stats = calculate_ndvi(lat, lon, start_date, end_date)
        return site_id, year, ndvi_stats
    except Exception as e:
        raise Exception(f"Error calculating NDVI for {site_id} {year}: {e}")


def process_ndvi(data):
    """Calculate NDVI for selected sites and seasons from config with parallel processing."""
    # Load config to get selected seasons
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    sites_config = config.get("sites", {})

    # Build list of tasks to process
    tasks = []
    for site_id, site_data in data["sites"].items():
        selected_years = sites_config.get(site_id, [])
        if not selected_years:
            continue

        lat = site_data["lat"]
        lon = site_data["lon"]

        for year, season in site_data["seasons"].items():
            if int(year) not in selected_years and year not in [
                str(y) for y in selected_years
            ]:
                continue

            start_date = season.get("season_start_date")
            end_date = season.get("season_end_date")

            if start_date and end_date:
                tasks.append((site_id, year, lat, lon, start_date, end_date))

    if not tasks:
        print("No sites/seasons selected for NDVI processing")
        return data

    print(f"Processing NDVI for {len(tasks)} site-seasons using parallel execution")
    print("Using 5 parallel threads\n")

    completed = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                process_site_season,
                site_id,
                year,
                lat,
                lon,
                start_date,
                end_date,
            ): (site_id, year)
            for site_id, year, lat, lon, start_date, end_date in tasks
        }

        # Process completed futures
        for future in as_completed(futures):
            site_id, year = futures[future]
            try:
                processed_site, processed_year, ndvi_stats = future.result()

                # Update the data
                season = data["sites"][processed_site]["seasons"][processed_year]
                season["ndvi_mean"] = ndvi_stats["ndvi_mean"]
                season["ndvi_min"] = ndvi_stats["ndvi_min"]
                season["ndvi_max"] = ndvi_stats["ndvi_max"]
                season["ndvi_range"] = ndvi_stats["ndvi_range"]
                season["ndvi_observations"] = ndvi_stats["ndvi_observations"]
                season["ndvi_time_series"] = ndvi_stats["ndvi_time_series"]
                season["ndvi_max_s2_gap_days"] = ndvi_stats["ndvi_max_s2_gap_days"]
                season["ndvi_s2_gap_count"] = ndvi_stats["ndvi_s2_gap_count"]
                season["ndvi_s2_weighted_gap_score"] = ndvi_stats[
                    "ndvi_s2_weighted_gap_score"
                ]

                completed += 1
                print(
                    f"[{completed}/{len(tasks)}] {processed_site} {processed_year}: "
                    f"{ndvi_stats['ndvi_observations']} obs, "
                    f"mean={ndvi_stats['ndvi_mean']:.3f}"
                )
            except Exception as e:
                completed += 1
                print(f"[{completed}/{len(tasks)}] {site_id} {year}: Failed - {e}")
                # Keep default values
                data["sites"][site_id]["seasons"][year]["ndvi_range"] = 0.0

    return data


def save_selected_sites(geojson: Dict):
    """Save selected sites with NDVI data to GeoJSON."""
    with open("selected_sites.geojson", "w") as f:
        json.dump(geojson, f, indent=2, default=str)
    print("Updated selected_sites.geojson with NDVI statistics")


def fetch_all_raw_ndvi():
    """Fetch NDVI time series for all selected sites/seasons and store in selected_sites.geojson."""
    import json

    import yaml

    all_sites_path = "all_sites.geojson"
    selected_sites_path = "selected_sites.geojson"
    config_path = "config.yaml"

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Read season dates from all_sites.geojson (has updated full-year dates)
    with open(all_sites_path, "r") as f:
        all_sites_geojson = json.load(f)

    # Try to read existing selected_sites.geojson, or create from all_sites
    try:
        with open(selected_sites_path, "r") as f:
            selected_geojson = json.load(f)
    except FileNotFoundError:
        selected_geojson = {"type": "FeatureCollection", "features": []}

    all_features_by_site = {
        f["properties"]["sitename"]: f for f in all_sites_geojson["features"]
    }
    selected_features_by_site = {
        f["properties"]["sitename"]: f for f in selected_geojson["features"]
    }

    for site_id, years in config["sites"].items():
        all_feature = all_features_by_site.get(site_id)
        if not all_feature:
            continue

        # Get or create feature in selected_sites
        if site_id in selected_features_by_site:
            feature = selected_features_by_site[site_id]
        else:
            # Copy from all_sites
            feature = json.loads(json.dumps(all_feature))
            selected_geojson["features"].append(feature)
            selected_features_by_site[site_id] = feature

        props = feature["properties"]
        lat = feature["geometry"]["coordinates"][1]
        lon = feature["geometry"]["coordinates"][0]

        for year in years:
            year = str(year)
            # Get season dates from all_sites (updated with full year)
            all_season = all_features_by_site[site_id]["properties"]["seasons"].get(
                year
            )
            if not all_season:
                continue

            # Update season dates in selected_sites
            if "seasons" not in props:
                props["seasons"] = {}
            if year not in props["seasons"]:
                props["seasons"][year] = json.loads(json.dumps(all_season))
            else:
                # Update dates but preserve existing NDVI data
                props["seasons"][year]["season_start_date"] = all_season[
                    "season_start_date"
                ]
                props["seasons"][year]["season_end_date"] = all_season[
                    "season_end_date"
                ]
                props["seasons"][year]["season_length_days"] = all_season[
                    "season_length_days"
                ]

            start_date = props["seasons"][year]["season_start_date"]
            end_date = props["seasons"][year]["season_end_date"]

            ndvi_series = fetch_ndvi_time_series(lat, lon, start_date, end_date)
            props["seasons"][year]["ndvi_time_series_raw"] = ndvi_series
            print(f"Fetched NDVI for {site_id} {year}: {len(ndvi_series)} dates")

    with open(selected_sites_path, "w") as f:
        json.dump(selected_geojson, f, indent=2)
    print("Updated selected_sites.geojson with raw NDVI time series.")


def analyze_all_ndvi():
    """Analyze NDVI time series for all selected sites/seasons using cached raw NDVI."""
    import json

    import yaml

    geojson_path = "selected_sites.geojson"
    config_path = "config.yaml"

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    with open(geojson_path, "r") as f:
        geojson = json.load(f)

    features_by_site = {f["properties"]["sitename"]: f for f in geojson["features"]}

    for site_id, years in config["sites"].items():
        feature = features_by_site.get(site_id)
        if not feature:
            continue
        props = feature["properties"]
        for year in years:
            year = str(year)
            season = props.get("seasons", {}).get(year)
            if not season:
                continue
            ndvi_series = season.get("ndvi_time_series_raw") or season.get(
                "ndvi_time_series"
            )
            if not ndvi_series:
                print(f"No NDVI data for {site_id} {year}, skipping.")
                continue

            # Convert existing data to raw format (remove outlier flags if present)
            raw_series = []
            for entry in ndvi_series:
                raw_entry = {"date": entry["date"], "ndvi": entry["ndvi"]}
                raw_series.append(raw_entry)

            stats = calculate_ndvi_from_series(raw_series)
            season.update(stats)
            print(
                f"Analyzed NDVI for {site_id} {year}: {stats['ndvi_observations']} obs"
            )

    with open(geojson_path, "w") as f:
        json.dump(geojson, f, indent=2)
    print("Updated selected_sites.geojson with NDVI statistics.")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["fetch", "analyze", "full"], default="full")
    args = parser.parse_args()

    init_ee()

    if args.mode == "fetch":
        fetch_all_raw_ndvi()
    elif args.mode == "analyze":
        analyze_all_ndvi()
    else:
        fetch_all_raw_ndvi()
        analyze_all_ndvi()


if __name__ == "__main__":
    main()
