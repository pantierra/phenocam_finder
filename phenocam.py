#!/usr/bin/env python3
"""
Fetch PhenoCam site data for all European sites and organize by season.
"""

import json
from datetime import datetime
from typing import Dict

import requests
import yaml  # type: ignore

PHENOCAM_API = "https://phenocam.nau.edu/api/cameras/"

# European geographical bounds (approximate)
EUROPE_BOUNDS = {
    "lat_min": 35.0,  # Southern Europe
    "lat_max": 71.0,  # Northern Europe
    "lon_min": -10.0,  # Western Europe (Atlantic)
    "lon_max": 40.0,  # Eastern Europe
}


def load_config() -> Dict:
    """Load configuration for NDVI selection only."""
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


def is_in_europe(lat: float, lon: float) -> bool:
    """Check if coordinates are within European bounds."""
    return (
        EUROPE_BOUNDS["lat_min"] <= lat <= EUROPE_BOUNDS["lat_max"]
        and EUROPE_BOUNDS["lon_min"] <= lon <= EUROPE_BOUNDS["lon_max"]
    )


def get_all_european_sites() -> Dict[str, Dict]:
    """Fetch all PhenoCam sites located in Europe."""
    european_sites = {}
    url = PHENOCAM_API
    page = 1

    print("Fetching all European PhenoCam sites...")

    while url:
        print(f"  Fetching page {page}...")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Check current page of results
        for camera in data.get("results", []):
            lat = camera.get("Lat")
            lon = camera.get("Lon")
            site_id = camera.get("Sitename")

            if lat and lon and site_id:
                if is_in_europe(lat, lon):
                    # Extract vegetation type
                    sitemetadata = camera.get("sitemetadata", {})
                    veg_type = sitemetadata.get("primary_veg_type", "")

                    # Convert vegetation type codes to readable names
                    veg_type_map = {
                        "GR": "Grassland",
                        "AG": "Agriculture",
                        "DB": "Deciduous Broadleaf",
                        "EN": "Evergreen Needleleaf",
                        "SH": "Shrubland",
                        "WL": "Wetland",
                        "TU": "Tundra",
                        "DN": "Deciduous Needleleaf",
                        "EB": "Evergreen Broadleaf",
                        "MX": "Mixed Forest",
                    }

                    european_sites[site_id] = {
                        "sitename": site_id,
                        "lat": lat,
                        "lon": lon,
                        "vegetation_type": veg_type_map.get(veg_type, veg_type),
                        "description": sitemetadata.get(
                            "site_description", f"PhenoCam site {site_id}"
                        ),
                        "elevation": camera.get("Elev"),
                        "country": sitemetadata.get("country", ""),
                        "date_first": camera.get("date_first"),
                        "date_last": camera.get("date_last"),
                    }

        # Move to next page if available
        url = data.get("next")
        page += 1

    print(f"Found {len(european_sites)} European sites")
    return european_sites


def get_site_seasons(site_info: Dict) -> Dict:
    """Get seasonal data for a site based on camera date range."""
    seasons = {}

    # Define season boundaries (Whole year)
    season_start_month_day = (1, 1)  # January 1
    season_end_month_day = (12, 31)  # December 31

    # Don't create seasons beyond 2024 (no satellite data for future)
    max_year = 2024

    date_first = site_info.get("date_first")
    date_last = site_info.get("date_last")

    if date_first and date_last:
        try:
            # Parse dates
            start_date = datetime.strptime(date_first, "%Y-%m-%d")
            end_date = datetime.strptime(date_last, "%Y-%m-%d")

            # Generate seasons for all years where camera covers the full season
            for year in range(start_date.year, min(end_date.year + 1, max_year + 1)):
                season_start = datetime(year, *season_start_month_day)
                season_end = datetime(year, *season_end_month_day)

                # Check if camera covers entire season
                if start_date <= season_start and end_date >= season_end:
                    seasons[str(year)] = {
                        "season_start_date": season_start.strftime("%Y-%m-%d"),
                        "season_end_date": season_end.strftime("%Y-%m-%d"),
                        "season_length_days": (season_end - season_start).days,
                        "vegetation_type": site_info.get("vegetation_type", ""),
                        # Placeholders for satellite data
                        "sentinel2_scenes": 0,
                        "sentinel3_scenes": 0,
                        "s2_cloud_cover_mean": 0.0,
                        "max_s2_gap_days": 0,
                        "s2_gap_count": 0,
                        "s2_weighted_gap_score": 0.0,
                        # Placeholders for NDVI
                        "ndvi_mean": 0.0,
                        "ndvi_min": 0.0,
                        "ndvi_max": 0.0,
                        "ndvi_observations": 0,
                        "ndvi_time_series": [],
                        # NDVI gap statistics (for clear scenes only)
                        "ndvi_max_s2_gap_days": 0,
                        "ndvi_s2_gap_count": 0,
                        "ndvi_s2_weighted_gap_score": 0.0,
                    }
        except Exception as e:
            print(f"    Error parsing dates for {site_info['sitename']}: {e}")

    # If no seasons found, add 2024 as default (latest year with satellite data)
    if not seasons:
        current_year = 2024  # Use 2024 as default (latest year with satellite data)
        season_start = datetime(current_year, *season_start_month_day)
        season_end = datetime(current_year, *season_end_month_day)

        seasons[str(current_year)] = {
            "season_start_date": season_start.strftime("%Y-%m-%d"),
            "season_end_date": season_end.strftime("%Y-%m-%d"),
            "season_length_days": (season_end - season_start).days,
            "vegetation_type": site_info.get("vegetation_type", ""),
            "sentinel2_scenes": 0,
            "sentinel3_scenes": 0,
            "s2_cloud_cover_mean": 0.0,
            "max_s2_gap_days": 0,
            "s2_gap_count": 0,
            "s2_weighted_gap_score": 0.0,
            "ndvi_mean": 0.0,
            "ndvi_min": 0.0,
            "ndvi_max": 0.0,
            "ndvi_observations": 0,
            "ndvi_time_series": [],
            # NDVI gap statistics (for clear scenes only)
            "ndvi_max_s2_gap_days": 0,
            "ndvi_s2_gap_count": 0,
            "ndvi_s2_weighted_gap_score": 0.0,
        }

    return seasons


def process_all_european_sites() -> Dict:
    """Process all European PhenoCam sites."""
    # Get all European sites
    european_sites = get_all_european_sites()

    # Load config to mark selected sites
    config = load_config()
    selected_sites = config.get("sites", {})

    results: Dict = {"sites": {}}

    print(f"\nProcessing {len(european_sites)} European sites...")
    for site_id, site_info in european_sites.items():
        try:
            # Create site data
            site_data = {
                "sitename": site_info["sitename"],
                "lat": site_info["lat"],
                "lon": site_info["lon"],
                "vegetation_type": site_info["vegetation_type"],
                "description": site_info["description"],
                "elevation": site_info["elevation"],
                "country": site_info["country"],
                "ndvi_selected": site_id in selected_sites,  # Mark if selected for NDVI
                "seasons": {},
            }

            # Get seasonal data
            seasons = get_site_seasons(site_info)
            site_data["seasons"] = seasons

            # Mark which seasons are selected for NDVI
            if site_id in selected_sites:
                selected_years = selected_sites[site_id]
                for year in site_data["seasons"]:
                    site_data["seasons"][year]["ndvi_selected"] = int(
                        year
                    ) in selected_years or year in [str(y) for y in selected_years]

            results["sites"][site_id] = site_data

        except Exception as e:
            print(f"Error processing {site_id}: {e}")
            continue

    # Print summary
    total_sites = len(results["sites"])
    total_seasons = sum(len(site["seasons"]) for site in results["sites"].values())
    ndvi_sites = sum(
        1 for site in results["sites"].values() if site.get("ndvi_selected", False)
    )
    ndvi_seasons = sum(
        1
        for site in results["sites"].values()
        if site.get("ndvi_selected", False)
        for season in site["seasons"].values()
        if season.get("ndvi_selected", False)
    )

    print("\nSummary:")
    print(f"  Total European sites: {total_sites}")
    print(f"  Total site-seasons: {total_seasons}")
    print(f"  Sites selected for NDVI: {ndvi_sites}")
    print(f"  Seasons selected for NDVI: {ndvi_seasons}")

    # Print countries represented
    countries = set(
        site.get("country", "Unknown")
        for site in results["sites"].values()
        if site.get("country")
    )
    if countries:
        print(f"  Countries: {', '.join(sorted(countries))}")

    return results


def save_results(data: Dict):
    """Save results to GeoJSON file."""
    # Convert to GeoJSON format
    features = []
    for site_id, site_data in data["sites"].items():
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [site_data["lon"], site_data["lat"]],
            },
            "properties": {
                "sitename": site_data["sitename"],
                "vegetation_type": site_data["vegetation_type"],
                "description": site_data["description"],
                "elevation": site_data["elevation"],
                "country": site_data.get("country", ""),
                "ndvi_selected": site_data.get("ndvi_selected", False),
                "seasons": site_data["seasons"],
            },
        }
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    with open("all_sites.geojson", "w") as f:
        json.dump(geojson, f, indent=2, default=str)
    print("Saved GeoJSON to all_sites.geojson")


def main():
    """Main entry point."""
    results = process_all_european_sites()
    save_results(results)


if __name__ == "__main__":
    main()
