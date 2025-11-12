#!/usr/bin/env python3
"""
Run all data collection scripts in sequence.
"""

import subprocess
import sys
from pathlib import Path


def run_script(script_name: str) -> bool:
    """Run a script and return success status."""
    print(f"\n{'=' * 50}")
    print(f"Running {script_name}...")
    print("=" * 50)

    try:
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            check=True,
        )

        # Print output
        if result.stdout:
            print(result.stdout)

        return True

    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}:")
        print(e.stderr if e.stderr else str(e))
        return False


def main():
    """Run all scripts in sequence."""
    scripts = ["phenocam.py", "get_scenes.py", "get_indices.py"]

    print("Starting European PhenoCam data collection pipeline...")
    print("\nWorkflow:")
    print("  1. phenocam.py   - Fetch ALL European PhenoCam sites → all_sites.geojson")
    print("  2. get_scenes.py - Get satellite scenes for ALL sites → all_sites.geojson")
    print(
        "  3. get_indices.py - Calculate NDVI for config.yaml sites → selected_sites.geojson"
    )
    print("\nThis will process all European sites (~35-40 sites) for satellite data,")
    print("but only calculate NDVI time series for your selected sites.")

    for script in scripts:
        if not Path(script).exists():
            print(f"Error: {script} not found!")
            sys.exit(1)

        success = run_script(script)
        if not success:
            print(f"\nPipeline stopped due to error in {script}")
            sys.exit(1)

    print("\n" + "=" * 50)
    print("✅ All scripts completed successfully!")
    print("Output files:")
    print("  - all_sites.geojson: ALL European sites with scene statistics")
    print("  - selected_sites.geojson: Config sites with NDVI time series")
    print("=" * 50)


if __name__ == "__main__":
    main()
