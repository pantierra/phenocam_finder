#!/usr/bin/env python3
"""
Simple main entry point for PhenoCam satellite data finder.
"""

import sys
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent / "src"))

from phenocam_finder.core import main

if __name__ == "__main__":
    main()
