#!/usr/bin/env python3
"""
PhenoCam Satellite Data Finder

A tool for identifying satellite data availability at PhenoCam network locations
with focus on European domain site selection for ecological research.
"""

from .config import EUROPE_BOUNDS, SATELLITE, SPATIAL
from .core import DataFinder

__version__ = "1.0.0"
__author__ = "PhenoCam Team"

__all__ = [
    "DataFinder",
    "EUROPE_BOUNDS",
    "SPATIAL",
    "SATELLITE",
]
