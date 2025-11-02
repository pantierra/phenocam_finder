#!/usr/bin/env python3
"""
Tests for core DataFinder functionality.
"""

from unittest.mock import Mock, patch

from phenocam_finder.core import DataFinder


class TestDataFinderInit:
    """Test DataFinder initialization"""

    def test_init_default_cache_dir(self):
        """Test initialization with default cache directory"""
        finder = DataFinder()
        assert finder.phenocam is not None
        assert finder.satellite is not None

    def test_init_custom_cache_dir(self, temp_cache_dir):
        """Test initialization with custom cache directory"""
        finder = DataFinder(cache_dir=temp_cache_dir)
        assert finder.phenocam is not None
        assert finder.satellite is not None


class TestLocationProcessing:
    """Test location data processing workflows"""

    def test_find_data_for_locations_success(
        self, sample_european_locations, mock_satellite_response
    ):
        """Test finding data for locations successfully"""
        with patch("phenocam_finder.core.PhenoCamQuery") as mock_phenocam_class, patch(
            "phenocam_finder.core.SatelliteQuery"
        ) as mock_satellite_class:
            mock_phenocam = Mock()
            mock_phenocam.get_all_locations.return_value = sample_european_locations
            mock_phenocam_class.return_value = mock_phenocam

            mock_satellite = Mock()
            mock_satellite.search_satellite_data.return_value = mock_satellite_response
            mock_satellite_class.return_value = mock_satellite

            finder = DataFinder()
            results = finder.find_data_for_locations(max_locations=1)

            assert len(results) == 1
            assert results[0]["location"]["sitename"] == "bavarian-forest"
            assert "satellite_images" in results[0]
            assert results[0]["satellite_images"] == 10

    def test_find_data_for_locations_with_error(self, sample_european_locations):
        """Test finding data with API error"""
        with patch("phenocam_finder.core.PhenoCamQuery") as mock_phenocam_class, patch(
            "phenocam_finder.core.SatelliteQuery"
        ) as mock_satellite_class:
            mock_phenocam = Mock()
            mock_phenocam.get_all_locations.return_value = sample_european_locations
            mock_phenocam_class.return_value = mock_phenocam

            mock_satellite = Mock()
            mock_satellite.search_satellite_data.side_effect = Exception("API Error")
            mock_satellite_class.return_value = mock_satellite

            finder = DataFinder()
            results = finder.find_data_for_locations(max_locations=1)

            assert len(results) == 1
            assert "error" in results[0]


class TestSiteEvaluation:
    """Test site suitability evaluation"""

    def test_evaluate_site_suitability_good_conditions(self, mock_satellite_response):
        """Test site evaluation with good satellite conditions"""
        with patch("phenocam_finder.core.SatelliteQuery") as mock_satellite_class:
            mock_satellite = Mock()

            # Mock both satellite query methods
            mock_satellite.search_satellite_data.return_value = mock_satellite_response
            mock_satellite.search_satellite_data_daterange.return_value = (
                mock_satellite_response
            )
            mock_satellite_class.return_value = mock_satellite

            finder = DataFinder()
            location = {
                "sitename": "bavarian-forest",
                "lat": 49.0369,
                "lon": 13.4147,
                "date_first": "2020-04-01",
                "date_last": "2023-10-31",
            }

            result = finder.evaluate_site_suitability(
                lat=location["lat"],
                lon=location["lon"],
                sitename=location["sitename"],
                location=location,
            )

            assert "suitability_score" in result
            assert result["sitename"] == "bavarian-forest"
            # With good satellite data (low cloud cover, regular scenes), score should be decent
            assert result["suitability_score"] > 0.3

    def test_evaluate_site_suitability_with_api_error(self):
        """Test site evaluation with satellite query error returns unknown status"""
        with patch("phenocam_finder.core.SatelliteQuery") as mock_satellite_class:
            mock_satellite = Mock()
            mock_satellite.search_satellite_data.side_effect = Exception("API Error")
            mock_satellite.search_satellite_data_daterange.side_effect = Exception(
                "API Error"
            )
            mock_satellite_class.return_value = mock_satellite

            finder = DataFinder()
            location = {
                "sitename": "bavarian-forest",
                "lat": 49.0369,
                "lon": 13.4147,
            }

            result = finder.evaluate_site_suitability(
                lat=location["lat"],
                lon=location["lon"],
                sitename=location["sitename"],
            )

            # API error should return None/null score, not 0.0 (which implies known unsuitability)
            assert result["suitability_score"] is None
            assert "error" in result
            assert "API Error" in result["error"]

    def test_evaluate_all_sites(
        self, sample_european_locations, mock_satellite_response
    ):
        """Test evaluating all sites"""
        with patch("phenocam_finder.core.PhenoCamQuery") as mock_phenocam_class, patch(
            "phenocam_finder.core.SatelliteQuery"
        ) as mock_satellite_class:
            mock_phenocam = Mock()
            mock_phenocam.get_all_locations.return_value = sample_european_locations
            mock_phenocam_class.return_value = mock_phenocam

            mock_satellite = Mock()

            # Mock both satellite query methods
            mock_satellite.search_satellite_data.return_value = mock_satellite_response
            mock_satellite.search_satellite_data_daterange.return_value = (
                mock_satellite_response
            )
            mock_satellite_class.return_value = mock_satellite

            finder = DataFinder()
            results = finder.evaluate_all_sites(max_locations=1)

            # evaluate_all_sites returns results per growing season year
            assert len(results) >= 1
            assert "suitability_score" in results[0]
            # Results should be sorted by suitability score (best first)
            scores = [
                r["suitability_score"]
                for r in results
                if r["suitability_score"] is not None
            ]
            if len(scores) > 1:
                assert scores == sorted(scores, reverse=True)


class TestMetricsCalculation:
    """Test site metrics calculation"""

    def test_calculate_site_metrics_with_data(self):
        """Test metrics calculation with satellite data"""
        finder = DataFinder()

        s2_features = [
            {"properties": {"datetime": "2023-06-01T00:00:00Z"}},
            {"properties": {"datetime": "2023-07-01T00:00:00Z"}},
        ]
        s3_features = [
            {"properties": {"datetime": "2023-06-15T00:00:00Z"}},
        ]

        metrics = finder._calculate_site_metrics(s2_features, s3_features, 2023)

        assert "sentinel2_scenes" in metrics
        assert "sentinel3_scenes" in metrics
        assert "suitability_score" in metrics
        assert metrics["sentinel2_scenes"] == 2
        assert metrics["sentinel3_scenes"] == 1

    def test_calculate_site_metrics_empty_data(self):
        """Test metrics calculation with no data"""
        finder = DataFinder()

        metrics = finder._calculate_site_metrics([], [], 2023)

        assert metrics["sentinel2_scenes"] == 0
        assert metrics["sentinel3_scenes"] == 0
        assert metrics["suitability_score"] == 0.0

    def test_calculate_site_metrics_comprehensive(self):
        """Test comprehensive metrics calculation with logical scoring"""
        finder = DataFinder()

        # Create test data with realistic cloud cover patterns
        s2_features = [
            {
                "properties": {
                    "datetime": f"2023-{month:02d}-15T00:00:00Z",
                    "eo:cloud_cover": 20.0
                    if month in [4, 5, 9, 10]
                    else 15.0,  # Better in shoulder seasons
                }
            }
            for month in [4, 5, 6, 7, 8, 9, 10]  # Growing season
        ] + [
            {
                "properties": {
                    "datetime": f"2023-{month:02d}-15T00:00:00Z",
                    "eo:cloud_cover": 45.0,  # Higher cloud cover in winter
                }
            }
            for month in [1, 2, 3, 11, 12]  # Non-growing season
        ]

        s3_features = [
            {
                "properties": {
                    "datetime": f"2023-{month:02d}-10T00:00:00Z",
                    "eo:cloud_cover": 25.0,
                }
            }
            for month in [5, 6, 7, 8, 9]  # Some growing season overlap
        ]

        metrics = finder._calculate_site_metrics(s2_features, s3_features, 2023)

        # Test all core metrics are present and logically valid
        assert metrics["sentinel2_scenes"] == 12
        assert metrics["sentinel3_scenes"] == 5
        assert "s2_scenes_per_month" in metrics
        assert "s3_scenes_per_month" in metrics
        assert "max_s2_gap_days" in metrics
        assert "max_s3_gap_days" in metrics
        assert "cloud_cover_mean" in metrics
        assert "temporal_overlap_days" in metrics
        assert isinstance(metrics["suitability_score"], (int, float))
        assert 0.0 <= metrics["suitability_score"] <= 1.0

        # Logical assertions: good data should score reasonably well
        assert metrics["cloud_cover_mean"] < 50.0  # Should be reasonable average
        assert metrics["suitability_score"] > 0.4  # Good data = good score

    def test_poor_satellite_conditions_score_low(
        self, mock_high_cloud_satellite_response
    ):
        """Test that poor satellite conditions (high cloud) result in low scores"""
        with patch("phenocam_finder.core.SatelliteQuery") as mock_satellite_class:
            mock_satellite = Mock()
            mock_satellite.search_satellite_data_daterange.return_value = (
                mock_high_cloud_satellite_response
            )
            mock_satellite_class.return_value = mock_satellite

            finder = DataFinder()
            location = {
                "sitename": "cloudy-site",
                "lat": 49.0,
                "lon": 13.0,
                "date_first": "2023-06-01",
                "date_last": "2023-07-31",
            }

            result = finder.evaluate_site_suitability(
                lat=location["lat"],
                lon=location["lon"],
                sitename=location["sitename"],
                location=location,
            )

            # High cloud cover (>80%) should result in reduced suitability, but not terrible if good temporal coverage
            assert result["suitability_score"] < 0.5

    def test_sparse_satellite_data_scores_low(self, mock_sparse_satellite_response):
        """Test that sparse satellite data results in low scores"""
        with patch("phenocam_finder.core.SatelliteQuery") as mock_satellite_class:
            mock_satellite = Mock()
            mock_satellite.search_satellite_data_daterange.return_value = (
                mock_sparse_satellite_response
            )
            mock_satellite_class.return_value = mock_satellite

            finder = DataFinder()
            location = {
                "sitename": "sparse-site",
                "lat": 49.0,
                "lon": 13.0,
                "date_first": "2023-06-01",
                "date_last": "2023-08-31",
            }

            result = finder.evaluate_site_suitability(
                lat=location["lat"],
                lon=location["lon"],
                sitename=location["sitename"],
                location=location,
            )

            # Very few scenes (large gaps) should result in low suitability
            assert result["suitability_score"] < 0.4
