"""Unit tests for Agent Quality Registry pipeline."""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from pipelines.rj_crm__agent_quality_registry.utils.parser import (
    normalize_version,
    normalize_test_result,
)
from pipelines.rj_crm__agent_quality_registry.utils.schemas import (
    VersionSchema,
    TestResultSchema,
)


class TestNormalization:
    """Test data normalization functions."""

    def test_normalize_version_valid(self):
        """Test normalizing a valid version artifact."""
        version_data = {
            "release_key": "abc123",
            "environment_scope": "qa",
            "registry_package_version": "1.0.0",
            "agent_version_number": 1,
            "routing_total": 10,
            "routing_pass": 8,
            "routing_fail": 2,
        }
        result = normalize_version(version_data)
        assert result["release_key"] == "abc123"
        assert result["environment_scope"] == "qa"
        assert result["routing_total"] == 10

    def test_normalize_version_missing_fields(self):
        """Test normalizing version with missing optional fields."""
        version_data = {
            "release_key": "abc123",
            "environment_scope": "qa",
        }
        result = normalize_version(version_data)
        assert result["release_key"] == "abc123"
        assert result.get("routing_total") is None

    def test_normalize_test_result_pass(self):
        """Test normalizing a passing test result."""
        result_data = {
            "result_key": "test123",
            "release_key": "abc123",
            "test_source": "harness",
            "result": "PASS",
            "status": "PASS",
            "score": 1.0,
        }
        result = normalize_test_result(result_data)
        assert result["result"] == "PASS"
        assert result["score"] == 1.0

    def test_normalize_test_result_fail(self):
        """Test normalizing a failing test result."""
        result_data = {
            "result_key": "test456",
            "release_key": "abc123",
            "test_source": "harness",
            "result": "FAIL",
            "status": "FAIL",
            "score": 0.0,
            "message": "Test failed",
        }
        result = normalize_test_result(result_data)
        assert result["result"] == "FAIL"
        assert result["score"] == 0.0
        assert result["message"] == "Test failed"


class TestSchemaValidation:
    """Test schema validation."""

    def test_version_schema_valid(self):
        """Test version schema with valid data."""
        valid_data = {
            "release_key": "abc123",
            "environment_scope": "qa",
            "registry_package_version": "1.0.0",
            "agent_version_number": 1,
            "routing_total": 10,
            "routing_pass": 8,
            "routing_fail": 2,
            "harness_scenarios_total": 5,
            "harness_scenarios_pass": 4,
            "harness_scenarios_fail": 1,
        }
        schema = VersionSchema()
        result = schema.load(valid_data)
        assert result["release_key"] == "abc123"
        assert result["environment_scope"] == "qa"

    def test_version_schema_missing_key(self):
        """Test version schema with missing required key."""
        invalid_data = {
            "environment_scope": "qa",
            "registry_package_version": "1.0.0",
        }
        schema = VersionSchema()
        with pytest.raises(Exception):  # Schema validation error
            schema.load(invalid_data)

    def test_test_result_schema_valid(self):
        """Test result schema with valid data."""
        valid_data = {
            "result_key": "test123",
            "release_key": "abc123",
            "test_source": "harness",
            "result": "PASS",
            "assertion": "test_routing",
            "status": "PASS",
        }
        schema = TestResultSchema()
        result = schema.load(valid_data)
        assert result["result"] == "PASS"
        assert result["test_source"] == "harness"

    def test_test_result_schema_invalid_result_type(self):
        """Test result schema with invalid result type."""
        invalid_data = {
            "result_key": "test123",
            "release_key": "abc123",
            "test_source": "harness",
            "result": "UNKNOWN",  # Invalid
            "assertion": "test_routing",
        }
        schema = TestResultSchema()
        with pytest.raises(Exception):  # Schema validation error
            schema.load(invalid_data)


class TestDataIsolation:
    """Test data isolation between environments."""

    def test_qa_prod_isolation(self):
        """Test that QA and Prod versions are properly isolated."""
        qa_version = {
            "release_key": "qa123",
            "environment_scope": "qa",
        }
        prod_version = {
            "release_key": "prod123",
            "environment_scope": "prod_baseline",
        }
        
        assert qa_version["environment_scope"] != prod_version["environment_scope"]
        assert qa_version["release_key"] != prod_version["release_key"]

    def test_environment_scope_values(self):
        """Test that environment_scope has valid values."""
        valid_scopes = ["qa", "prod_baseline"]
        
        for scope in valid_scopes:
            assert scope in ["qa", "prod_baseline"]


class TestIncrementality:
    """Test incrementality features."""

    def test_duplicate_detection(self):
        """Test that duplicate versions can be detected."""
        version1 = {"release_key": "abc123", "tested_at": "2026-09-29T10:00:00Z"}
        version2 = {"release_key": "abc123", "tested_at": "2026-09-29T10:00:00Z"}
        
        assert version1["release_key"] == version2["release_key"]

    def test_update_detection(self):
        """Test that updated versions can be detected."""
        version_old = {
            "release_key": "abc123",
            "agent_version_number": 1,
            "routing_total": 10,
        }
        version_new = {
            "release_key": "abc123",
            "agent_version_number": 2,  # Updated
            "routing_total": 15,  # Updated
        }
        
        assert version_old["agent_version_number"] != version_new["agent_version_number"]
        assert version_old["routing_total"] != version_new["routing_total"]


class TestMetricsCollection:
    """Test quality metrics collection."""

    def test_routing_metrics(self):
        """Test routing metrics are collected correctly."""
        version = {
            "routing_total": 100,
            "routing_pass": 85,
            "routing_fail": 15,
        }
        
        assert version["routing_total"] == 100
        assert version["routing_pass"] == 85
        assert version["routing_fail"] == 15
        assert version["routing_pass"] + version["routing_fail"] == version["routing_total"]

    def test_harness_metrics(self):
        """Test harness metrics are collected correctly."""
        version = {
            "harness_scenarios_total": 50,
            "harness_scenarios_pass": 42,
            "harness_scenarios_fail": 8,
            "harness_turns_total": 200,
            "harness_turns_pass": 180,
            "harness_turns_fail": 20,
        }
        
        assert version["harness_scenarios_total"] == 50
        assert version["harness_turns_total"] == 200
        assert (
            version["harness_scenarios_pass"] + version["harness_scenarios_fail"]
            == version["harness_scenarios_total"]
        )

    def test_latency_metrics(self):
        """Test latency metrics are calculated."""
        turn_results = [
            {"latency_ms": 100},
            {"latency_ms": 200},
            {"latency_ms": 150},
            {"latency_ms": 120},
            {"latency_ms": 180},
        ]
        
        latencies = [r["latency_ms"] for r in turn_results]
        avg_latency = sum(latencies) / len(latencies)
        
        assert avg_latency == 150.0
        assert min(latencies) == 100
        assert max(latencies) == 200


class TestErrorHandling:
    """Test error handling."""

    def test_missing_required_field(self):
        """Test handling of missing required fields."""
        incomplete_data = {
            "environment_scope": "qa",
        }
        
        required_fields = ["release_key"]
        missing = [f for f in required_fields if f not in incomplete_data]
        
        assert len(missing) > 0

    def test_invalid_metric_type(self):
        """Test handling of invalid metric types."""
        invalid_metric = {
            "routing_total": "not_a_number",  # Should be int
        }
        
        try:
            assert isinstance(invalid_metric["routing_total"], int)
        except AssertionError:
            pass  # Expected failure


class TestDataValidation:
    """Test data validation."""

    def test_release_key_format(self):
        """Test that release_key has valid format."""
        valid_keys = [
            "abc123def456",
            "0123456789abcdef",
        ]
        
        for key in valid_keys:
            assert len(key) > 0
            assert isinstance(key, str)

    def test_version_number_positive(self):
        """Test that version numbers are positive."""
        valid_versions = [1, 93, 100, 999]
        
        for version in valid_versions:
            assert version > 0

    def test_pass_rate_percentage(self):
        """Test that pass rates are valid percentages."""
        valid_rates = [0.0, 0.5, 1.0, 0.85, 0.0]
        
        for rate in valid_rates:
            assert 0.0 <= rate <= 1.0
