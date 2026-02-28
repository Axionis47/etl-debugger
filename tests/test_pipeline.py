"""Tests for pipeline config loading."""

from pathlib import Path

import pytest

from src.pipeline import load_pipeline, load_error_log, setup_test_db


GOLDEN_DIR = Path(__file__).parent.parent / "golden_set"


class TestLoadPipeline:
    def test_load_case_01(self):
        config = load_pipeline(GOLDEN_DIR / "case_01_schema_mismatch" / "pipeline.yaml")
        assert config.name == "daily_sales_load"
        assert config.source.type == "csv"
        assert config.destination.engine == "duckdb"
        assert config.destination.table == "stg_sales"
        assert "INSERT INTO" in config.transform.sql

    def test_load_case_02(self):
        config = load_pipeline(GOLDEN_DIR / "case_02_missing_column" / "pipeline.yaml")
        assert config.name == "customer_enrichment"
        assert config.destination.engine == "sqlite"

    def test_load_case_04_multi_table(self):
        config = load_pipeline(GOLDEN_DIR / "case_04_join_key_mismatch" / "pipeline.yaml")
        assert config.source.type == "multi_table"
        assert config.source.tables == ["orders", "customers"]


class TestLoadErrorLog:
    def test_load_existing_log(self):
        log = load_error_log(GOLDEN_DIR / "case_01_schema_mismatch" / "pipeline.yaml")
        assert "ERROR" in log
        assert "total_amount" in log

    def test_load_missing_log(self, tmp_path):
        log = load_error_log(tmp_path / "pipeline.yaml")
        assert log == ""


class TestSetupTestDb:
    def test_setup_duckdb(self, tmp_path):
        config = load_pipeline(GOLDEN_DIR / "case_01_schema_mismatch" / "pipeline.yaml")
        db_path = setup_test_db(config, GOLDEN_DIR / "case_01_schema_mismatch")
        assert Path(db_path).exists()
        # Cleanup
        Path(db_path).unlink(missing_ok=True)

    def test_setup_sqlite(self, tmp_path):
        config = load_pipeline(GOLDEN_DIR / "case_02_missing_column" / "pipeline.yaml")
        db_path = setup_test_db(config, GOLDEN_DIR / "case_02_missing_column")
        assert Path(db_path).exists()
        # Cleanup
        Path(db_path).unlink(missing_ok=True)
