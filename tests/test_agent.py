"""Tests for the agent loop and scoring."""

import pytest

from src.agent import AgentLoop
from src.models import DiagnosisReport, DiagnosisStatus
from eval.scorer import score_root_cause, _extract_terms


class TestParseResponse:
    """Test the agent's response parsing (no LLM needed)."""

    def setup_method(self):
        self.agent = AgentLoop.__new__(AgentLoop)

    def test_parse_structured_response(self):
        content = """Based on my investigation, here is my diagnosis:

ROOT_CAUSE: Column name mismatch - pipeline uses 'total_amount' but table has 'amount'
FIX_TYPE: sql_modification
FIX_DESCRIPTION: Change column reference from total_amount to amount
FIXED_SQL:
INSERT INTO stg_sales (order_id, customer_id, amount, order_date)
SELECT order_id, customer_id, total_amount AS amount, order_date
FROM source_data
VERIFICATION_QUERY:
SELECT COUNT(*) FROM stg_sales WHERE amount IS NOT NULL"""

        report = self.agent._parse_diagnosis(content)
        assert report.status == DiagnosisStatus.SUCCESS
        assert "total_amount" in report.root_cause
        assert "amount" in report.root_cause
        assert "INSERT INTO" in report.fixed_sql
        assert "COUNT" in report.verification_query

    def test_parse_unstructured_response(self):
        content = "The issue is that the column names don't match."
        report = self.agent._parse_diagnosis(content)
        assert report.status == DiagnosisStatus.SUCCESS
        assert report.raw_response == content

    def test_extract_field(self):
        content = "ROOT_CAUSE: Missing column region\nFIX_TYPE: sql_modification"
        assert AgentLoop._extract_field(content, "ROOT_CAUSE") == "Missing column region"
        assert AgentLoop._extract_field(content, "FIX_TYPE") == "sql_modification"
        assert AgentLoop._extract_field(content, "MISSING_FIELD") == ""


class TestScoring:
    def test_root_cause_match_exact(self):
        pred = "Column name mismatch: total_amount vs amount"
        expected = "Column name mismatch: pipeline references total_amount but table has amount"
        assert score_root_cause(pred, expected) is True

    def test_root_cause_match_partial(self):
        pred = "Missing column region in source data"
        expected = "Missing column 'region' in source CSV data"
        assert score_root_cause(pred, expected) is True

    def test_root_cause_no_match(self):
        pred = "Database connection timeout"
        expected = "Column name mismatch"
        assert score_root_cause(pred, expected) is False

    def test_root_cause_empty(self):
        assert score_root_cause("", "something") is False
        assert score_root_cause("something", "") is False

    def test_extract_terms(self):
        terms = _extract_terms("Column name mismatch: 'total_amount' vs 'amount'")
        assert "column" in terms
        assert "total_amount" in terms
        assert "amount" in terms
        assert "the" not in terms
