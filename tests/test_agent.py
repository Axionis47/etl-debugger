"""Tests for the agent loop and scoring."""

import pytest

from src.agent import AgentLoop, _strip_code_fences
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


class TestParseEdgeCases:
    """Test hardened parsing against common LLM output variations."""

    def setup_method(self):
        self.agent = AgentLoop.__new__(AgentLoop)

    def test_markdown_root_cause_format(self):
        """Models sometimes use **Root Cause**: instead of ROOT_CAUSE:."""
        content = """**Root Cause**: Column 'total_amount' is not in destination table stg_sales.

**Fix**:
```sql
INSERT INTO stg_sales (order_id, customer_id, amount, order_date)
SELECT order_id, customer_id, total_amount AS amount, order_date
FROM source_data
```"""
        report = self.agent._parse_diagnosis(content)
        assert "total_amount" in report.root_cause
        assert "INSERT INTO" in report.fixed_sql

    def test_root_cause_prose_format(self):
        """Models sometimes say 'The root cause is: ...'."""
        content = """The root cause is: join key format mismatch between orders and customers.

FIXED_SQL:
INSERT INTO enriched_orders (order_id, customer_name, order_total)
SELECT o.order_id, c.name, o.total
FROM orders o
JOIN customers c ON o.customer_id = CAST(REPLACE(c.customer_id, 'CUST-', '') AS INTEGER)
VERIFICATION_QUERY:
SELECT COUNT(*) FROM enriched_orders"""
        report = self.agent._parse_diagnosis(content)
        assert "join" in report.root_cause.lower()
        assert "INSERT INTO" in report.fixed_sql

    def test_sql_with_tilde_fences(self):
        """Handle ~~~ code fences instead of ```."""
        content = """ROOT_CAUSE: Type cast error on price column
FIXED_SQL:
~~~sql
INSERT INTO fact_pricing (product_id, price, effective_date)
SELECT product_id,
       CASE WHEN price = 'N/A' THEN NULL ELSE CAST(REPLACE(price, '$', '') AS DOUBLE) END,
       effective_date
FROM source_data
~~~
VERIFICATION_QUERY:
~~~sql
SELECT COUNT(*) FROM fact_pricing
~~~"""
        report = self.agent._parse_diagnosis(content)
        assert "INSERT INTO" in report.fixed_sql
        assert "N/A" in report.fixed_sql
        assert "COUNT" in report.verification_query

    def test_sql_fallback_from_code_block(self):
        """When FIXED_SQL label is missing, extract INSERT INTO from code blocks."""
        content = """The fix is to alias the column:

```sql
INSERT INTO stg_sales (order_id, customer_id, amount, order_date)
SELECT order_id, customer_id, total_amount AS amount, order_date
FROM source_data
```

Verify with:
```sql
SELECT COUNT(*) FROM stg_sales
```"""
        report = self.agent._parse_diagnosis(content)
        assert "INSERT INTO" in report.fixed_sql
        assert "amount" in report.fixed_sql

    def test_mixed_case_labels(self):
        """Labels with mixed casing should still parse."""
        content = """root_cause: Column mismatch between source and dest
fix_type: sql_modification
Fixed_SQL:
INSERT INTO stg_sales (order_id, amount) SELECT order_id, total_amount AS amount FROM source_data
Verification_Query:
SELECT COUNT(*) FROM stg_sales"""
        report = self.agent._parse_diagnosis(content)
        assert "mismatch" in report.root_cause.lower()
        assert "INSERT INTO" in report.fixed_sql

    def test_strip_code_fences_backtick(self):
        """Standard ``` fences are stripped."""
        sql = "```sql\nSELECT 1\n```"
        assert _strip_code_fences(sql) == "SELECT 1"

    def test_strip_code_fences_tilde(self):
        """~~~ fences are stripped."""
        sql = "~~~sql\nSELECT 1\n~~~"
        assert _strip_code_fences(sql) == "SELECT 1"

    def test_strip_code_fences_uppercase(self):
        """```SQL (uppercase) fences are stripped."""
        sql = "```SQL\nSELECT 1\n```"
        assert _strip_code_fences(sql) == "SELECT 1"

    def test_strip_code_fences_no_language(self):
        """Bare ``` fences are stripped."""
        sql = "```\nSELECT 1\n```"
        assert _strip_code_fences(sql) == "SELECT 1"

    def test_extract_field_flexible_multiple(self):
        """_extract_field_flexible tries multiple patterns."""
        content = "**Root Cause**: Missing column in source\nSome other text"
        result = AgentLoop._extract_field_flexible(content, [
            r"\*\*Root\s*Cause\*\*",
            r"Root\s*Cause",
        ])
        assert "Missing column" in result


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
