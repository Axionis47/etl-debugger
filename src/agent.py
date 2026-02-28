from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel

from src.llm import OllamaClient, SYSTEM_PROMPT, TOOL_FUNCTIONS
from src.models import DiagnosisReport, DiagnosisStatus, PipelineConfig
from src.tools import execute_tool
from src.tools.schema_inspector import compare_schemas, sample_values
from src.tools.sql_executor import get_db_path

console = Console()


class AgentLoop:
    """ReAct-style agent loop for ETL pipeline debugging.

    The agent iterates through reason-act-observe cycles:
    1. Reason: LLM analyzes context and decides next action
    2. Act: Execute a tool call or provide final diagnosis
    3. Observe: Append tool result and loop back

    Terminates when the LLM provides a final answer (no tool calls)
    or max_steps is reached.
    """

    def __init__(
        self,
        llm: OllamaClient,
        max_steps: int = 10,
        verbose: bool = False,
    ):
        self.llm = llm
        self.max_steps = max_steps
        self.verbose = verbose
        self.history: list[dict[str, Any]] = []
        self.steps_taken: int = 0

    def run(
        self,
        pipeline: PipelineConfig,
        error_log: str,
        case_dir: str | None = None,
    ) -> DiagnosisReport:
        """Run the agent loop to diagnose a pipeline failure.

        Args:
            pipeline: The pipeline configuration.
            error_log: Contents of the error log.
            case_dir: Directory containing the test case files.

        Returns:
            A DiagnosisReport with the agent's findings.
        """
        self.steps_taken = 0
        self.history = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": self._build_user_prompt(pipeline, error_log, case_dir)},
        ]

        start_time = time.time()

        for step in range(1, self.max_steps + 1):
            self.steps_taken = step

            if self.verbose:
                console.print(f"\n[bold cyan]--- Step {step}/{self.max_steps} ---[/bold cyan]")

            try:
                response = self.llm.chat(
                    messages=self.history,
                    tools=TOOL_FUNCTIONS,
                )
            except Exception as e:
                if self.verbose:
                    console.print(f"  [red]LLM Error: {e}[/red]")
                return DiagnosisReport(
                    status=DiagnosisStatus.ERROR,
                    steps_taken=self.steps_taken,
                    raw_response=f"LLM Error: {e}",
                )

            message = response.message

            # Append the assistant message to history
            # Note: we don't store tool_calls in history to avoid SDK validation
            # issues with mock ToolCall objects. The conversation flow is maintained
            # through the content and tool result messages.
            self.history.append({
                "role": "assistant",
                "content": message.content or "",
            })

            if self.verbose and message.content:
                console.print(Panel(message.content, title="Agent Reasoning", border_style="blue"))

            # If the LLM wants to call tools
            if message.tool_calls:
                for tool_call in message.tool_calls:
                    tool_name = tool_call.function.name
                    tool_args = tool_call.function.arguments

                    if self.verbose:
                        console.print(f"  [yellow]Tool:[/yellow] {tool_name}({tool_args})")

                    result = execute_tool(tool_name, tool_args)

                    if self.verbose:
                        # Truncate long results for display
                        display = result[:500] + "..." if len(result) > 500 else result
                        console.print(f"  [green]Result:[/green] {display}")

                    self.history.append({
                        "role": "user",
                        "content": f"Tool '{tool_name}' returned:\n{result}",
                    })
                continue

            # No tool calls — this is the final answer
            elapsed = time.time() - start_time
            report = self._parse_diagnosis(message.content or "")
            report.steps_taken = self.steps_taken
            return report

        # Max steps reached without a final answer
        elapsed = time.time() - start_time
        return DiagnosisReport(
            status=DiagnosisStatus.MAX_STEPS_REACHED,
            steps_taken=self.steps_taken,
            raw_response="Agent reached maximum steps without providing a diagnosis.",
        )

    def _build_user_prompt(
        self,
        pipeline: PipelineConfig,
        error_log: str,
        case_dir: str | None,
    ) -> str:
        """Build the initial user prompt with pipeline context.

        Pre-computes diagnostic context (schema comparisons, join key samples)
        and injects it directly into the prompt. This is critical because small
        models (7B) often complete in a single step without calling tools.
        """
        parts = [
            "I have a broken ETL pipeline that needs debugging.",
            "",
            "## Pipeline Configuration",
            f"Name: {pipeline.name}",
            f"Source type: {pipeline.source.type}",
            f"Destination engine: {pipeline.destination.engine}",
            f"Destination table: {pipeline.destination.table}",
            "",
            "Transform SQL:",
            "```sql",
            pipeline.transform.sql.strip(),
            "```",
            "",
            "## Error Log",
            "```",
            error_log.strip(),
            "```",
        ]

        if case_dir:
            # List available files the agent can read
            case_path = Path(case_dir)
            files = [f.name for f in case_path.iterdir() if f.is_file()]
            parts.extend([
                "",
                "## Available Files",
                f"Directory: {case_dir}",
                f"Files: {', '.join(files)}",
            ])

        # Pre-compute diagnostic context if database is available
        engine = pipeline.destination.engine
        diagnostics = self._precompute_diagnostics(pipeline, error_log, engine)
        if diagnostics:
            parts.extend(["", "## Pre-computed Diagnostics", diagnostics])

        parts.extend([
            "",
            f"The database engine is {engine}. "
            f"Use engine='{engine}' when calling tools.",
            "",
            "Please diagnose the root cause and propose a fix.",
        ])

        return "\n".join(parts)

    def _precompute_diagnostics(
        self,
        pipeline: PipelineConfig,
        error_log: str,
        engine: str,
    ) -> str:
        """Pre-compute schema comparisons and join diagnostics.

        Returns diagnostic context as a formatted string, or empty string
        if no database is configured (e.g. during unit tests).
        """
        db_path = get_db_path(engine)
        if not db_path:
            return ""

        parts: list[str] = []
        dest_table = pipeline.destination.table
        transform_sql = pipeline.transform.sql

        # Determine source tables from pipeline config
        source_tables = self._get_source_tables(pipeline)

        # Schema comparison for each source table vs destination
        for src_table in source_tables:
            try:
                comparison = compare_schemas(src_table, dest_table, engine)
                parts.append(f"### Schema: {src_table} vs {dest_table}")
                parts.append(comparison)
                parts.append("")
            except Exception:
                pass

        # Zero-row join diagnostic: when error mentions "0 rows" and SQL has JOIN
        error_lower = error_log.lower()
        sql_upper = transform_sql.upper()
        if ("0 rows" in error_lower or "0 row" in error_lower) and "JOIN" in sql_upper:
            parts.append("### WARNING: Join produced 0 rows")
            parts.append(
                "The transform SQL uses a JOIN but produced 0 rows. "
                "This usually means join key VALUES don't match between tables "
                "(e.g., integer 101 vs string 'CUST-101'). "
                "Compare the actual key values below:"
            )
            # Try to extract join key columns and sample them
            join_samples = self._sample_join_keys(transform_sql, engine)
            if join_samples:
                parts.append("")
                parts.append(join_samples)
            parts.append("")

        return "\n".join(parts) if parts else ""

    @staticmethod
    def _get_source_tables(pipeline: PipelineConfig) -> list[str]:
        """Extract source table names from pipeline config."""
        if pipeline.source.tables:
            return pipeline.source.tables
        # Single-source: infer table name from source path
        if pipeline.source.path:
            return [Path(pipeline.source.path).stem]
        return []

    @staticmethod
    def _sample_join_keys(transform_sql: str, engine: str) -> str:
        """Extract join key columns from SQL and sample their values.

        Parses simple JOIN ... ON conditions and calls sample_values
        for each side of the join to reveal format mismatches.
        """
        # Match patterns like: JOIN <table> <alias> ON <expr> = <expr>
        join_pattern = re.compile(
            r'JOIN\s+(\w+)\s+(\w+)\s+ON\s+'
            r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)',
            re.IGNORECASE,
        )
        match = join_pattern.search(transform_sql)
        if not match:
            return ""

        parts: list[str] = []
        # Extract both sides of the join condition
        left_alias, left_col = match.group(3), match.group(4)
        right_alias, right_col = match.group(5), match.group(6)

        # Map aliases to table names from the SQL
        from_pattern = re.compile(r'FROM\s+(\w+)\s+(\w+)', re.IGNORECASE)
        from_match = from_pattern.search(transform_sql)

        alias_map: dict[str, str] = {}
        if from_match:
            alias_map[from_match.group(2)] = from_match.group(1)
        alias_map[match.group(2)] = match.group(1)

        # Sample values for each side of the join
        for alias, col in [(left_alias, left_col), (right_alias, right_col)]:
            table = alias_map.get(alias, alias)
            try:
                result = sample_values(table, col, engine)
                parts.append(result)
            except Exception:
                pass

        return "\n".join(parts) if parts else ""

    def _parse_diagnosis(self, content: str) -> DiagnosisReport:
        """Parse the LLM's final response into a structured DiagnosisReport.

        Uses multiple fallback strategies to extract fields from varying
        LLM output formats (structured labels, markdown headers, prose).
        """
        root_cause = self._extract_field(content, "ROOT_CAUSE")
        fix_type = self._extract_field(content, "FIX_TYPE") or "sql_modification"
        fix_description = self._extract_field(content, "FIX_DESCRIPTION")
        fixed_sql = self._extract_sql_block(content, "FIXED_SQL")
        verification_query = self._extract_sql_block(content, "VERIFICATION_QUERY")

        # Fallback: try alternative root cause labels
        if not root_cause:
            root_cause = self._extract_field_flexible(content, [
                r"\*\*Root\s*Cause\*\*",
                r"Root\s*Cause",
                r"The\s+root\s+cause\s+is",
                r"Issue",
                r"Problem",
            ])

        # Fallback: extract SQL from content if FIXED_SQL label wasn't found
        if not fixed_sql:
            fixed_sql = self._extract_sql_fallback(content)

        # Fallback: extract verification query from any trailing SELECT
        if not verification_query and fixed_sql:
            verification_query = self._extract_verification_fallback(content, fixed_sql)

        # If structured parsing didn't work, use the raw content
        if not root_cause and not fixed_sql:
            return DiagnosisReport(
                status=DiagnosisStatus.SUCCESS,
                raw_response=content,
                root_cause=content[:200],
            )

        return DiagnosisReport(
            status=DiagnosisStatus.SUCCESS,
            root_cause=root_cause,
            fix_type=fix_type,
            fix_description=fix_description,
            fixed_sql=fixed_sql,
            verification_query=verification_query,
            raw_response=content,
        )

    @staticmethod
    def _extract_field(content: str, field_name: str) -> str:
        """Extract a single-line field value like 'ROOT_CAUSE: ...'."""
        pattern = rf"{field_name}:\s*(.+?)(?:\n|$)"
        match = re.search(pattern, content, re.IGNORECASE)
        return match.group(1).strip() if match else ""

    @staticmethod
    def _extract_field_flexible(content: str, patterns: list[str]) -> str:
        """Try multiple label patterns to extract a field value.

        Handles variations like **Root Cause**: ..., Root Cause: ...,
        The root cause is: ..., etc.
        """
        for pat in patterns:
            match = re.search(rf"{pat}\s*[:]\s*(.+?)(?:\n|$)", content, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                # Strip trailing markdown artifacts
                value = value.rstrip("*").strip()
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_sql_block(content: str, field_name: str) -> str:
        """Extract a multi-line SQL block after a field label."""
        # Try to find content between FIELD_NAME: and the next field or end
        pattern = rf"{field_name}:\s*\n(.*?)(?=\n[A-Z_]+:|$)"
        match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
        if match:
            sql = match.group(1).strip()
            sql = _strip_code_fences(sql)
            return sql

        # Fallback: look for ```sql blocks after the field name
        pattern = rf"{field_name}:.*?```(?:sql|SQL)?\s*\n(.*?)```"
        match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Fallback: look for ~~~sql blocks
        pattern = rf"{field_name}:.*?~~~(?:sql|SQL)?\s*\n(.*?)~~~"
        match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()

        return ""

    @staticmethod
    def _extract_sql_fallback(content: str) -> str:
        """Extract SQL when FIXED_SQL label is missing.

        Looks for INSERT INTO or corrected SELECT statements in code blocks
        or raw content.
        """
        # Try code-fenced SQL blocks containing INSERT INTO
        for fence in ["```", "~~~"]:
            pattern = rf"{fence}(?:sql|SQL)?\s*\n(.*?){fence}"
            matches = re.findall(pattern, content, re.DOTALL)
            for block in matches:
                block = block.strip()
                if re.search(r'\bINSERT\s+INTO\b', block, re.IGNORECASE):
                    return block

        # Try unfenced INSERT INTO ... SELECT blocks
        match = re.search(
            r'(INSERT\s+INTO\s+\w+\s*\([^)]+\)\s*\n\s*SELECT\s+.+?)(?=\n\n|\nROOT_CAUSE|\nFIX_|\nVERIFICATION|\Z)',
            content,
            re.DOTALL | re.IGNORECASE,
        )
        if match:
            return _strip_code_fences(match.group(1).strip())

        return ""

    @staticmethod
    def _extract_verification_fallback(content: str, fixed_sql: str) -> str:
        """Extract verification query when VERIFICATION_QUERY label is missing.

        Looks for a SELECT COUNT(*) or similar query after the fixed SQL.
        """
        # Find position after the fixed SQL in content
        sql_pos = content.find(fixed_sql)
        if sql_pos == -1:
            # Try first line of fixed SQL
            first_line = fixed_sql.split("\n")[0]
            sql_pos = content.find(first_line)

        if sql_pos == -1:
            return ""

        after = content[sql_pos + len(fixed_sql.split("\n")[0]):]

        # Look for SELECT COUNT or SELECT * as verification
        match = re.search(
            r'(SELECT\s+COUNT\s*\(.*?\)\s+FROM\s+\w+.*?)(?:\n\n|\Z)',
            after,
            re.DOTALL | re.IGNORECASE,
        )
        if match:
            return _strip_code_fences(match.group(1).strip())

        return ""


def _strip_code_fences(sql: str) -> str:
    """Remove all markdown code fence artifacts from SQL."""
    # Remove opening fences like ```sql, ```SQL, ~~~sql, ~~~
    sql = re.sub(r"^(?:```|~~~)\w*\s*\n?", "", sql)
    # Remove closing fences
    sql = re.sub(r"\n?(?:```|~~~)\s*$", "", sql)
    # Remove any remaining standalone ``` or ~~~ lines
    lines = [line for line in sql.split("\n") if line.strip() not in ("```", "~~~")]
    return "\n".join(lines).strip()
