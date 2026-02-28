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
        """Build the initial user prompt with pipeline context."""
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

        parts.extend([
            "",
            f"The database engine is {pipeline.destination.engine}. "
            f"Use engine='{pipeline.destination.engine}' when calling tools.",
            "",
            "Please diagnose the root cause and propose a fix.",
        ])

        return "\n".join(parts)

    def _parse_diagnosis(self, content: str) -> DiagnosisReport:
        """Parse the LLM's final response into a structured DiagnosisReport."""
        root_cause = self._extract_field(content, "ROOT_CAUSE")
        fix_type = self._extract_field(content, "FIX_TYPE") or "sql_modification"
        fix_description = self._extract_field(content, "FIX_DESCRIPTION")
        fixed_sql = self._extract_sql_block(content, "FIXED_SQL")
        verification_query = self._extract_sql_block(content, "VERIFICATION_QUERY")

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
        pattern = rf"{field_name}:.*?```sql?\n(.*?)```"
        match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()

        return ""


def _strip_code_fences(sql: str) -> str:
    """Remove all markdown code fence artifacts from SQL."""
    # Remove opening fences like ```sql, ```
    sql = re.sub(r"^```\w*\s*\n?", "", sql)
    # Remove closing fences
    sql = re.sub(r"\n?```\s*$", "", sql)
    # Remove any remaining standalone ``` lines
    lines = [line for line in sql.split("\n") if line.strip() != "```"]
    return "\n".join(lines).strip()
