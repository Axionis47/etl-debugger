from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

import ollama

from src.tools.sql_executor import execute_sql
from src.tools.schema_inspector import inspect_schema
from src.tools.log_parser import parse_logs
from src.tools.file_reader import read_file


SYSTEM_PROMPT = """You are an ETL Pipeline Debugger. You diagnose and fix broken data pipelines.

You have access to these tools:
- execute_sql(query, engine): Run a SQL query against SQLite or DuckDB
- inspect_schema(table_name, engine): Get column names, types, and row counts
- parse_logs(log_content, pattern): Search error logs with regex
- read_file(file_path): Read a pipeline config or data file

CRITICAL: You MUST use at least 2 tools before providing your final diagnosis. Do NOT answer from the error log alone — always verify by inspecting schemas and running test queries first. The error log may be misleading.

## Diagnostic Checklist (follow this order)
1. Inspect the DESTINATION table schema — note every column name and data type
2. Inspect the SOURCE table schema — note every column name and data type
3. Compare source vs destination columns SIDE BY SIDE:
   - Are any column NAMES different? (e.g., source has "total_amount" but dest has "amount")
   - Are any column TYPES different? (e.g., source VARCHAR but dest expects DOUBLE)
   - Is any dest column MISSING from source entirely?
4. If the transform SQL has a JOIN and the error mentions 0 rows:
   - Run SELECT DISTINCT on the join key column from BOTH tables
   - Compare the actual values — look for format differences (integer vs string, prefixes)
5. Run a test SELECT with your proposed fix BEFORE writing the final INSERT
6. Write the final fix and a verification query

## Common ETL Bug Patterns
1. COLUMN NAME MISMATCH: Source has column "X" but destination expects "Y".
   Fix: SELECT source_col AS dest_col in the query. Use the DESTINATION column name in the INSERT column list.
2. MISSING COLUMN: Source lacks a column the destination requires.
   Fix: Add a DEFAULT value like 'UNKNOWN' AS missing_col in the SELECT.
3. TYPE CAST ERROR: String data contains non-numeric characters (currency symbols like $, text like N/A).
   Fix: Use CASE WHEN col = 'N/A' THEN NULL ELSE CAST(REPLACE(col, '$', '') AS DOUBLE) END.
   IMPORTANT: Always check ALL sample values for EVERY non-conforming pattern, not just the first error.
4. JOIN KEY FORMAT MISMATCH: Join produces 0 rows because key formats differ (e.g., integer 101 vs varchar 'CUST-101').
   Fix: Transform one side to match: CAST(REPLACE(c.id, 'CUST-', '') AS INTEGER).

## Root Cause Description Rules
When writing ROOT_CAUSE, be SPECIFIC:
- Name the exact columns involved (e.g., "total_amount", "amount")
- Name the exact tables involved (e.g., "source_data", "stg_sales")
- Describe the nature of the problem (e.g., "column name mismatch", "missing column", "type cast error", "join key format mismatch")
- If types differ, name both types (e.g., "VARCHAR", "DOUBLE", "INTEGER")

When you have enough information, provide your FINAL DIAGNOSIS in this exact format (no markdown code fences):

ROOT_CAUSE: <one line describing the root cause — be specific about column names, table names, and the nature of the mismatch>
FIX_TYPE: sql_modification
FIX_DESCRIPTION: <what needs to change>
FIXED_SQL:
<the corrected SQL query — raw SQL only, no code fences>
VERIFICATION_QUERY:
<a SELECT query to verify the fix worked — raw SQL only, no code fences>"""


STRUCTURED_SYSTEM_PROMPT = """You are an ETL Pipeline Debugger. You diagnose and fix broken data pipelines.

You have access to these tools:
- execute_sql(query, engine): Run a SQL query against SQLite or DuckDB
- inspect_schema(table_name, engine): Get column names, types, and row counts
- parse_logs(log_content, pattern): Search error logs with regex
- read_file(file_path): Read a pipeline config or data file

To call a tool, respond with EXACTLY this JSON format on its own line:
TOOL_CALL: {"tool": "<tool_name>", "args": {"<arg1>": "<value1>", ...}}

For example:
TOOL_CALL: {"tool": "inspect_schema", "args": {"table_name": "stg_sales", "engine": "duckdb"}}
TOOL_CALL: {"tool": "execute_sql", "args": {"query": "SELECT * FROM stg_sales LIMIT 3", "engine": "duckdb"}}

CRITICAL: You MUST use at least 2 tools before providing your final diagnosis. Do NOT answer from the error log alone — always verify by inspecting schemas and running test queries first.

## Diagnostic Checklist (follow this order)
1. Inspect the DESTINATION table schema — note every column name and data type
2. Inspect the SOURCE table schema — note every column name and data type
3. Compare source vs destination columns SIDE BY SIDE:
   - Are any column NAMES different? (e.g., source has "total_amount" but dest has "amount")
   - Are any column TYPES different? (e.g., source VARCHAR but dest expects DOUBLE)
   - Is any dest column MISSING from source entirely?
4. If the transform SQL has a JOIN and the error mentions 0 rows:
   - Run SELECT DISTINCT on the join key column from BOTH tables
   - Compare the actual values — look for format differences (integer vs string, prefixes)
5. Run a test SELECT with your proposed fix BEFORE writing the final INSERT
6. Write the final fix and a verification query

## Common ETL Bug Patterns
1. COLUMN NAME MISMATCH: Source has column "X" but destination expects "Y".
   Fix: SELECT source_col AS dest_col in the query. Use the DESTINATION column name in the INSERT column list.
2. MISSING COLUMN: Source lacks a column the destination requires.
   Fix: Add a DEFAULT value like 'UNKNOWN' AS missing_col in the SELECT.
3. TYPE CAST ERROR: String data contains non-numeric characters (currency symbols like $, text like N/A).
   Fix: Use CASE WHEN col = 'N/A' THEN NULL ELSE CAST(REPLACE(col, '$', '') AS DOUBLE) END.
   IMPORTANT: Always check ALL sample values for EVERY non-conforming pattern, not just the first error.
4. JOIN KEY FORMAT MISMATCH: Join produces 0 rows because key formats differ (e.g., integer 101 vs varchar 'CUST-101').
   Fix: Transform one side to match: CAST(REPLACE(c.id, 'CUST-', '') AS INTEGER).

## Root Cause Description Rules
When writing ROOT_CAUSE, be SPECIFIC:
- Name the exact columns involved (e.g., "total_amount", "amount")
- Name the exact tables involved (e.g., "source_data", "stg_sales")
- Describe the nature of the problem (e.g., "column name mismatch", "missing column", "type cast error", "join key format mismatch")
- If types differ, name both types (e.g., "VARCHAR", "DOUBLE", "INTEGER")

When you have enough information, provide your FINAL DIAGNOSIS in this exact format (no markdown code fences):

ROOT_CAUSE: <one line describing the root cause — be specific about column names, table names, and the nature of the mismatch>
FIX_TYPE: sql_modification
FIX_DESCRIPTION: <what needs to change>
FIXED_SQL:
<the corrected SQL query — raw SQL only, no code fences>
VERIFICATION_QUERY:
<a SELECT query to verify the fix worked — raw SQL only, no code fences>

Important rules:
- Call ONE tool at a time using the TOOL_CALL format
- Wait for each tool result before calling the next
- You MUST inspect schemas before diagnosing"""


# The tools available to the agent, as plain functions for Ollama's native tool calling
TOOL_FUNCTIONS: list[callable] = [execute_sql, inspect_schema, parse_logs, read_file]


@dataclass
class FunctionCall:
    name: str
    arguments: dict[str, Any]


@dataclass
class MessageProxy:
    """Unified message interface that works with both native and structured modes."""
    content: str = ""
    tool_calls: list[Any] | None = None


@dataclass
class ResponseProxy:
    """Unified response interface."""
    message: MessageProxy = field(default_factory=MessageProxy)


class OllamaClient:
    """Wrapper around the Ollama Python SDK with automatic fallback.

    Supports two modes:
    - native: Uses Ollama's built-in tool calling (for supported models)
    - structured: Parses TOOL_CALL JSON from model output (universal fallback)

    The mode is auto-detected on the first call. If native tool calling fails
    with a 'does not support tools' error, it falls back to structured mode.
    """

    def __init__(self, model: str = "qwen2.5-coder:7b", tool_mode: str = "auto"):
        self.model = model
        self.tool_mode = tool_mode  # "auto", "native", or "structured"
        self._resolved_mode: str | None = None

    def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[callable] | None = None,
    ) -> ResponseProxy:
        """Send a chat request to Ollama with optional tool definitions."""
        mode = self._resolve_mode(messages, tools)

        if mode == "native":
            return self._chat_native(messages, tools)
        else:
            return self._chat_structured(messages)

    def _resolve_mode(self, messages, tools) -> str:
        """Determine whether to use native or structured tool calling."""
        if self._resolved_mode:
            return self._resolved_mode

        if self.tool_mode == "native":
            self._resolved_mode = "native"
            return "native"
        elif self.tool_mode == "structured":
            self._resolved_mode = "structured"
            return "structured"

        # Auto-detect: try native first
        if tools:
            try:
                ollama.chat(
                    model=self.model,
                    messages=[{"role": "user", "content": "test"}],
                    tools=tools,
                    options={"num_predict": 1},
                )
                self._resolved_mode = "native"
                return "native"
            except Exception as e:
                if "does not support tools" in str(e):
                    self._resolved_mode = "structured"
                    return "structured"
                # Other errors — still try native
                self._resolved_mode = "native"
                return "native"

        self._resolved_mode = "native"
        return "native"

    def _chat_native(
        self,
        messages: list[dict[str, Any]],
        tools: list[callable] | None = None,
    ) -> ResponseProxy:
        """Use Ollama's native tool calling."""
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "options": {"temperature": 0},
        }
        if tools:
            kwargs["tools"] = tools

        response = ollama.chat(**kwargs)

        # Convert to our unified interface
        proxy_calls = None
        if response.message.tool_calls:
            proxy_calls = response.message.tool_calls

        content = response.message.content or ""

        # Fallback: some models emit tool calls as JSON in content instead of tool_calls
        # This handles both raw JSON and markdown-wrapped JSON (```json ... ```)
        if not proxy_calls and tools:
            stripped = content.strip()
            # Strip markdown code fences
            if stripped.startswith("```"):
                stripped = re.sub(r"^```\w*\n?", "", stripped)
                stripped = stripped.split("```")[0].strip()
            if stripped.startswith("{"):
                parsed = self._parse_content_tool_call(stripped)
                if parsed:
                    return ResponseProxy(
                        message=MessageProxy(content="", tool_calls=[parsed])
                    )

        return ResponseProxy(
            message=MessageProxy(content=content, tool_calls=proxy_calls)
        )

    @staticmethod
    def _parse_content_tool_call(content: str) -> Any | None:
        """Parse a tool call from content that looks like JSON."""
        # Try to extract the first JSON object
        json_str = _extract_balanced_json(content.strip(), 0)
        if not json_str:
            return None
        try:
            data = json.loads(json_str)
            name = data.get("name", "")
            args = data.get("arguments", {})
            if name:
                return type("ToolCall", (), {
                    "function": type("Function", (), {
                        "name": name,
                        "arguments": args,
                    })()
                })()
        except (json.JSONDecodeError, KeyError):
            pass
        return None

    def _chat_structured(self, messages: list[dict[str, Any]]) -> ResponseProxy:
        """Use structured text parsing for tool calls (universal fallback)."""
        # Swap system prompt to structured version
        patched = []
        for m in messages:
            if m.get("role") == "system":
                patched.append({"role": "system", "content": STRUCTURED_SYSTEM_PROMPT})
            elif m.get("role") == "tool":
                # Convert tool results to assistant/user format for models without tool role
                patched.append({"role": "user", "content": f"Tool result:\n{m['content']}"})
            else:
                # Strip tool_calls from assistant messages (not supported in structured mode)
                cleaned = {k: v for k, v in m.items() if k != "tool_calls"}
                patched.append(cleaned)

        response = ollama.chat(
            model=self.model,
            messages=patched,
            options={"temperature": 0},
        )

        content = response.message.content or ""

        # Parse TOOL_CALL from response
        tool_call = self._parse_tool_call(content)
        if tool_call:
            # Truncate content to just the reasoning BEFORE the tool call
            # This prevents the model from seeing its own premature answer
            truncated = content.split("TOOL_CALL:")[0].strip()
            return ResponseProxy(
                message=MessageProxy(
                    content=truncated,
                    tool_calls=[tool_call],
                )
            )

        # No tool call found — this is a final answer
        return ResponseProxy(
            message=MessageProxy(content=content, tool_calls=None)
        )

    @staticmethod
    def _parse_tool_call(content: str) -> Any | None:
        """Parse a TOOL_CALL JSON from model output."""
        # Find the start of TOOL_CALL:
        match = re.search(r'TOOL_CALL:\s*', content)
        if not match:
            return None

        # Extract balanced JSON starting from the first {
        json_start = content.find("{", match.end() - 1)
        if json_start == -1:
            json_start = content.find("{", match.start())
        if json_start == -1:
            return None

        json_str = _extract_balanced_json(content, json_start)
        if not json_str:
            return None

        try:
            data = json.loads(json_str)
            tool_name = data.get("tool", "")
            tool_args = data.get("args", {})

            # Create a mock tool call object compatible with the agent loop
            call = type("ToolCall", (), {
                "function": type("Function", (), {
                    "name": tool_name,
                    "arguments": tool_args,
                })()
            })()
            return call
        except (json.JSONDecodeError, KeyError):
            return None


def _extract_balanced_json(text: str, start: int) -> str | None:
    """Extract a balanced JSON object from text starting at the given position."""
    if start >= len(text) or text[start] != "{":
        return None

    depth = 0
    in_string = False
    escape = False

    for i in range(start, len(text)):
        c = text[i]

        if escape:
            escape = False
            continue

        if c == "\\":
            escape = True
            continue

        if c == '"' and not escape:
            in_string = not in_string
            continue

        if in_string:
            continue

        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]

    return None
