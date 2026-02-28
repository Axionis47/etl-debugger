from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    type: str
    path: str | None = None
    tables: list[str] | None = None


class DestinationConfig(BaseModel):
    engine: Literal["sqlite", "duckdb"]
    table: str


class TransformConfig(BaseModel):
    sql: str


class PipelineConfig(BaseModel):
    name: str
    source: SourceConfig
    destination: DestinationConfig
    transform: TransformConfig


class AgentAction(BaseModel):
    """Structured output for models that don't support native tool calling."""

    thought: str = Field(description="Reasoning about what to do next")
    action_type: Literal["tool_call", "final_answer"] = Field(
        description="Whether to call a tool or provide the final answer"
    )
    tool_name: str | None = Field(default=None, description="Name of the tool to call")
    tool_args: dict | None = Field(default=None, description="Arguments for the tool")
    answer: str | None = Field(default=None, description="Final answer if action_type is final_answer")


class DiagnosisStatus(str, Enum):
    SUCCESS = "success"
    MAX_STEPS_REACHED = "max_steps_reached"
    ERROR = "error"


class DiagnosisReport(BaseModel):
    status: DiagnosisStatus
    root_cause: str = ""
    fix_type: str = ""
    fix_description: str = ""
    fixed_sql: str = ""
    verification_query: str = ""
    steps_taken: int = 0
    raw_response: str = ""


class ExpectedFix(BaseModel):
    root_cause: str
    fix_type: str
    fix_description: str
    fixed_sql: str
    verification_query: str
    expected_row_count_min: int | None = None


class GoldenCase(BaseModel):
    id: str
    name: str
    difficulty: str
    category: str
    engine: Literal["sqlite", "duckdb"]
    pipeline: PipelineConfig | None = None
    error_log: str = ""
    expected_fix: ExpectedFix | None = None


class CaseResult(BaseModel):
    case_id: str
    case_name: str = ""
    root_cause_match: bool = False
    fix_valid: bool = False
    steps_taken: int = 0
    time_seconds: float = 0.0
    error: str = ""


class EvalReport(BaseModel):
    model: str
    results: list[CaseResult]

    @property
    def diagnosis_accuracy(self) -> float:
        if not self.results:
            return 0.0
        return sum(1 for r in self.results if r.root_cause_match) / len(self.results)

    @property
    def fix_accuracy(self) -> float:
        if not self.results:
            return 0.0
        return sum(1 for r in self.results if r.fix_valid) / len(self.results)

    @property
    def mean_steps(self) -> float:
        if not self.results:
            return 0.0
        return sum(r.steps_taken for r in self.results) / len(self.results)

    @property
    def mean_time(self) -> float:
        if not self.results:
            return 0.0
        return sum(r.time_seconds for r in self.results) / len(self.results)
