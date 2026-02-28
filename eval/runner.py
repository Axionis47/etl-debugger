from __future__ import annotations

import time
from pathlib import Path

import yaml
from rich.console import Console

from src.agent import AgentLoop
from src.llm import OllamaClient
from src.models import (
    CaseResult,
    DiagnosisStatus,
    EvalReport,
    ExpectedFix,
    GoldenCase,
    PipelineConfig,
)
from src.pipeline import load_pipeline, load_error_log, setup_test_db
from src.tools.sql_executor import set_db_path, execute_sql
from src.tools.file_reader import set_base_dir
from eval.scorer import score_root_cause, score_fix

console = Console()


class EvalRunner:
    """Runs the agent against all golden set cases and collects results."""

    def __init__(self, llm: OllamaClient, golden_dir: str = "golden_set"):
        self.llm = llm
        self.golden_dir = Path(golden_dir)
        self.cases = self._load_manifest()

    def _load_manifest(self) -> list[GoldenCase]:
        """Load the golden set manifest."""
        manifest_path = self.golden_dir / "manifest.yaml"
        with open(manifest_path) as f:
            data = yaml.safe_load(f)

        cases = []
        for case_data in data["cases"]:
            cases.append(GoldenCase(**case_data))
        return cases

    def run_all(self) -> EvalReport:
        """Run all golden set cases and return an evaluation report."""
        results = []
        for i, case in enumerate(self.cases, 1):
            console.print(f"[bold]Running case {i}/{len(self.cases)}: {case.name}[/bold]")
            result = self.run_case(case)
            results.append(result)

            status = "[green]PASS[/green]" if result.fix_valid else "[red]FAIL[/red]"
            console.print(f"  Result: {status} ({result.steps_taken} steps, {result.time_seconds:.1f}s)")

            if result.error:
                console.print(f"  [red]Error: {result.error}[/red]")

        return EvalReport(model=self.llm.model, results=results)

    def run_case(self, case: GoldenCase) -> CaseResult:
        """Run the agent on a single golden set case."""
        case_dir = self.golden_dir / case.id
        start = time.time()

        try:
            # Load pipeline and error log
            pipeline = load_pipeline(case_dir / "pipeline.yaml")
            error_log = load_error_log(case_dir / "pipeline.yaml")

            # Load expected fix
            expected_fix = self._load_expected_fix(case_dir)

            # Set up test database
            db_path = setup_test_db(pipeline, case_dir)
            set_db_path(case.engine, db_path)
            set_base_dir(str(case_dir))

            # Run agent
            agent = AgentLoop(llm=self.llm, max_steps=15, verbose=False)
            diagnosis = agent.run(pipeline, error_log, case_dir=str(case_dir))

            elapsed = time.time() - start

            # Score
            root_cause_match = score_root_cause(diagnosis.root_cause, expected_fix.root_cause)

            fix_valid = False
            if diagnosis.fixed_sql and diagnosis.status == DiagnosisStatus.SUCCESS:
                fix_valid = score_fix(
                    fixed_sql=diagnosis.fixed_sql,
                    verification_query=diagnosis.verification_query or expected_fix.verification_query,
                    engine=case.engine,
                    expected_min_rows=expected_fix.expected_row_count_min,
                )

            # Cleanup temp database
            db_file = Path(db_path)
            if db_file.exists():
                db_file.unlink()

            return CaseResult(
                case_id=case.id,
                case_name=case.name,
                root_cause_match=root_cause_match,
                fix_valid=fix_valid,
                steps_taken=diagnosis.steps_taken,
                time_seconds=elapsed,
            )

        except Exception as e:
            elapsed = time.time() - start
            return CaseResult(
                case_id=case.id,
                case_name=case.name,
                steps_taken=0,
                time_seconds=elapsed,
                error=str(e),
            )

    @staticmethod
    def _load_expected_fix(case_dir: Path) -> ExpectedFix:
        """Load the expected fix for a golden set case."""
        fix_path = case_dir / "expected_fix.yaml"
        with open(fix_path) as f:
            data = yaml.safe_load(f)
        return ExpectedFix(**data)
