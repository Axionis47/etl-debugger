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
from eval.scorer import score_root_cause, score_fix, _extract_terms

console = Console()


class EvalRunner:
    """Runs the agent against all golden set cases and collects results."""

    def __init__(
        self,
        llm: OllamaClient,
        golden_dir: str = "golden_set",
        verbose_scoring: bool = False,
    ):
        self.llm = llm
        self.golden_dir = Path(golden_dir)
        self.verbose_scoring = verbose_scoring
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

    def run_all(self, case_filter: str | None = None) -> EvalReport:
        """Run golden set cases and return an evaluation report.

        Args:
            case_filter: Optional case ID prefix to run a single case
                         (e.g., "case_01" or "case_03_type_cast_error").
        """
        cases_to_run = self.cases
        if case_filter:
            cases_to_run = [c for c in self.cases if c.id.startswith(case_filter)]
            if not cases_to_run:
                console.print(f"[red]No case matching '{case_filter}' found.[/red]")
                available = ", ".join(c.id for c in self.cases)
                console.print(f"Available: {available}")
                return EvalReport(model=self.llm.model, results=[])

        results = []
        for i, case in enumerate(cases_to_run, 1):
            console.print(f"[bold]Running case {i}/{len(cases_to_run)}: {case.name}[/bold]")
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
            fix_exec_result = ""
            if diagnosis.fixed_sql and diagnosis.status == DiagnosisStatus.SUCCESS:
                fix_valid = score_fix(
                    fixed_sql=diagnosis.fixed_sql,
                    verification_query=diagnosis.verification_query or expected_fix.verification_query,
                    engine=case.engine,
                    expected_min_rows=expected_fix.expected_row_count_min,
                )
                # Capture SQL execution result for verbose output
                if self.verbose_scoring:
                    fix_exec_result = execute_sql(diagnosis.fixed_sql, case.engine)

            # Verbose scoring output
            if self.verbose_scoring:
                self._print_verbose_scoring(
                    case, diagnosis, expected_fix,
                    root_cause_match, fix_valid, fix_exec_result,
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

    def _print_verbose_scoring(
        self, case, diagnosis, expected_fix,
        root_cause_match, fix_valid, fix_exec_result,
    ):
        """Print detailed scoring breakdown for a case."""
        console.print(f"\n  [dim]{'=' * 60}[/dim]")
        console.print(f"  [bold]Verbose Scoring: {case.id}[/bold]")

        # Root cause analysis
        pred_terms = _extract_terms(diagnosis.root_cause) if diagnosis.root_cause else set()
        exp_terms = _extract_terms(expected_fix.root_cause)
        overlap = pred_terms & exp_terms
        ratio = len(overlap) / len(exp_terms) if exp_terms else 0

        console.print(f"  [cyan]Predicted root cause:[/cyan] {diagnosis.root_cause[:150] or '(empty)'}")
        console.print(f"  [cyan]Expected root cause:[/cyan]  {expected_fix.root_cause[:150]}")
        console.print(f"  [cyan]Matched terms:[/cyan]  {sorted(overlap) if overlap else '(none)'}")
        console.print(f"  [cyan]Missing terms:[/cyan]  {sorted(exp_terms - pred_terms) if exp_terms - pred_terms else '(none)'}")
        console.print(f"  [cyan]Overlap ratio:[/cyan]  {ratio:.2f} (threshold: 0.50)")

        rc_status = "[green]PASS[/green]" if root_cause_match else "[red]FAIL[/red]"
        console.print(f"  [cyan]Root cause:[/cyan]    {rc_status}")

        # Fix analysis
        if diagnosis.fixed_sql:
            sql_preview = diagnosis.fixed_sql[:200].replace("\n", " ")
            console.print(f"  [cyan]Fixed SQL:[/cyan]     {sql_preview}...")
            if fix_exec_result:
                exec_preview = fix_exec_result[:100].replace("\n", " ")
                console.print(f"  [cyan]SQL result:[/cyan]   {exec_preview}")
        else:
            console.print(f"  [cyan]Fixed SQL:[/cyan]     (empty - no SQL extracted)")

        fix_status = "[green]PASS[/green]" if fix_valid else "[red]FAIL[/red]"
        console.print(f"  [cyan]Fix valid:[/cyan]     {fix_status}")
        console.print(f"  [dim]{'=' * 60}[/dim]")

    @staticmethod
    def _load_expected_fix(case_dir: Path) -> ExpectedFix:
        """Load the expected fix for a golden set case."""
        fix_path = case_dir / "expected_fix.yaml"
        with open(fix_path) as f:
            data = yaml.safe_load(f)
        return ExpectedFix(**data)
