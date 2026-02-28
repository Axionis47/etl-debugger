from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.agent import AgentLoop
from src.llm import OllamaClient
from src.models import DiagnosisStatus
from src.pipeline import load_pipeline, load_error_log, setup_test_db
from src.tools.sql_executor import set_db_path, execute_sql
from src.tools.file_reader import set_base_dir

console = Console()


@click.group()
@click.version_option(version="0.1.0", prog_name="etl-debug")
def main():
    """ETL Pipeline Debugger Agent — AI-powered ETL diagnostics."""
    pass


@main.command()
@click.option("--pipeline", "-p", required=True, type=click.Path(exists=True), help="Path to pipeline YAML file")
@click.option("--log", "-l", type=click.Path(exists=True), default=None, help="Path to error log (auto-detected if omitted)")
@click.option("--model", "-m", default="qwen2.5-coder:7b", help="Ollama model name")
@click.option("--max-steps", default=10, help="Maximum agent iterations")
@click.option("--auto-fix", is_flag=True, help="Apply the proposed fix and verify")
@click.option("--verbose", "-v", is_flag=True, help="Show full agent trace")
@click.option("--tool-mode", type=click.Choice(["auto", "native", "structured"]), default="auto", help="Tool calling mode")
def diagnose(pipeline: str, log: str | None, model: str, max_steps: int, auto_fix: bool, verbose: bool, tool_mode: str):
    """Diagnose a broken ETL pipeline."""
    pipeline_path = Path(pipeline)
    case_dir = pipeline_path.parent

    console.print(f"\n[bold]ETL Pipeline Debugger[/bold]")
    console.print(f"Model: {model}")
    console.print(f"Pipeline: {pipeline_path.name}")
    console.print()

    # Load pipeline config
    config = load_pipeline(pipeline_path)

    # Load error log
    if log:
        error_log = Path(log).read_text()
    else:
        error_log = load_error_log(pipeline_path)

    if not error_log:
        console.print("[red]No error log found. Provide one with --log.[/red]")
        return

    # Set up test database
    db_path = setup_test_db(config, case_dir)
    set_db_path(config.destination.engine, db_path)
    set_base_dir(str(case_dir))

    # Run agent
    llm = OllamaClient(model=model, tool_mode=tool_mode)
    agent = AgentLoop(llm=llm, max_steps=max_steps, verbose=verbose)

    console.print(f"Tool mode: {llm._resolved_mode or tool_mode}")
    with console.status("[bold green]Agent is diagnosing...[/bold green]"):
        report = agent.run(config, error_log, case_dir=str(case_dir))

    # Display results
    _display_report(report)

    # Auto-fix
    if auto_fix and report.fixed_sql and report.status == DiagnosisStatus.SUCCESS:
        console.print("\n[bold yellow]Applying fix...[/bold yellow]")
        result = execute_sql(report.fixed_sql, config.destination.engine)
        console.print(f"  Execute: {result}")

        if "Error" in result:
            console.print("[bold red]Fix failed to execute.[/bold red]")
        elif report.verification_query:
            verify = execute_sql(report.verification_query, config.destination.engine)
            console.print(f"  Verify:  {verify}")
            if "Error" in verify:
                console.print("[bold red]Verification query failed.[/bold red]")
            else:
                console.print("[bold green]Fix applied and verified.[/bold green]")

    # Cleanup temp database
    db_file = Path(db_path)
    if db_file.exists():
        db_file.unlink()


@main.command()
@click.option("--golden-dir", "-g", default="golden_set", type=click.Path(exists=True), help="Golden set directory")
@click.option("--model", "-m", default="qwen2.5-coder:7b", help="Ollama model name")
@click.option("--output", "-o", type=click.Choice(["table", "json"]), default="table", help="Output format")
@click.option("--tool-mode", type=click.Choice(["auto", "native", "structured"]), default="auto", help="Tool calling mode")
def eval(golden_dir: str, model: str, output: str, tool_mode: str):
    """Run evaluation against the golden set."""
    from eval.runner import EvalRunner

    console.print(f"\n[bold]ETL Debugger Eval Suite[/bold]")
    console.print(f"Model: {model}")
    console.print(f"Golden set: {golden_dir}")
    console.print()

    llm = OllamaClient(model=model, tool_mode=tool_mode)
    runner = EvalRunner(llm=llm, golden_dir=golden_dir)

    report = runner.run_all()

    if output == "json":
        console.print(report.model_dump_json(indent=2))
    else:
        _display_eval_report(report)


@main.command("list-tools")
def list_tools():
    """List available agent tools."""
    table = Table(title="Agent Tools")
    table.add_column("Tool", style="cyan")
    table.add_column("Description")

    table.add_row("execute_sql", "Run SQL queries against SQLite or DuckDB")
    table.add_row("inspect_schema", "Inspect table schemas, column types, row counts")
    table.add_row("compare_schemas", "Compare two table schemas side by side with type mismatch detection")
    table.add_row("sample_values", "Get distinct sample values from a column")
    table.add_row("parse_logs", "Search and filter ETL error logs")
    table.add_row("read_file", "Read pipeline configs and data files")

    console.print(table)


def _display_report(report):
    """Pretty-print a diagnosis report."""
    status_color = "green" if report.status == DiagnosisStatus.SUCCESS else "red"

    console.print(Panel(
        f"[bold {status_color}]Status: {report.status.value}[/bold {status_color}]\n"
        f"Steps taken: {report.steps_taken}",
        title="Diagnosis Report",
    ))

    if report.root_cause:
        console.print(f"\n[bold]Root Cause:[/bold] {report.root_cause}")

    if report.fix_description:
        console.print(f"[bold]Fix:[/bold] {report.fix_description}")

    if report.fixed_sql:
        console.print(f"\n[bold]Fixed SQL:[/bold]")
        console.print(Panel(report.fixed_sql, border_style="green"))

    if report.verification_query:
        console.print(f"[bold]Verification Query:[/bold] {report.verification_query}")


def _display_eval_report(report):
    """Pretty-print an eval report."""
    table = Table(title=f"Eval Results — {report.model}")
    table.add_column("Case", style="cyan")
    table.add_column("Root Cause", justify="center")
    table.add_column("Fix Valid", justify="center")
    table.add_column("Steps", justify="center")
    table.add_column("Time (s)", justify="center")

    for r in report.results:
        rc = "[green]PASS[/green]" if r.root_cause_match else "[red]FAIL[/red]"
        fv = "[green]PASS[/green]" if r.fix_valid else "[red]FAIL[/red]"
        if r.error:
            rc = f"[red]ERR[/red]"
            fv = f"[red]ERR[/red]"
        table.add_row(r.case_id, rc, fv, str(r.steps_taken), f"{r.time_seconds:.1f}")

    console.print(table)
    console.print()
    console.print(f"[bold]Diagnosis Accuracy:[/bold] {report.diagnosis_accuracy:.0%} ({sum(1 for r in report.results if r.root_cause_match)}/{len(report.results)})")
    console.print(f"[bold]Fix Accuracy:[/bold]       {report.fix_accuracy:.0%} ({sum(1 for r in report.results if r.fix_valid)}/{len(report.results)})")
    console.print(f"[bold]Mean Steps:[/bold]          {report.mean_steps:.1f}")
    console.print(f"[bold]Mean Time:[/bold]           {report.mean_time:.1f}s")


if __name__ == "__main__":
    main()
