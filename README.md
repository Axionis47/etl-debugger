# ETL Pipeline Debugger Agent

An **agentic AI system** that autonomously diagnoses and fixes broken **SQL and ETL pipelines** using local LLMs via Ollama. Built with a **ReAct (Reason + Act) agent loop**, the system investigates pipeline failures through iterative tool use, identifies root causes, and proposes verified fixes.

Includes a **golden set evaluation framework** with scoring rubrics to measure accuracy and **hill-climb on metrics** for continuous optimization.

## What It Does

- Ingests ETL pipeline definitions (YAML) and error logs
- Runs an autonomous **agentic workflow**: diagnose &rarr; hypothesize &rarr; test &rarr; verify
- Uses tools (SQL executor, schema inspector, log parser) to gather evidence
- Proposes and validates fixes against live databases (SQLite / DuckDB)
- Evaluates agent performance with golden sets and automated scoring rubrics

## Architecture

```
User ──> CLI ──> Agent Loop (ReAct) ──> LLM (Ollama) + Tools ──> Diagnosis Report
                    │
                    ├── execute_sql      → Run queries on SQLite/DuckDB
                    ├── inspect_schema   → Column names, types, row counts
                    ├── compare_schemas  → Side-by-side schema comparison with type mismatch detection
                    ├── sample_values    → Distinct value sampling for join key / data quality analysis
                    ├── parse_logs       → Search error logs with regex
                    └── read_file        → Read pipeline configs & data
```

The full **AI engineering lifecycle** is represented: design, **prompt/tool engineering**, **evals**, measurement, and optimization.

### Agent Loop

The core is a **ReAct-style agent loop** — the LLM reasons about the problem, acts by calling tools, observes the results, and iterates until it converges on a diagnosis. This is the same pattern used in production **automated coding agents**.

```
for each step (max 15):
    LLM receives: system prompt + pipeline context + pre-computed diagnostics + conversation history
    LLM responds with:
      (a) tool_call  → execute tool, append result, loop
      (b) final_answer → extract diagnosis + proposed fix, exit
```

The agent also **pre-computes diagnostic context** (schema comparisons, join key samples) and injects them into the initial prompt. This compensates for small models that may complete in a single step without calling tools.

### Tools

| Tool | Purpose |
|------|---------|
| `execute_sql` | Run SQL queries against SQLite or DuckDB |
| `inspect_schema` | Inspect table schemas, column types, row counts |
| `compare_schemas` | Compare two table schemas side-by-side with type mismatch detection and sample values |
| `sample_values` | Get distinct sample values from a column (join keys, data quality) |
| `parse_logs` | Search and filter ETL error logs with regex |
| `read_file` | Read pipeline configs, CSV data, SQL scripts |

## Hill-Climbing Results

The system was iteratively optimized through **5 iterations** of prompt engineering, tool improvements, and parsing hardening. Each iteration targeted specific failure cases and was measured against the golden set:

| Iteration | Change | Diagnosis Accuracy | Fix Accuracy | Delta |
|-----------|--------|-------------------|--------------|-------|
| 0 (baseline) | Initial implementation | 25% (1/4) | 25% (1/4) | -- |
| 1 | Diagnostic checklist + bug pattern library in system prompt | 50% (2/4) | 50% (2/4) | +25% |
| 2 | `compare_schemas` + `sample_values` tools + data quality guidance | 75% (3/4) | 75% (3/4) | +25% |
| 3 | Pre-computed diagnostics in user prompt + join key sampling | 75% (3/4) | 75% (3/4) | stable |
| 4 | Hardened output parsing with fallback extraction + edge-case tests | 100% (4/4) | 50-75% (varies) | +25% diag |
| 5 | Verbose scoring, per-case eval, observability | 100% (4/4) | 50-75% (varies) | observability |

**Key insight:** Fix accuracy with a 7B parameter local model varies between runs due to non-deterministic generation. Diagnosis accuracy stabilized at 100% after parsing hardening (Iteration 4). Larger models (32B+) would improve fix accuracy further.

### Latest Eval Output (qwen2.5-coder:7b)

```
Eval Results -- qwen2.5-coder:7b
+---------------------------+------------+-----------+-------+----------+
| Case                      | Root Cause | Fix Valid | Steps | Time (s) |
+---------------------------+------------+-----------+-------+----------+
| case_01_schema_mismatch   |    PASS    |   PASS    |   1   |   23.8   |
| case_02_missing_column    |    PASS    |   PASS    |   1   |    9.7   |
| case_03_type_cast_error   |    PASS    |   PASS    |   1   |   11.5   |
| case_04_join_key_mismatch |    PASS    |   FAIL    |   1   |   11.3   |
+---------------------------+------------+-----------+-------+----------+

Diagnosis Accuracy: 100% (4/4)
Fix Accuracy:       75% (3/4)
Mean Steps:          1.0
Mean Time:           14.1s
```

## Quick Start

```bash
# Prerequisites: Ollama running with a model
ollama pull qwen2.5-coder:7b

# Install
pip install -e .

# Diagnose a pipeline
etl-debug diagnose --pipeline golden_set/case_01_schema_mismatch/pipeline.yaml --verbose

# Diagnose and auto-fix
etl-debug diagnose --pipeline golden_set/case_03_type_cast_error/pipeline.yaml --auto-fix

# Run the full evaluation suite
etl-debug eval

# Run a single case
etl-debug eval --case case_03

# Run eval with verbose scoring breakdown
etl-debug eval --verbose-scoring

# List available tools
etl-debug list-tools
```

## Golden Set

Four test cases covering common ETL failure patterns, each with a pipeline config, error log, source data, and expected fix:

| Case | Bug Type | Engine | Difficulty |
|------|----------|--------|-----------|
| Schema Mismatch | Column renamed in destination table | DuckDB | Easy |
| Missing Column | Source stopped sending `region` field | SQLite | Easy |
| Type Cast Error | `$12.50` and `N/A` can't cast to DOUBLE | DuckDB | Medium |
| Join Key Mismatch | INT `101` vs VARCHAR `CUST-101` join keys | DuckDB | Medium |

## Evaluation Framework

The eval framework implements **golden sets and rubrics** to measure the accuracy of AI-driven processes -- designed for **hill-climbing on metrics** to optimize the agentic system.

### Metrics

| Metric | How It's Scored |
|--------|----------------|
| **Root Cause Match** | Keyword overlap between agent diagnosis and expected root cause (50% threshold) |
| **Fix Validity** | Agent's fixed SQL executes without error AND verification query returns expected rows |
| **Efficiency** | Steps taken + wall-clock time (tracked, not gated) |

### Verbose Scoring

Use `--verbose-scoring` to see detailed scoring breakdowns per case:

```
Verbose Scoring: case_01_schema_mismatch
  Predicted root cause: Column name mismatch - 'total_amount' vs 'amount' in stg_sales
  Expected root cause:  Column name mismatch: pipeline references total_amount but table has amount
  Matched terms:  ['amount', 'column', 'mismatch', 'total_amount']
  Missing terms:  ['pipeline', 'references', 'table']
  Overlap ratio:  0.57 (threshold: 0.50)
  Root cause:    PASS
  Fixed SQL:     INSERT INTO stg_sales ...
  Fix valid:     PASS
```

## CLI Reference

```
etl-debug diagnose --pipeline <path> [options]
  --log, -l              Path to error log (auto-detected if omitted)
  --model, -m            Ollama model name (default: qwen2.5-coder:7b)
  --max-steps            Maximum agent iterations (default: 10)
  --auto-fix             Apply the proposed fix and verify
  --verbose, -v          Show full agent trace (every tool call + LLM response)
  --tool-mode            Tool calling mode: auto, native, or structured (default: auto)

etl-debug eval [options]
  --golden-dir, -g       Golden set directory (default: golden_set/)
  --model, -m            Ollama model name
  --output, -o           Output format: table or json
  --tool-mode            Tool calling mode: auto, native, or structured
  --verbose-scoring      Show detailed scoring breakdown per case
  --case                 Run a single case by ID prefix (e.g., case_01)

etl-debug list-tools
etl-debug --version
```

## Design Decisions

- **Why ReAct over plan-then-execute?** More debuggable, shows reasoning trace, naturally self-corrects when a tool returns unexpected results. This is the standard pattern in production coding agents.
- **Why Ollama?** Runs fully local, no API keys, reproducible across machines. The model is configurable via `--model` flag, so any Ollama-compatible model works.
- **Why DuckDB + SQLite?** Embedded databases -- zero server setup. DuckDB for analytics SQL patterns, SQLite for transactional. No external infrastructure needed to run or demo.
- **Why keyword scoring over LLM-as-judge?** Reproducible, deterministic, no second LLM call needed. Avoids the "quis custodiet" problem of using an LLM to evaluate an LLM.
- **Why no LangChain/CrewAI?** Building the agent loop from scratch demonstrates deeper understanding of agentic architectures. The entire ReAct loop is ~200 lines of Python.
- **Why dual-mode tool calling?** Not all Ollama models support native tool calling. The agent auto-detects model capabilities and falls back to structured text parsing -- making it model-agnostic.
- **Why pre-computed diagnostics?** Small models (7B) often complete in a single step without calling tools. Injecting schema comparisons and join key samples directly into the prompt ensures the agent has diagnostic data regardless of tool-calling behavior.

## Project Structure

```
etl-debugger/
├── src/
│   ├── cli.py              # CLI entry point (Click + Rich)
│   ├── agent.py            # ReAct agent loop + pre-computed diagnostics
│   ├── llm.py              # Ollama client wrapper (dual-mode tool calling)
│   ├── models.py           # Pydantic data models
│   ├── pipeline.py         # Pipeline config loader + test DB setup
│   └── tools/              # Agent tools (6 tools)
│       ├── sql_executor.py
│       ├── schema_inspector.py  # inspect_schema, compare_schemas, sample_values
│       ├── log_parser.py
│       └── file_reader.py
├── eval/
│   ├── runner.py           # Golden set evaluation runner (verbose scoring, per-case eval)
│   └── scorer.py           # Scoring: keyword overlap + execution-based validation
├── golden_set/             # 4 test cases with expected fixes
├── tests/                  # 46 unit + integration tests
├── pyproject.toml
└── README.md
```

## How to Add New Test Cases

1. Create a new directory under `golden_set/` (e.g., `case_05_null_handling/`)
2. Add `pipeline.yaml`, `error.log`, `source_data.csv`, `dest_schema.sql`, and `expected_fix.yaml`
3. Register the case in `golden_set/manifest.yaml`
4. Run `etl-debug eval` to test

## Tech Stack

- **Python 3.11+** -- Core language
- **Ollama** -- Local LLM inference (no API keys)
- **DuckDB** -- Embedded analytics database
- **SQLite** -- Embedded transactional database
- **Click** -- CLI framework
- **Rich** -- Terminal formatting
- **Pydantic** -- Data validation and serialization
- **pytest** -- Testing (46 tests)

## Limitations

- Limited to SQL-based ETL pipelines (no Spark/Airflow DAG debugging)
- Relies on error log quality -- vague logs produce vague diagnoses
- Local 7B models occasionally hallucinate SQL syntax; larger models improve accuracy
- Golden set currently has 4 cases -- expand for more robust evaluation
- Fix accuracy varies between runs with small models due to non-deterministic generation

## License

MIT
