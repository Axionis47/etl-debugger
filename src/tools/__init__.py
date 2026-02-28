from src.tools.sql_executor import execute_sql
from src.tools.schema_inspector import inspect_schema, compare_schemas, sample_values
from src.tools.log_parser import parse_logs
from src.tools.file_reader import read_file

TOOL_REGISTRY: dict[str, callable] = {
    "execute_sql": execute_sql,
    "inspect_schema": inspect_schema,
    "compare_schemas": compare_schemas,
    "sample_values": sample_values,
    "parse_logs": parse_logs,
    "read_file": read_file,
}


def get_tools() -> list[callable]:
    """Return all tools as a list of callables for the Ollama SDK."""
    return list(TOOL_REGISTRY.values())


def execute_tool(name: str, args: dict) -> str:
    """Look up and execute a tool by name."""
    if name not in TOOL_REGISTRY:
        return f"Error: Unknown tool '{name}'. Available: {list(TOOL_REGISTRY.keys())}"
    try:
        return TOOL_REGISTRY[name](**args)
    except Exception as e:
        return f"Error executing {name}: {e}"
