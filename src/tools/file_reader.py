from __future__ import annotations

from pathlib import Path

# Base directory for sandboxing file reads
_base_dir: str = ""


def set_base_dir(path: str) -> None:
    """Set the base directory for sandboxed file reads."""
    global _base_dir
    _base_dir = str(Path(path).resolve())


def read_file(file_path: str) -> str:
    """Read the contents of a file (pipeline config, CSV data, SQL scripts).

    Args:
        file_path: Path to the file to read. Must be within the project directory.

    Returns:
        File contents as a string, truncated to 2000 characters if too large.
    """
    try:
        resolved = Path(file_path).resolve()

        # Sandbox check
        if _base_dir and not str(resolved).startswith(_base_dir):
            return f"Error: Access denied. File must be within {_base_dir}"

        if not resolved.exists():
            return f"Error: File not found: {file_path}"

        if not resolved.is_file():
            return f"Error: Not a file: {file_path}"

        content = resolved.read_text()

        if len(content) > 2000:
            return content[:2000] + f"\n... (truncated, {len(content)} total characters)"

        return content
    except Exception as e:
        return f"Error reading file: {e}"
