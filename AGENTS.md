# AGENTS.md

This document provides guidelines for agents working on this codebase.

### CRITICAL: NO GIT

- **DO NOT** use any `git` commands (e.g., `git add`, `git commit`, `git push`, `git lfs`, etc.).
- **DO NOT** modify the git configuration or use git-related tools.
- All git operations are strictly reserved for the user.

## History

Refer to [AGENTS_HISTORY.md](AGENTS_HISTORY.md) to see the history of agentic interventions and the current/past topics being worked on.

## Build, Lint, Test Commands

### Installation

```bash
uv sync                     # Sync dependencies from pyproject.toml
uv run pre-commit install   # Install pre-commit hooks
```

### Dependency Management

```bash
uv add <package>            # Add a dependency
uv add <package> -D         # Add a dev dependency
uv remove <package>         # Remove a dependency
uv add -r requirements.txt  # Add from requirements file
```

### Running Tests

```bash
uv run pytest                        # Run all tests
uv run pytest tests/                 # Run tests from specific directory
uv run pytest -k "test_name"         # Run tests matching pattern
uv run pytest --markers              # List available markers
uv run pytest -m slow                # Run tests marked as slow
```

### Type Checking

```bash
uvx ty check src/                     # Run ty on src directory
uvx ty check tests/                   # Run ty on tests directory
```

### Linting and Formatting

```bash
uv run ruff check --preview src/     # Run ruff linter
uv run ruff check --fix --preview src/  # Run ruff with auto-fix (use --preview)
uv run ruff format src/              # Format code with ruff
uv run flake8                        # Run flake8 (pydoclint only)
uv run pre-commit run --all-files    # Run all pre-commit hooks
uv run pre-commit run ruff --all-files  # Run specific hook
```

Note: Never call pip directly. Use `uv run <command>` or `uv run python -m <command>`.

### CRITICAL: Always use `uv run` for Python

- **ALWAYS** use `uv run python -c "..."` to execute Python code snippets, never bare `python3 -c`
- **ALWAYS** use `uv run` to invoke Python tools (pytest, ruff, mypy, etc.)
- This ensures the correct virtual environment and dependencies are used
- Example: `uv run python -c "from rich.progress import Progress; import inspect; print(inspect.signature(Progress.add_task))"`

## Code Style Guidelines

### General Principles

- Write clear, maintainable code
- Use type hints for function signatures
- Add docstrings to all public functions
- Prefer explicit over implicit

### Python-Specific Guidelines

#### Type Hinting

- Use built-in generics: `list`, `dict`, `set`, `tuple` instead of
  `List`, `Dict`, `Set`, `Tuple` from `typing`. `Any` is an exception to this rule.
- Use `x | None` instead of `Optional[x]`
- Use `x | None` instead of `Union[x, None]`
- Avoid excessive verbosity that degrades readability
- All function signatures should have type hints

#### Docstrings

- Use numpy-style docstrings for all functions
- Add docstring to every function, especially large/core/user-facing ones
- Include argument descriptions for all meaningful parameters
- Include type in docstring verbatim (exactly as in code)
- Add "Defaults to ..." as last sentence for parameters with defaults
- Example:

  ```python
  def process_data(input_path: Path, output_dir: Path | None = None) -> DataFrame:
      """
      Process input data and save to output directory.

      Parameters
      ----------
      input_path : Path
          Path to input file. Defaults to 'data/input.csv'.
      output_dir : Path | None
          Directory to save processed data. Defaults to None.

      Returns
      -------
      DataFrame
          Processed dataframe.
      """
  ```

#### Logging

- Always use `logging` instead of `print`
- Set up logging at module level:
  ```python
  import logging
  log = logging.getLogger(__name__)
  setup_logging(logging.DEBUG, __name__)
  ```

#### Imports

- Use isort (configured in pyproject.toml)
- Group imports: standard library, third-party, local
- Use absolute imports

#### Parallelization

- Use `joblib` for simple parallelization
- Use `ThreadPoolExecutor` or `ProgressPoolExecutor` when `as_completed` is needed (e.g., progress bars)

#### CLI

- Use `cyclopts` for CLI interfaces
- Avoid hydra, jsonargparse unless specifically requested

#### Path Handling

- When filtering paths, always skip files starting with `.`:
  ```python
  files = path.glob("[!.]*.parquet")
  ```

#### Progress Bars

- Use `from myscript.utils import default_bar` for progress bars:

  ```python
  from myscript.utils import default_bar

  with default_bar() as progress:
      task = progress.add_task("Processing", total=len(items))
      for item in items:
          process(item)
          progress.advance(task)
  ```

#### String Formatting

- Always use f-strings, never `%` formatting or `.format()`

#### Comments

- Avoid section comments (##, ###, etc.)
- Only add comments for complex, non-obvious code (rare)
- Keep comments concise and meaningful

#### Ternary Operator

- Never use `or` as ternary
- Always use explicit `x if condition else y`

### Naming Conventions

- `snake_case` for variables, functions, methods
- `PascalCase` for classes
- `SCREAMING_SNAKE_CASE` for constants
- `_leading_underscore` for private/internal members

### Error Handling

- Use specific exceptions (not bare `except:`)
- Add context to exceptions when helpful
- Log errors with appropriate level before raising

### File Organization

- Follow structure: `src/<package>/`
- Tests in `tests/` directory
- Configuration in `pyproject.toml`

## Pre-commit Hooks

The repository uses pre-commit hooks that run:

- `remove-crlf`, `remove-tabs`, `forbid-tabs`
- `trailing-whitespace`
- `check-merge-conflict`
- `ruff` (lint + format)
- `ty` on `src/`
- `pydoclint-flake8`

Run `pre-commit run --all-files` to verify code quality before committing.

## Key Tools and Versions

- Python: >=3.12
- Linter: ruff
- Formatter: ruff-format
- Type checker: ty (pyright disabled)
- Docstring style: numpy (pydoclint)
- Test runner: pytest
