### Pixi

In order to use the relevant pixi environment, you can call:

```bash
pixi shell && ...
```

Or, optionally, as a one-liner:

```bash
pixi run ...
```

Additionally, by inspecting `pixi.toml`, you can see particular pre-configured tasks such as:

```
pixi run pytest
pixi run ruff-fix
```

### Mypy

This repo strives to adhere to type-checking using mypy. All code should pass static type checking without errors or warnings.

```bash
pixi run mypy
```

### Code Formatting and Linting

Code formatting and linting is handled by Ruff:

```bash
# Format code
pixi run ruff format

# Check for linting issues
pixi run ruff check

# Auto-fix linting issues
pixi run ruff-fix
```

### Testing

Tests are run using pytest:

```bash
# Run all tests
pixi run pytest

# Run with coverage
pixi run pytest --cov

# Run specific test file
pixi run pytest tests/test_example.py
```

### Development Container

This project uses VS Code devcontainers for consistent development environments. The container includes:

- Python runtime and dependencies
- Pre-configured tools (mypy, ruff, pytest)
- GitHub CLI
- Common Linux utilities

You will need:

- Docker (the Docker engine and optionally Docker desktop for convenience)
- VSCode with the `Dev Containers` extension installed (or another IDE / library that respects the `devcontainer.json` standard)

To use the devcontainer:
1. Open the project in VS Code
2. Use `Cmd + Shift + P` -> `Dev Containers: Reopen in Container`
3. All tools are pre-installed and configured

To rebuild the devcontainer (for reproducibility, or when making / validating environment changes)
- `Cmd + Shift + P` -> `Dev Containers: Rebuild Container`

The above will reuse unchanged docker build layers, to speed up the build (changing only what Docker views as new, and anything _lower_ in the build instructions). It will take longer, but if you want to be _really_ sure you are getting a totally clean and reproducible environment, you can run:
- `Cmd + Shift + P` -> `Dev Containers: Rebuild Container Without Cache`


### Pre-commit Workflow

Before committing code, ensure:

```bash
pixi run ruff-fix    # Fix formatting and auto-fixable issues
pixi run mypy        # Verify type checking passes
pixi run pytest     # Ensure all tests pass
```