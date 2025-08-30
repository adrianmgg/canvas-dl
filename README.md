# development
## list problems
```bash
# linter issues
uv run ruff check
# type checker issues
uv run mypy src
```

## fix & format
```bash
# run formatter
uv run ruff format
# auto-fix some linter problems
uv run ruff check --fix
```

## IDE setup
- set up ruff (lint & format) in your ide of choice: [docs](https://docs.astral.sh/ruff/editors/setup/)
- set up mypy in your ide of choice
