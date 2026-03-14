# Contributing

Contribution workflow and guidelines for Skill Radar.

## Workflow

1. Create a feature branch from `main`
2. Make changes following [Coding Standards](coding-standards.md)
3. Run `make check` (lint + format + types + unit tests)
4. Commit with a clear message
5. Open a pull request

## Pre-commit Hooks

Installed via `make bootstrap`. Runs ruff linting and formatting checks automatically on commit.

## Quality Gates

Every change must pass:

```bash
make check    # lint + format + mypy + pytest
```

This runs:
- `ruff check .` — linting (100+ rules)
- `ruff format --check .` — formatting verification
- `mypy src` — strict type checking
- `pytest tests/unit` — unit test suite

## Commit Messages

Use conventional commit style:
- `feat: add occupation similarity scoring`
- `fix: handle null salary in Adzuna Silver`
- `refactor: extract common validation helpers`
- `docs: update Gold pipeline guide`

## Branch Naming

- `feature/<description>` — new features
- `fix/<description>` — bug fixes
- `docs/<description>` — documentation changes

## Full Guidelines

See the root [CONTRIBUTING.md](../../CONTRIBUTING.md) for the complete contribution guide including detailed code review criteria and project conventions.

## References

- [Coding Standards](coding-standards.md) — style and conventions
- [Testing](testing.md) — test strategy and execution
