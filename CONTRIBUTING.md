# Contributing to Skill Radar

Thank you for contributing to **Skill Radar**.

This document defines the development workflow, coding standards, and contribution rules to ensure code quality, reproducibility, and a clean Git history.

> Before opening a PR, run `make check` and ensure all checks pass.

---

# 1. Branching Strategy

We use a simple and professional branching model:

- `main` → Stable, release-ready (protected)
- `develop` → Integration branch
- `feature/<topic>` → New features
- `fix/<topic>` → Bug fixes
- `chore/<topic>` → Tooling / CI / maintenance

## Rules

- Always branch from `develop`.
- Open Pull Requests targeting `develop`.
- `main` only receives changes via PR from `develop`.
- Direct pushes to protected branches are disabled.

---

# 2. Development Setup

## First-Time Setup

After cloning the repository, run:

```bash
make bootstrap
```

This will:

* Install all project dependencies (`uv sync --dev`)
* Install Git pre-commit hooks
* Run a full environment validation

---

## Environment Validation

To verify your environment at any time, run:

```bash
make doctor
```

This performs a read-only health check of:

### Environment & Tooling

* `uv` installation
* Python version (3.11 required)
* Project dependencies available

### Repository Configuration

* `.env` file presence
* Required environment variables (e.g. `ADZUNA_APP_ID`, `ADZUNA_APP_KEY`)
* Pre-commit hook installation

### Runtime Sanity

* Package import (`skill_radar`)
* Docker availability
* MinIO health
* Spark + Iceberg smoke test

If any check fails, the script prints clear and actionable instructions.

---

## Automatic Repair Mode

If setup issues are detected, you can attempt automatic fixes:

```bash
make doctor-fix
```

This will:

* Reinstall dependencies if needed
* Install missing pre-commit hooks

---

## Installing uv (if not already installed)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

or on macOS:

```bash
brew install uv
```

---

## Dependency Management

All dependencies are managed via `uv`.

Install or update dependencies with:

```bash
uv sync --dev
```

All versions are locked in `uv.lock`.

Do **not** modify `uv.lock` manually.

---

# 3. Running the Project

## Local Infrastructure

Start lakehouse stack:

```bash
make infra
```

Reset stack completely:
```bash
make infra-reset
```

Validate infrastructure:
```bash
make smoke
```

⸻

## Running Tests

Unit tests:
```bash
make test
```

Integration tests (requires Docker infra running):
```bash
make itest
```

Full validation (what CI runs):
```bash
make ci
```

---

# 4. Managing Dependencies

Always use uv (never `pip install`).

Add runtime dependency:

```bash
uv add <package>
```

Add development dependency:

```bash
uv add --dev <package>
```

Commit both:

```bash
pyproject.toml
uv.lock
```

---

# 5. Code Quality Standards

All contributors must validate their changes locally before opening a Pull Request.

We provide convenient Make targets to run quality checks.

---

## Automatic Fix (During Development)

While coding, you may auto-fix formatting and lint issues:

```bash
make fix
```

This runs:

* Ruff lint auto-fix
* Ruff formatter

This command modifies files in place.

---

## Full Local Validation (Before PR)

Before opening a PR, run:

```bash
make check
```

This validates:

* Ruff lint (no auto-fix)
* Ruff formatting (check mode)
* Type checking (mypy)
* Unit tests (pytest)

This command does **not** modify files.
It must pass before submitting a PR.

---

## Pre-commit Hooks

Install once:

```bash
uv run pre-commit install
```

Pre-commit runs automatically on every commit.

To manually run all hooks:

```bash
make precommit
```

CI enforces the same checks automatically.

---

# 6. Pull Request Guidelines

Before opening a PR:

* Rebase onto latest `develop`
* Ensure all checks pass locally
* Keep PRs focused and reasonably small

PR description must include:

* What was changed
* Why it was changed
* How it was tested
* Reference to issue (if applicable)

Example:

```
Closes #42
```

At least **one reviewer approval** is required.

---

# 7. Commit Message Convention

We follow **Conventional Commits**:

```
<type>(optional-scope): short description
```

Examples:

```
feat(ingestion): add watermark logic
fix(ci): correct uv sync command
docs: update setup instructions
```

### Allowed Types

* feat
* fix
* refactor
* docs
* test
* chore
* ci
* perf
* revert

### Rules

* Use imperative form ("add", not "added")
* Keep it concise
* One logical change per commit
* No trailing period

---

# 8. Squash and Merge

Feature branches may contain multiple commits.

Pull Requests must be merged using **Squash and merge** to maintain a clean history.

The final squashed commit must follow the Conventional Commit format.

> **Another Option — CLI (Before Opening the PR)**
>
> To keep the Git history clean, contributors should squash commits on their feature branch **before creating a pull request**.
>
> 1. **Work on a feature branch:**
> ```bash
> git checkout -b feature/my-feature
> # make changes and commit as usual
> git commit -m "feat: add new preprocessing step"
> ```
> 2. **Squash commits locally (optional but recommended):**
>
> ```bash
> git rebase -i develop
> ```
>
> - In the editor, leave the first commit as `pick` and change all others to `squash`.
>
> - Edit the final commit message to summarize the changes.
>
> 3. **Push your branch:**
>
> ```bash
> git push --force-with-lease
> ```
>
> 4. **Open a Pull Request targeting `develop`.**
>
> - The PR should show a single commit if you squashed locally.
> - If not, GitHub allows you to “Squash and merge” when merging the PR.

---

# 9. Security & Best Practices

* Never commit `.env` files or secrets.

* Never expose API keys in logs.

* Do not commit generated data.

* Keep the import structure consistent:

  ```python
  from skill_radar import ...
  ```

* Do not bypass CI checks.

---

# 10. Philosophy

Skill Radar aims to be:

* Reproducible
* Deterministic
* Cleanly versioned
* Production-oriented

Code should be readable, typed where useful, and maintainable.

When in doubt: prefer simplicity.
