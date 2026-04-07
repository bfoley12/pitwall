# Contributing to pitwall

Thanks for your interest in contributing! pitwall is in early development (`0.x`), so the API is still evolving - but that also means there's plenty of room to shape things.

## Setup

```bash
git clone https://github.com/bfoley12/pitwall.git
cd pitwall
uv sync
```

## Development
Run tests, lint, and type check before submitting anything:

```bash
just check
```

Or individually:

```bash
ruff format
ruff check (--fix (--unsafe-fixes))
basedpyright
uv run pytest
```

Both must pass clean.

## What to work on

Check [open issues](https://github.com/bfoley12/pitwall/issues) for anything tagged `good first issue` or `help wanted`. Some areas where contributions are especially useful:

- **Test coverage** for existing feeds
- **Documentation** of existing classes and functions
- **Examples** of loading and basic transformations/uses

If you want to tackle something not listed, open an issue first so we can discuss the approach.

## Pull requests

- Keep PRs focused - one feed, one fix, one feature.
- Include tests for new feeds or changed behavior.
- All tests and basedpyright must pass.
- Use clear commit messages. No strict format required, just be descriptive.

## Code conventions

- **Pydantic v2** models for all feed .json.
- **Polars** for all DataFrames - no Pandas - especially in feed .jsonStream.
- **httpx** for HTTP.
  - This will change to niquest when I have time to read up on it. Stay with httpx for now.
- Store raw strings at ingest (e.g., timestamps as `pl.String`). Parsing and casting belong in the transformation layer.
- Fail loudly on malformed data rather than silently skipping.

## Architecture quick reference

- `fetch()` is a thin HTTP/URL-builder layer. It does not resolve sessions or meetings.
- `get()` handles all domain resolution (meeting, session, folder lookup) and calls `fetch()` with fully resolved paths.
- Feed models live in their own modules and are registered for class-based `client.get(F1DataContainer)` calls.

## Questions?

Open a GitHub issue or discussion. There's no Discord/Slack yet.