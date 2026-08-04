# The Process Behind the Platform — interactive site

`index.html` is a single-file, dependency-free static site telling the
*process* story of this project: how a human supervised an autonomous coding
agent (Claude Code) that helped build an AI agent system, including the
corrections, dead ends, and the moments where "done" didn't survive scrutiny.

**Source of truth:** [`PROJECT_NARRATIVE.md`](PROJECT_NARRATIVE.md).
Every number and commit hash on the page is cross-checked against the
repository history and the build sessions' records; the page's *Fidelity*
section discloses the two places where the narrative's wording and the
session records diverged.

## Viewing

```bash
# from the repo root — any static server works
python3 -m http.server 8710 --directory docs
# then open http://localhost:8710/
```

Or simply open `docs/index.html` directly in a browser (no build step, no
network requests, no external assets).

This layout (`/docs`) is also GitHub-Pages-ready: Settings → Pages →
"Deploy from a branch" → `/docs`.

## Fidelity rules this site follows

- Nothing invented or embellished beyond `PROJECT_NARRATIVE.md`.
- Interactive demos implement the **real** production rules and thresholds
  (the geo hard rule, the ordering guard, the secondary-roles behavior, the
  actual salvaged confusion matrix) — they are not stylized illustrations.
- Where the narrative lacked detail (chat-side design discussions), the page
  says so via source tags instead of filling gaps.

---

## Documentation index (this folder)

All project documentation lives here; the root keeps only `README.md`.

**Interactive site** (open in a browser)
- [`index.html`](index.html) — the process story
- [`phases.html`](phases.html) — what each phase does, with what, and why
- [`stack.html`](stack.html) — each tool's role in the system
- [`architecture.html`](architecture.html) — the LangGraph agent graphs, clickable

**Narrative & status**
- [`PROJECT_NARRATIVE.md`](PROJECT_NARRATIVE.md) — how the project was actually built (site source of truth)
- [`PROJECT_STATUS.md`](PROJECT_STATUS.md) — phase-by-phase status and verification log

**Design & policy**
- [`DATA_GOVERNANCE.md`](DATA_GOVERNANCE.md) — access, redaction, and retention policy
- [`REPLAY_STRATEGY.md`](REPLAY_STRATEGY.md) — idempotent replay design (staging + MERGE)

**Investigations**
- [`GEO_FLAGGING_INVESTIGATION.md`](GEO_FLAGGING_INVESTIGATION.md) — geo over-flagging root cause + fix
- [`PATTERN_ID_INVESTIGATION.md`](PATTERN_ID_INVESTIGATION.md) — fraud-pattern identification gap + fix
