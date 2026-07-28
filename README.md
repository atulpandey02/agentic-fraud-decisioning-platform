# Agentic Fraud Decisioning Platform

Real-time fraud decisioning: Kafka + Spark streaming features → Redis online
store / Snowflake audit → RAG-grounded multi-agent decisions → governance &
human-in-the-loop → agentic BI over the decision log.

> **The story behind the code:** [`docs/index.html`](docs/index.html) is an
> interactive single-page record of *how* this was built — the supervision
> loop between a human and an autonomous coding agent, the bugs found twice,
> the audit that contradicted an approval, and the metrics that lied.
> Source of truth: [`PROJECT_NARRATIVE.md`](PROJECT_NARRATIVE.md).
> View locally: `python3 -m http.server 8710 --directory docs`.

The code is one installable package, `fraud_platform`. Everything below assumes
the repo root as the working directory and the virtualenv activated
(`source venv/bin/activate`), or prefix commands with `./venv/bin/`.

## Architecture (where things live)

| Area | Package | What it does |
|------|---------|--------------|
| Streaming features | `fraud_platform.stream_processing` | Spark feature engine, velocity engine, hard-rule + weighted scoring |
| Data generation | `fraud_platform.data_generator` | Synthetic users/devices/transactions with injectable fraud |
| Retrieval (RAG) | `fraud_platform.retrieval` | Weaviate hybrid search over fraud policy docs |
| Single agent | `fraud_platform.agents.single_agent` | Phase 3 ReAct agent + its 3 tools |
| Multi-agent | `fraud_platform.agents.multi_agent` | Supervisor orchestrator + 4 specialists |
| Governance / HITL | `fraud_platform.governance` | Autonomy tiers, decision persistence, review queue |
| Observability / eval | `fraud_platform.observability` | Trace writer, LangSmith, eval + LLM judge |
| BI dashboard | `fraud_platform.bi_dashboard` | Guarded NL2SQL over the audit log (Streamlit) |
| DB tooling | `fraud_platform.db` | Schema contract, validators, migration runner, replay MERGE |

Config is one typed, validated object: `fraud_platform.settings.get_settings()`.
Credentials come from `.env` at the repo root (git-ignored). See
`DATA_GOVERNANCE.md` for the access/redaction/retention policy and
`GEO_FLAGGING_INVESTIGATION.md` / `PATTERN_ID_INVESTIGATION.md` for the
scoring/accuracy analyses.

## Commands

### Bootstrap (install)
```bash
python3.10 -m venv venv && source venv/bin/activate
# Local dev (any OS) — resolve from the pyproject ranges:
pip install -e ".[rag,agents,bi,dev]"
# Reproducible / CI install — pin to the LOCKED resolution. The lock
# (constraints.txt) is generated on Linux to match CI and pins
# torch==*+cpu, so it needs the PyTorch CPU index:
pip install -c constraints.txt -e ".[rag,agents,bi,dev]" \
    --extra-index-url https://download.pytorch.org/whl/cpu
pip check                                  # must report no broken requirements
cp .env.example .env                       # then fill in credentials (if provided)
```
Dependency groups mirror the phased install: `rag`, `agents`, `bi`, `dev`.
`constraints.txt` is the **Linux/CI lock** (CPU torch); the CI workflow uses
exactly the pinned command above. On macOS, prefer the plain range install —
the `+cpu` torch build is Linux-only.

### Infrastructure up / down
```bash
docker compose up -d          # Kafka, Spark, Redis, Weaviate, Kafka UI
docker compose ps             # health
docker compose down           # stop (add -v to also drop volumes)
```

### Database migrations
```bash
# Fresh account/database — run these THREE in order (the order matters):
#   1. snowflake/schema.sql   baseline: DB + schemas + base tables (run once)
#   2. snowflake/rbac.sql     roles + grants — MUST run before step 3, because
#                             migrations V002/V004 grant privileges to these
#                             roles and fail if the roles don't exist yet
#      (then snowflake/rbac_local_example.sql to grant the roles to your user)
#   3. fraud-migrate          incremental VNNN__*.sql (idempotent)
# Apply 1 and 2 as ACCOUNTADMIN in a Snowflake worksheet (or via the connector).
fraud-migrate --dry-run       # show pending migrations, apply nothing
fraud-migrate                 # apply pending VNNN__*.sql, idempotently
# equivalently: python -m fraud_platform.db.migrate
```

### Test, lint, type-check, security — exactly what CI runs
```bash
pytest -q                                              # 196 unit + mocked-adapter tests, no credentials
ruff check .                                           # lint: real-bug rules (F)
mypy fraud_platform/settings.py fraud_platform/db      # type-check (scoped to typed modules)
bandit -c pyproject.toml -r fraud_platform -ll         # security scan (medium+ severity)
pip check                                              # dependency graph consistent
```
CI (`.github/workflows/ci.yml`) runs all of the above on every push/PR,
installing from `constraints.txt`. None of it needs Snowflake/Groq/Redis/Weaviate
— the demos are the only things that touch live services.

### Demos (require live infra + credentials; exercise real services)
```bash
python -m fraud_platform.agents.single_agent.run_demo   # Phase 3 ReAct loop
python -m fraud_platform.agents.multi_agent.run_demo    # Phase 4 orchestration
python -m fraud_platform.governance.run_demo            # Phase 5 decide->tier->persist->review
python -m fraud_platform.observability.eval_runner      # Phase 6 eval + LLM judge
streamlit run fraud_platform/bi_dashboard/streamlit_app.py   # Phase 7 BI dashboard
```
The demos and `eval_runner` intentionally hit real Redis/Snowflake/Weaviate/Groq
— they are not mocked. The unit test suite (`pytest`) needs none of that.

### Pipeline (streaming)
```bash
python -m fraud_platform.data_generator.transaction_stream_generator   # produce to Kafka
python -m fraud_platform.stream_processing.feature_engine              # consume + compute features
```

### Shutdown
```bash
docker compose down           # stop services (keep data)
docker compose down -v        # stop services and delete volumes (Kafka/Redis/Weaviate data)
```

## Notes
- `constraints.txt` is the locked dependency resolution (`pip freeze`); use it
  with `-c` for reproducible installs.
- Snowflake roles are least-privilege per path: BI → `BI_ROLE`, agents →
  `AGENT_ROLE`, pipeline → `PIPELINE_ROLE`, migrations → admin. No application
  path defaults to ACCOUNTADMIN.
