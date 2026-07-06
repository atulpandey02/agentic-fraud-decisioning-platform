# Agentic Fraud Decisioning Platform

Real-time fraud decisioning: Kafka + Spark streaming features → Redis online
store / Snowflake audit → RAG-grounded multi-agent decisions → governance &
human-in-the-loop → agentic BI over the decision log.

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
pip install -e ".[rag,agents,bi,dev]"      # all groups, editable
pip install -c constraints.txt -e .        # or: pin to the locked resolution
pip check                                  # must report no broken requirements
cp .env.example .env                       # then fill in credentials (if provided)
```
Dependency groups mirror the phased install: `rag`, `agents`, `bi`, `dev`.

### Infrastructure up / down
```bash
docker compose up -d          # Kafka, Spark, Redis, Weaviate, Kafka UI
docker compose ps             # health
docker compose down           # stop (add -v to also drop volumes)
```

### Database migrations
```bash
# fresh database: run the baseline once, then incremental migrations
#   (apply snowflake/schema.sql in a Snowflake worksheet as the baseline)
fraud-migrate --dry-run       # show pending migrations, apply nothing
fraud-migrate                 # apply pending VNNN__*.sql, idempotently
# equivalently: python -m fraud_platform.db.migrate
```

### Test
```bash
pytest -q                     # 125 unit/contract tests, no credentials needed
```

### Lint
```bash
ruff check .                  # real-bug rules (F): unused imports, undefined names
ruff check . --fix            # auto-fix what it can
```

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
