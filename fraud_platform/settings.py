# =============================================================
# SETTINGS — one typed, validated configuration for the platform
# =============================================================
# Replaces the old per-phase config.py layering, where each phase's
# config.py loaded the one beneath it by FILE PATH via importlib and
# re-exported its constants. That chain existed only to work around a
# non-package layout: six modules all named `config`, resolved by
# whichever directory happened to be on sys.path. Now that the code
# is a real package, that whole mechanism is gone — there is ONE
# settings module, imported the ordinary way.
#
# Two things this adds over os.getenv sprinkled everywhere:
#   1. TYPES + VALIDATION AT STARTUP. Settings.load() builds a
#      pydantic model from the environment and raises immediately,
#      with a field-named message, if a port isn't an int or a
#      required credential is missing — instead of an AttributeError
#      or a None silently reaching a connector three layers down.
#   2. ROLE-SPECIFIC CREDENTIALS. Each consumer connects under the
#      least-privilege Snowflake role designed for it (Priority 1's
#      finding: the BI path must never inherit ACCOUNTADMIN). The
#      role is resolved per-consumer here, in one auditable place,
#      rather than every module reaching for a single shared role.
# =============================================================

from __future__ import annotations

import os
from functools import lru_cache
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator


class SnowflakeSettings(BaseModel):
    """Connection shared by every consumer; the ROLE differs per
    consumer and is resolved by the role_for() helpers below."""

    account: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: str = "FRAUD_DETECTION"
    warehouse: str = "COMPUTE_WH"

    # Role-specific — least privilege per access domain (rbac.sql).
    # A generic fallback role is kept for the pipeline path only,
    # because PIPELINE_ROLE currently lacks the FEATURES SELECT its
    # startup probe needs (see migrations/V004); the BI and AGENT
    # roles are real, verified, and do NOT fall back to an admin.
    pipeline_role: str = "PIPELINE_ROLE"
    agent_role: str = "AGENT_ROLE"
    bi_role: str = "BI_ROLE"
    migration_role: str = "ACCOUNTADMIN"  # ops tooling, not an app path — DDL/grants need admin

    # Admin roles must never back an application (BI/agent) path — the
    # Priority 1 secondary-roles finding in prose form.
    forbidden_app_roles: frozenset = frozenset(
        {"ACCOUNTADMIN", "ORGADMIN", "SECURITYADMIN", "SYSADMIN"}
    )

    def require_credentials(self) -> "SnowflakeSettings":
        """Fail fast if the connection is unusable. Called by the
        consumers that actually open a Snowflake connection, so a
        pure-unit-test import never trips it."""
        missing = [k for k in ("account", "user", "password") if not getattr(self, k)]
        if missing:
            raise RuntimeError(
                f"Missing Snowflake credentials: {missing}. Set them in .env."
            )
        return self


class RedisSettings(BaseModel):
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    ttl_seconds: int = 86_400


class WeaviateSettings(BaseModel):
    host: str = "localhost"
    http_port: int = 8090
    grpc_port: int = 50051


class LLMSettings(BaseModel):
    groq_api_key: Optional[str] = None
    agent_model: str = "llama-3.3-70b-versatile"
    langsmith_api_key: Optional[str] = None
    langsmith_project: str = "fraud-decisioning-platform"

    def require_groq(self) -> str:
        if not self.groq_api_key:
            raise RuntimeError("GROQ_API_KEY not set. Add it to your .env before running agents.")
        return self.groq_api_key


class Settings(BaseModel):
    """Root settings object. Build with Settings.load()."""

    snowflake: SnowflakeSettings = Field(default_factory=SnowflakeSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    weaviate: WeaviateSettings = Field(default_factory=WeaviateSettings)
    llm: LLMSettings = Field(default_factory=LLMSettings)

    @field_validator("*")
    @classmethod
    def _noop(cls, v):  # keeps pydantic from complaining about extra validators; real checks are per-field types
        return v

    # --- role resolution: one place decides which role each path uses ---
    def bi_connect_role(self) -> str:
        role = os.getenv("BI_SNOWFLAKE_ROLE", self.snowflake.bi_role)
        return role

    def agent_connect_role(self) -> str:
        return os.getenv("AGENT_SNOWFLAKE_ROLE", self.snowflake.agent_role)

    def pipeline_connect_role(self) -> str:
        return os.getenv("PIPELINE_SNOWFLAKE_ROLE", self.snowflake.pipeline_role)

    def migration_connect_role(self) -> str:
        return os.getenv("MIGRATION_SNOWFLAKE_ROLE", self.snowflake.migration_role)

    @classmethod
    def load(cls) -> "Settings":
        """Read the environment (.env included) into a validated
        Settings. Raises on a malformed value (e.g. a non-integer
        port) with a field-named pydantic error."""
        load_dotenv()

        def _int(name: str, default: int) -> int:
            raw = os.getenv(name)
            if raw is None or raw == "":
                return default
            return int(raw)

        return cls(
            snowflake=SnowflakeSettings(
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                database=os.getenv("SNOWFLAKE_DATABASE", "FRAUD_DETECTION"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            ),
            redis=RedisSettings(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=_int("REDIS_PORT", 6379),
                db=_int("REDIS_DB", 0),
            ),
            weaviate=WeaviateSettings(
                host=os.getenv("WEAVIATE_HOST", "localhost"),
                http_port=_int("WEAVIATE_HTTP_PORT", 8090),
                grpc_port=_int("WEAVIATE_GRPC_PORT", 50051),
            ),
            llm=LLMSettings(
                groq_api_key=os.getenv("GROQ_API_KEY"),
                agent_model=os.getenv("AGENT_LLM_MODEL", "llama-3.3-70b-versatile"),
                langsmith_api_key=os.getenv("LANGSMITH_API_KEY"),
                langsmith_project=os.getenv("LANGSMITH_PROJECT", "fraud-decisioning-platform"),
            ),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide singleton. lru_cache means the environment is read
    and validated exactly once, the first time any module asks."""
    return Settings.load()
