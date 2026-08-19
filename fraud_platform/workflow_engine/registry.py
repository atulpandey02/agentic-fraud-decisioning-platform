# =============================================================
# TOOL REGISTRY — the workflow engine's tool catalog + dispatch
# =============================================================
# When is a registry worth building? Honestly, not at five tools —
# a static dict binding would be just as safe and simpler, and this
# header says so out loud. A registry earns its complexity when the
# tool count grows past what a human can hold in their head and the
# LLM needs to DISCOVER tools (search) rather than be handed all of
# them. We build the INTERFACE correctly here (search + metadata +
# a single guarded execute path) so the scale-up is a data change,
# not a rewrite — while being honest that at this size it is a
# demonstration of the pattern, not a necessity.
#
# The load-bearing invariant: execution NEVER dispatches a tool
# that isn't registered. An unknown tool in a plan is a FEASIBILITY
# failure caught in code (feasibility.py), never a runtime guess —
# the same "invariants from code, judgment from models" split the
# rest of the platform uses.
# =============================================================

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Tool categories. There are deliberately NO write/destructive
# categories — the feasibility check (config.ALLOWED_TOOL_CATEGORIES)
# rejects anything outside this set, so a future write tool fails
# safe until the policy is consciously widened.
READ_DATA = "READ_DATA"   # pulls existing data (features, history, decisions)
ANALYZE = "ANALYZE"       # deterministic computation over data (counts, conditions)
NOTIFY = "NOTIFY"         # outbound side effect via an auditable connector (Slack/email)


@dataclass(frozen=True)
class ToolSpec:
    """One registered capability. `execute(**args)` is the callable the
    executor invokes AFTER the registry has validated the args against
    `args_schema` — so a tool body never sees malformed input."""

    name: str
    description: str
    args_schema: type[BaseModel]
    category: str
    execute: Callable[..., object]
    read_only: bool = True
    requires_approval: bool = False


class ToolRegistry:
    """Register, search, fetch, and (guarded) execute ToolSpecs."""

    def __init__(self) -> None:
        self._tools: dict[str, ToolSpec] = {}

    def register(self, spec: ToolSpec) -> ToolSpec:
        if spec.name in self._tools:
            raise ValueError(f"tool {spec.name!r} is already registered")
        self._tools[spec.name] = spec
        logger.debug("registered tool %s (%s)", spec.name, spec.category)
        return spec

    def get(self, name: str) -> Optional[ToolSpec]:
        return self._tools.get(name)

    def all(self) -> list[ToolSpec]:
        return list(self._tools.values())

    def names(self) -> set[str]:
        return set(self._tools)

    def search(self, query: str) -> list[ToolSpec]:
        """Keyword match over name + description, ranked by how many
        query terms hit. Deliberately NOT embeddings: at this tool
        count keyword search is transparent and dependency-free. The
        scale-up path is to swap this body for a vector search over
        the same ToolSpec metadata — the interface does not change."""
        terms = [t for t in query.lower().split() if t]
        if not terms:
            return self.all()
        scored: list[tuple[int, ToolSpec]] = []
        for spec in self._tools.values():
            hay = f"{spec.name} {spec.description}".lower()
            score = sum(1 for t in terms if t in hay)
            if score:
                scored.append((score, spec))
        return [s for _, s in sorted(scored, key=lambda x: (-x[0], x[1].name))]

    def execute(self, name: str, args: dict) -> object:
        """The ONE dispatch path. Refuses an unregistered tool (an
        unknown tool is never a runtime guess) and validates args
        against the tool's schema before the body runs."""
        spec = self.get(name)
        if spec is None:
            raise KeyError(f"unknown tool {name!r} — not in registry")
        validated = spec.args_schema(**args)          # raises on bad args
        return spec.execute(**validated.model_dump())

    def listing_for_prompt(self) -> str:
        """The exact tool listing injected into the planner prompt.
        The planner may use ONLY the tools that appear here — the
        registry is the single source of truth for what is callable,
        for both the model (this listing) and the code (execute)."""
        lines = []
        for spec in sorted(self._tools.values(), key=lambda s: s.name):
            fields = ", ".join(spec.args_schema.model_fields)
            lines.append(
                f"- {spec.name}({fields}) [{spec.category}] — {spec.description}"
            )
        return "\n".join(lines)
