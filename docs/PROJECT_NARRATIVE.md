# PROJECT NARRATIVE — The Process Behind the Platform

*This document exists for one specific purpose: to give an accurate record of
HOW this project was actually built — the decisions, the corrections, the
dead ends, and the collaboration pattern between a human, a conversational
AI assistant (Claude, in chat), and an autonomous coding agent (Claude Code)
— separate from what the code itself documents. Nothing in here should be
invented or embellished when this is turned into a website. If a section
below doesn't give enough detail to write about confidently, say so rather
than filling the gap with a plausible-sounding guess.*

---

## The core meta-story: an agent was used to help build an agent system

This project is a fraud decisioning platform whose whole point is an AI
agent (built with LangGraph) that reasons over transaction data and makes
ALLOW/BLOCK/ESCALATE decisions. Partway through building it, the human
collaborator (Atul) started using Claude Code — a separate, autonomous
coding agent — to accelerate building Phases 4 through 7. This means the
project itself became a live example of directing an AI agent to build part
of an AI agent system, and observing how well that agent's own self-reports
could be trusted.

The working pattern that emerged, across many rounds:

1. Scope a specific, bounded task for Claude Code (never "finish everything")
2. Claude Code builds, tests, and produces a written report claiming results
3. The report gets read CRITICALLY, not accepted at face value — specific
   numbers get cross-checked, vague claims get pushed back on, "done" gets
   distinguished from "smoke-tested"
4. Corrections and follow-up scoping get sent back as a new, bounded prompt
5. Repeat, with an explicit stop-and-report checkpoint after every stage

This pattern is the actual subject worth explaining on the website — not
just "here's what got built" but "here's what it looks like to supervise an
autonomous agent responsibly."

---

## Real moments worth featuring (each of these actually happened)

### 1. The security bug that was found TWICE, on two different code paths

During a security-hardening pass, Claude Code was asked to fix an issue with
`BI_ROLE` in the BI dashboard: even though the code connected using
`role="BI_ROLE"`, Snowflake was still allowing reads of a restricted schema.
The root cause: Snowflake activates ALL of a user's granted roles as
"secondary roles" by default, not just the one requested as primary — so the
human's own `ACCOUNTADMIN` grant was silently riding along and overriding the
restriction. The fix was setting secondary roles to NONE explicitly, not
just picking the right primary role.

The genuinely interesting part: two rounds later, during a different
priority (schema and processing correctness), Claude Code's own testing
independently discovered the EXACT SAME vulnerability class on a completely
different code path — `PIPELINE_ROLE` was able to read a schema
(`DECISIONS`) it had no grant on, via the same secondary-roles mechanism.
This was flagged as a pattern worth generalizing: once one instance of a bug
class is found, actively check other similar code paths for the same
mistake rather than assuming it was a one-off.

### 2. Three wrong dependency version guesses in a row, and what changed

While setting up the LangGraph-based agent, three consecutive attempts to
pin exact dependency versions (`langgraph`, `langchain-groq`, `langsmith`)
were wrong — one specified a version that didn't exist at all, one directly
contradicted what a dependency actually required. Rather than guess a
fourth time, the fix was to stop pinning speculatively and either let pip's
own resolver pick compatible versions, or read pip's actual error output
(which lists every real available version) instead of guessing blind.
Later, Claude Code's own independent audit found a DIFFERENT dependency
problem in the same area — a lockfile generated on macOS wasn't portable to
a Linux CI environment, breaking a fresh install even though local tests
passed. The lesson generalized: "works on my machine" is not the same
claim as "works in a clean environment," and both need to be tested
separately.

### 3. The geo-flagging investigation — a three-part root cause, not one bug

Early fraud-scoring logic flagged 44% of transactions for review against
only 15% actually being synthetic fraud — a 3x over-flagging rate. Rather
than just loosening a threshold, an investigation was required BEFORE any
fix was allowed, and it surfaced three distinct causes stacked together:
(1) the rule checked raw distance instead of the documented "implied travel
speed" concept the system was actually designed around, (2) out-of-order
data replay could compare a transaction against a location that was
chronologically in the future, producing nonsense results, (3) one
fraudulent transaction's location could "poison" a user's baseline,
causing a chain of unrelated false flags afterward. The fix addressed all
three, improving (not fully solving) the flag rate to 26.6% — and the
report was required to describe this honestly as "meaningfully improved,
still over-flagging," not as "fixed."

### 4. An independent audit contradicted an earlier "approved as complete" status

A previously-approved security item ("define redaction and access controls
for traces and prompts") had been satisfied by writing a POLICY DOCUMENT
describing what should be redacted — not by writing code that actually
redacts anything. A separate, independent read-only audit later caught
this gap directly: real user names and locations were being persisted
into audit trace storage completely unredacted. This became the clearest
example in the whole project of a claim being technically true in a
narrow sense (a document exists) while being false in the sense that
actually mattered (no protection existed in the running system).

### 5. A misleading "clean" metric caught before it could be repeated as fact

During a partial evaluation run that hit an external API rate limit, the
salvaged results reported "precision 1.0" alongside "recall 0.50" as if
both were equally meaningful. On inspection, the sample used to calculate
that "precision 1.0" contained ZERO legitimate transactions — meaning
there was no opportunity to generate a false positive at all, so the
number was mathematically undefined, not genuinely perfect. This was
caught and required to be relabeled rather than reported as a real result.

---

## Design decisions made through reasoning, not convention

- **ReAct over Plan-and-Execute or Reflection** for the single agent,
  specifically because the number of tool calls needed genuinely varies per
  transaction (a clear-cut case needs no history lookup; a borderline one
  needs all three tools) — Plan-and-Execute's fixed pipeline can't adapt to
  that without wasting latency on easy cases.
- **LangGraph over CrewAI**, specifically because of first-class support for
  cycles (ReAct's repeated loop) and node-level human-in-the-loop interrupts
  (needed for the governance/approval tiers) — not because CrewAI is worse,
  but because those two specific requirements sit in LangGraph's strengths.
- **Weaviate over Pinecone** for the retrieval layer, after verifying (not
  assuming) that Pinecone's free tier DID support hybrid search, but that
  Weaviate's score fusion was native while Pinecone's needed manual
  rebalancing — a real, verified technical reason, not just a hunch.
- **A2A protocol deliberately NOT adopted**, despite being a real, current
  standard, because it solves cross-organization agent interoperability and
  every agent in this system is internally owned, one team, one framework —
  there was no real problem for it to solve yet.
- **A Factory pattern deliberately NOT added**, for the same class of reason
  — it only earns its place when there's a genuine branching decision about
  which class to instantiate, and none existed at the time it was considered.

---

## Honest caveats for the website to preserve

- This is a learning and portfolio project built on synthetic data, not a
  deployed commercial fraud system.
- Evaluation sample sizes have been small (as few as 6 transactions in one
  round) — real statistical confidence has NOT yet been established, and
  the website should not imply otherwise.
- Not everything Claude Code reported as "done" survived scrutiny on first
  pass — several rounds required corrections after independent verification
  found gaps. This is a feature of the process worth showing honestly, not
  a flaw to hide.
