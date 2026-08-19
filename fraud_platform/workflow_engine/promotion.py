# =============================================================
# PROMOTION — build a workflow plan DETERMINISTICALLY (no LLM)
# =============================================================
# The BI page already holds the SQL the user just saw and approved.
# "Send to Slack" / "Schedule this report" must NOT round-trip through
# the planner and regenerate SQL (requirement #4) — so these builders
# construct the plan in CODE from a stored, already-validated template:
#
#   run_report_query(template)  ->  format_report(title)  ->  connector
#
# The plan is still a normal WorkflowPlan that goes through the SAME
# feasibility check, registry, and executor as an LLM-authored one —
# promotion changes only WHO wrote the plan (code, from a result the
# user saw), never the guardrails it must pass.
# =============================================================

from __future__ import annotations

from typing import Optional

from .planner import PlanStep, PlannedSchedule, WorkflowPlan


class Destination:
    """A validated delivery target. Exactly one connector, with the
    fields that connector needs — so a plan can't be built for a
    half-specified destination."""

    def __init__(self, connector: str, channel: Optional[str] = None,
                 to: Optional[str] = None, subject: Optional[str] = None) -> None:
        connector = (connector or "").lower()
        if connector == "slack":
            if not channel:
                raise ValueError("a Slack destination needs a channel (e.g. #fraud-ops)")
        elif connector == "email":
            if not to:
                raise ValueError("an email destination needs a recipient address")
        else:
            raise ValueError(f"unknown connector {connector!r} (expected 'slack' or 'email')")
        self.connector, self.channel, self.to, self.subject = connector, channel, to, subject

    def to_json(self) -> dict:
        return {"connector": self.connector, "channel": self.channel,
                "to": self.to, "subject": self.subject}

    def notify_step(self, step_number: int, format_step: int) -> PlanStep:
        """The final NOTIFY step, wired to the pre-rendered report text
        from the format_report step (Slack gets the compact form, email
        the detailed body — never raw rows)."""
        if self.connector == "slack":
            return PlanStep(step_number=step_number, tool_name="slack_send_message",
                            args={"channel": self.channel,
                                  "text": f"$step_{format_step}.slack_text"},
                            rationale="deliver the formatted report to Slack")
        return PlanStep(step_number=step_number, tool_name="email_send",
                        args={"to": self.to,
                              "subject": self.subject or f"$step_{format_step}.email_subject",
                              "body": f"$step_{format_step}.email_body"},
                        rationale="deliver the detailed report by email")


def build_report_plan(title: str, report_ref: str, destination: Destination,
                      windowed: bool, schedule: Optional[PlannedSchedule] = None,
                      goal: Optional[str] = None) -> WorkflowPlan:
    """The deterministic promotion plan: run the stored report, format it,
    deliver it. `windowed` decides whether the query step is parameterized
    by $trigger.window_start/$trigger.window_end (scheduled reports) or run
    as-is (immediate one-shot delivery)."""
    run_args: dict = {"report": report_ref}
    if windowed:
        run_args["window_start"] = "$trigger.window_start"
        run_args["window_end"] = "$trigger.window_end"

    steps = [
        PlanStep(step_number=1, tool_name="run_report_query", args=run_args,
                 rationale="run the validated report SQL (not regenerated)"),
        PlanStep(step_number=2, tool_name="format_report",
                 args={"title": title, "data": "$step_1"},
                 rationale="turn rows into a structured, readable report"),
        destination.notify_step(3, format_step=2),
    ]
    return WorkflowPlan(
        goal=goal or title, trigger=None, schedule=schedule, steps=steps,
    )
