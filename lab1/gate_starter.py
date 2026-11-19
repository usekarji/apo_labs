"""
Lab 1 — gate.py  (STARTER)
===========================
Implement the four confidence gates that guard every autonomous action.

Each gate is an AND condition — ALL four must pass before APO executes.
If any gate returns False, the action is blocked and routed to a human.

Gates
-----
1. Cost gate       — estimated action cost must be below the threshold
2. Blast radius    — fraction of pods affected must be below the max
3. Reversibility   — action must be undoable within the time limit
4. Time gate       — block during peak hours, month-end, deploy freeze

Thresholds (lab environment)
-----------------------------
  cost_per_hour_usd_max  : 500          (prod: 1000)
  blast_radius_max       : 0.10         (prod: 0.05)
  reversal_time_sec_max  : 600          (prod: 300)
  peak_hours             : 08–10 UTC    (morning peak window)

Run (simulate mode — no k3d needed):
    python gate_starter.py --simulate

Run (real mode — requires Prometheus and k3d):
    python gate_starter.py
"""

from __future__ import annotations
import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class ActionProposal:
    action_id: str
    deployment: str
    namespace: str
    estimated_cost_per_hour_usd: float
    blast_radius_fraction: float
    reversal_time_seconds: int
    symptom_class: str
    confidence_score: float


@dataclass
class GateResult:
    approved: bool
    blocked_by: list[str] = field(default_factory=list)
    reasons: list[str]    = field(default_factory=list)

    def __str__(self) -> str:
        if self.approved:
            return "✅  GateResult(approved=True)"
        return f"🚫  GateResult(approved=False, blocked_by={self.blocked_by})"


@dataclass
class GateConfig:
    cost_per_hour_usd_max: float = 500.0
    blast_radius_max: float      = 0.10
    reversal_time_sec_max: int   = 600
    peak_hours_utc: tuple        = (8, 10)   # block 08:00–10:00 UTC


class GateOrchestrator:

    def __init__(self, config: GateConfig | None = None, clock=None):
        self.config = config or GateConfig()
        # clock: optional callable returning an aware UTC datetime (for tests)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _now_utc(self) -> datetime:
        return self._clock()

    def evaluate(self, proposal: ActionProposal) -> GateResult:
        blocked_by: list[str] = []
        reasons: list[str]    = []

        gates = [
            ("cost_gate",        self._cost_gate),
            ("blast_radius_gate", self._blast_radius_gate),
            ("reversibility_gate", self._reversibility_gate),
            ("time_gate",         self._time_gate),
        ]

        for name, gate_fn in gates:
            passed, reason = gate_fn(proposal)
            if not passed:
                blocked_by.append(name)
                reasons.append(reason)

        return GateResult(
            approved=len(blocked_by) == 0,
            blocked_by=blocked_by,
            reasons=reasons,
        )

    def _cost_gate(self, proposal: ActionProposal) -> tuple[bool, str]:
        """
        PASS if proposal.estimated_cost_per_hour_usd <= config.cost_per_hour_usd_max.
        Return (passed: bool, reason: str).

        Hint: reason string should include the actual value and the threshold.
        Example: "cost $1200/hr exceeds limit $500/hr"
        """
        cost = proposal.estimated_cost_per_hour_usd
        limit = self.config.cost_per_hour_usd_max
        if cost <= limit:
            return True, f"cost ${cost:,.0f}/hr within limit ${limit:,.0f}/hr"
        return False, f"cost ${cost:,.0f}/hr exceeds limit ${limit:,.0f}/hr"

    def _blast_radius_gate(self, proposal: ActionProposal) -> tuple[bool, str]:
        """
        PASS if proposal.blast_radius_fraction <= config.blast_radius_max.
        Return (passed: bool, reason: str).

        Example: "blast radius 15% exceeds limit 10%"
        """
        radius = proposal.blast_radius_fraction
        limit = self.config.blast_radius_max
        if radius <= limit:
            return True, f"blast radius {radius:.0%} within limit {limit:.0%}"
        return False, f"blast radius {radius:.0%} exceeds limit {limit:.0%}"

    def _reversibility_gate(self, proposal: ActionProposal) -> tuple[bool, str]:
        """
        PASS if proposal.reversal_time_seconds <= config.reversal_time_sec_max.
        Return (passed: bool, reason: str).

        Example: "reversal time 900s exceeds limit 600s"
        """
        rt = proposal.reversal_time_seconds
        limit = self.config.reversal_time_sec_max
        if rt <= limit:
            return True, f"reversal time {rt}s within limit {limit}s"
        return False, f"reversal time {rt}s exceeds limit {limit}s"

    def _time_gate(self, proposal: ActionProposal) -> tuple[bool, str]:
        """
        PASS if the current UTC hour is NOT within peak_hours_utc range.
        Return (passed: bool, reason: str).

        config.peak_hours_utc is a (start_hour, end_hour) tuple (exclusive end).
        BLOCK if: start_hour <= current_utc_hour < end_hour

        Example: "action blocked during peak hours (08:00–10:00 UTC)"

        Tip: use datetime.now(timezone.utc).hour to get the current hour.
        """
        start, end = self.config.peak_hours_utc
        hour = self._now_utc().hour
        window = f"{start:02d}:00–{end:02d}:00 UTC"
        if start <= hour < end:
            return False, f"action blocked during peak hours ({window})"
        return True, f"current hour {hour:02d}:00 UTC outside peak hours ({window})"


# ── Test harness ──────────────────────────────────────────────────────────────

def _run_tests(config: GateConfig) -> None:
    orchestrator = GateOrchestrator(config)

    test_cases = [
        # (description, proposal, should_approve)
        (
            "Normal pod restart — should PASS all gates",
            ActionProposal(
                action_id="pod.rolling_restart",
                deployment="payment-service",
                namespace="default",
                estimated_cost_per_hour_usd=50.0,
                blast_radius_fraction=0.05,
                reversal_time_seconds=120,
                symptom_class="MEMORY_PRESSURE",
                confidence_score=0.84,
            ),
            True,
        ),
        (
            "Cost gate FAIL — $1200/hr exceeds $500 limit",
            ActionProposal(
                action_id="scale.horizontal_add",
                deployment="payment-service",
                namespace="default",
                estimated_cost_per_hour_usd=1200.0,
                blast_radius_fraction=0.05,
                reversal_time_seconds=120,
                symptom_class="CAPACITY_EXHAUSTION",
                confidence_score=0.78,
            ),
            False,
        ),
        (
            "Blast radius FAIL — 15% > 10% lab limit",
            ActionProposal(
                action_id="pod.rolling_restart",
                deployment="payment-service",
                namespace="default",
                estimated_cost_per_hour_usd=50.0,
                blast_radius_fraction=0.15,
                reversal_time_seconds=120,
                symptom_class="MEMORY_PRESSURE",
                confidence_score=0.84,
            ),
            False,
        ),
        (
            "Reversibility FAIL — 900s > 600s limit",
            ActionProposal(
                action_id="config.rollback",
                deployment="payment-service",
                namespace="default",
                estimated_cost_per_hour_usd=50.0,
                blast_radius_fraction=0.05,
                reversal_time_seconds=900,
                symptom_class="DEPLOYMENT_REGRESSION",
                confidence_score=0.76,
            ),
            False,
        ),
    ]

    print("\n" + "=" * 65)
    print("LAB 1 — GATE TESTS")
    print("=" * 65)

    passed = 0
    for desc, proposal, expected_approve in test_cases:
        try:
            result = orchestrator.evaluate(proposal)
            ok = result.approved == expected_approve
            status = "✅ PASS" if ok else "❌ FAIL"
            if ok:
                passed += 1
            print(f"\n{status}  {desc}")
            print(f"       approved={result.approved}  blocked_by={result.blocked_by}")
            if result.reasons:
                for r in result.reasons:
                    print(f"       reason: {r}")
        except NotImplementedError as e:
            print(f"\n⬜  NOT IMPLEMENTED — {desc}")
            print(f"   ({e})")

    print("\n" + "─" * 65)
    print(f"Results: {passed}/{len(test_cases)} tests passed")
    print()
    if passed == len(test_cases):
        print("All gates implemented correctly. Proceed to Step 3 in the lab README.")
    else:
        print("Some gates failed or are not implemented. Review the TODO comments above.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--simulate", action="store_true")
    args = parser.parse_args()
    _run_tests(GateConfig())
