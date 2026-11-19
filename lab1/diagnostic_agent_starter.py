"""
Lab 1 — diagnostic_agent.py  (STARTER)
========================================
Step 4 of Lab 1: Implement a diagnostic agent that reads Prometheus metrics,
computes a confidence score, and calls gate.py.

What this agent does
--------------------
1. Query Prometheus for RED metrics (Rate, Errors, Duration) for payment-service
2. Compute S, H, R, F scores from the metrics
3. Compute composite confidence: C = 0.35×S + 0.25×H + 0.25×R + 0.15×F
4. Map the symptom class to an action proposal
5. Call GateOrchestrator.evaluate(proposal) from gate.py
6. Print the result

Confidence formula components
------------------------------
  S  Signal Clarity      — how clearly metrics show the problem (0–1)
  H  Historical Match    — how often this pattern resolved with this action (0–1)
  R  Remediation Confidence — how reliable the proposed action is (0–1)
  F  Data Freshness      — how recent the telemetry is (0–1)

  C = 0.35×S + 0.25×H + 0.25×R + 0.15×F

Autonomy thresholds
-------------------
  C ≥ 0.90 → L5 (act silently)
  C ≥ 0.80 → L4 (act + notify)
  C ≥ 0.70 → L3 (act + approval)
  C ≥ 0.60 → L2 (suggest)
  C < 0.60 → L1 (escalate)

Run (simulate mode — no Prometheus needed):
    python diagnostic_agent_starter.py --simulate

Run (real mode — requires port-forwarded Prometheus):
    kubectl port-forward -n monitoring svc/prometheus 9090:9090 &
    python diagnostic_agent_starter.py --prometheus-url http://localhost:9090
"""

from __future__ import annotations
import argparse
import math
import sys
from datetime import datetime, timezone

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from gate_starter import GateOrchestrator, GateConfig, ActionProposal, GateResult
except ImportError:  # completed file saved as gate.py
    from gate import GateOrchestrator, GateConfig, ActionProposal, GateResult


SIMULATED_METRICS = {
    "memory_usage_ratio": 0.91,
    "error_rate_ratio": 0.038,
    "p99_latency_ms": 312,
    "p99_baseline_ms": 45,
    "restart_count_1h": 2,
    "last_scrape_age_seconds": 12,
}


class PrometheusClient:
    def __init__(self, base_url: str = "http://localhost:9090"):
        self.base_url = base_url.rstrip("/")

    def query(self, promql: str) -> float | None:
        """
        Execute a PromQL instant query and return the first scalar result.
        Returns None if the query fails or returns no data.

        Hint:
            endpoint = f"{self.base_url}/api/v1/query"
            params = {"query": promql}
            response = requests.get(endpoint, params=params, timeout=5)
            data = response.json()
            # data["data"]["result"] is a list of {metric: {}, value: [timestamp, "value"]}
            # Return float(data["data"]["result"][0]["value"][1])
        """
        if not HAS_REQUESTS:
            return None
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/query", params={"query": promql}, timeout=5
            )
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as e:
            print(f"  [prometheus] query failed: {e}", file=sys.stderr)
            return None
        if data.get("status") != "success":
            print(f"  [prometheus] error: {data.get('error')}", file=sys.stderr)
            return None
        result = data.get("data", {}).get("result", [])
        if not result:
            return None
        try:
            value = float(result[0]["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            return None
        # NaN / Inf happen e.g. on 0/0 when there is no traffic
        if math.isnan(value) or math.isinf(value):
            return None
        return value


# ── MetricsReader ────────────────────────────────────────────


class MetricsReader:
    def __init__(self, client: PrometheusClient, deployment: str, namespace: str):
        self.client     = client
        self.deployment = deployment
        self.namespace  = namespace

    def read(self) -> dict[str, float]:
        """
        Return a dict with these keys:
            memory_usage_ratio        — process_memory_usage_ratio for the deployment
            error_rate_ratio          — rate(http_requests_total{status_code=~"5.."}[2m]) /
                                        rate(http_requests_total[2m])
            p99_latency_ms            — histogram_quantile(0.99, ...) * 1000
            restart_count_1h          — increase(container_restart_total[1h])
            last_scrape_age_seconds   — time() - last scrape timestamp

        PromQL examples:
            memory:   process_memory_usage_ratio{deployment="payment-service"}
            errors:   rate(http_requests_total{deployment="payment-service",status_code=~"5.."}[2m])
                      / rate(http_requests_total{deployment="payment-service"}[2m])
            latency:  histogram_quantile(0.99,
                        rate(http_request_duration_seconds_bucket{deployment="payment-service"}[2m])
                      ) * 1000
            restarts: increase(container_restart_total{deployment="payment-service"}[1h])

        Tip: use round(..., 4) on each value.
        """
        sel = f'deployment="{self.deployment}",namespace="{self.namespace}"'
        queries = {
            # (promql, default if no data)
            "memory_usage_ratio": (
                f"max(process_memory_usage_ratio{{{sel}}})", 0.0),
            "error_rate_ratio": (
                f'sum(rate(http_requests_total{{{sel},status_code=~"5.."}}[2m]))'
                f" / sum(rate(http_requests_total{{{sel}}}[2m]))", 0.0),
            "p99_latency_ms": (
                "histogram_quantile(0.99, sum by (le) ("
                f"rate(http_request_duration_seconds_bucket{{{sel}}}[2m]))) * 1000", 0.0),
            "restart_count_1h": (
                f"sum(increase(container_restart_total{{{sel}}}[1h]))", 0.0),
            # age of the most recent successful scrape of any payment-service target
            "last_scrape_age_seconds": (
                f"time() - max(timestamp(up{{{sel}}} == 1))", 9999.0),
        }
        metrics: dict[str, float] = {}
        for key, (promql, default) in queries.items():
            value = self.client.query(promql)
            if value is None:
                print(f"  [metrics] no data for {key} — using default {default}",
                      file=sys.stderr)
                value = default
            metrics[key] = round(value, 4)
        return metrics


# ── ConfidenceScorer ─────────────────────────────────────────


class ConfidenceScorer:
    HISTORICAL_MATCH = {
        ("MEMORY_PRESSURE",       "pod.rolling_restart"):  0.78,
        ("DB_BOTTLENECK",         "db.connection_reset"):  0.65,
        ("DEPLOYMENT_REGRESSION", "config.rollback"):      0.91,
        ("CAPACITY_EXHAUSTION",   "scale.horizontal_add"): 0.72,
        ("DEPENDENCY_FAILURE",    "circuit.reset"):        0.60,
    }

    REMEDIATION_CONFIDENCE = {
        "pod.rolling_restart":  0.90,
        "db.connection_reset":  0.75,
        "config.rollback":      0.95,
        "scale.horizontal_add": 0.80,
        "circuit.reset":        0.65,
    }

    def compute(
        self,
        metrics: dict[str, float],
        symptom_class: str,
        action_id: str,
    ) -> dict[str, float]:
        """
        Return a dict:
            {
              "S": <signal_clarity>,
              "H": <historical_match>,
              "R": <remediation_confidence>,
              "F": <data_freshness>,
              "C": <composite_score>,
            }

        Scoring guidance
        ----------------
        S (Signal Clarity) — how clearly the metrics show the problem:
            memory_usage_ratio > 0.90 → S = 0.95
            memory_usage_ratio > 0.85 → S = 0.80
            memory_usage_ratio > 0.75 → S = 0.60
            else                      → S = 0.40

        H (Historical Match):
            Look up (symptom_class, action_id) in HISTORICAL_MATCH.
            Default: 0.50 if not found.

        R (Remediation Confidence):
            Look up action_id in REMEDIATION_CONFIDENCE.
            Default: 0.50 if not found.

        F (Data Freshness):
            last_scrape_age_seconds < 30  → F = 0.95
            last_scrape_age_seconds < 60  → F = 0.80
            last_scrape_age_seconds < 120 → F = 0.65
            else                          → F = 0.40

        Composite:
            C = round(0.35*S + 0.25*H + 0.25*R + 0.15*F, 4)
        """
        mem = metrics.get("memory_usage_ratio", 0.0)
        if mem > 0.90:
            S = 0.95
        elif mem > 0.85:
            S = 0.80
        elif mem > 0.75:
            S = 0.60
        else:
            S = 0.40

        H = self.HISTORICAL_MATCH.get((symptom_class, action_id), 0.50)
        R = self.REMEDIATION_CONFIDENCE.get(action_id, 0.50)

        age = metrics.get("last_scrape_age_seconds", float("inf"))
        if age < 30:
            F = 0.95
        elif age < 60:
            F = 0.80
        elif age < 120:
            F = 0.65
        else:
            F = 0.40

        C = round(0.35 * S + 0.25 * H + 0.25 * R + 0.15 * F, 4)
        return {"S": S, "H": H, "R": R, "F": F, "C": C}


# ── Symptom detection ────────────────────────────────────────


def detect_symptom_class(metrics: dict[str, float]) -> str:
    """
    Map metrics to a symptom class.

    Rules (apply in order, return the first match):
      MEMORY_PRESSURE:      memory_usage_ratio > 0.85
      DB_BOTTLENECK:        p99_latency_ms > 500 AND error_rate_ratio > 0.05
      DEPLOYMENT_REGRESSION: error_rate_ratio > 0.10
      CAPACITY_EXHAUSTION:  restart_count_1h > 5
      else:                 "UNKNOWN"

    Returns one of:
      "MEMORY_PRESSURE", "DB_BOTTLENECK", "DEPLOYMENT_REGRESSION",
      "CAPACITY_EXHAUSTION", "UNKNOWN"
    """
    mem      = metrics.get("memory_usage_ratio", 0.0)
    p99      = metrics.get("p99_latency_ms", 0.0)
    errors   = metrics.get("error_rate_ratio", 0.0)
    restarts = metrics.get("restart_count_1h", 0.0)

    if mem > 0.85:
        return "MEMORY_PRESSURE"
    if p99 > 500 and errors > 0.05:
        return "DB_BOTTLENECK"
    if errors > 0.10:
        return "DEPLOYMENT_REGRESSION"
    if restarts > 5:
        return "CAPACITY_EXHAUSTION"
    return "UNKNOWN"


def symptom_to_action(symptom_class: str) -> str:
    return {
        "MEMORY_PRESSURE":       "pod.rolling_restart",
        "DB_BOTTLENECK":         "db.connection_reset",
        "DEPLOYMENT_REGRESSION": "config.rollback",
        "CAPACITY_EXHAUSTION":   "scale.horizontal_add",
    }.get(symptom_class, "escalate_human")


def autonomy_level(c: float) -> str:
    if c >= 0.90: return "L5 — act silently"
    if c >= 0.80: return "L4 — act + notify"
    if c >= 0.70: return "L3 — act + approval"
    if c >= 0.60: return "L2 — suggest"
    return          "L1 — escalate to human"


def run(prometheus_url: str, simulate: bool) -> None:
    deployment = "payment-service"
    namespace  = "default"

    print("\n" + "=" * 65)
    print("LAB 1 — DIAGNOSTIC AGENT")
    print("=" * 65)

    if simulate:
        print("\n[SIMULATE] MEMORY_PRESSURE scenario")
        metrics = SIMULATED_METRICS
    else:
        if not HAS_REQUESTS:
            print("ERROR: 'requests' not installed.")
            sys.exit(1)
        client  = PrometheusClient(prometheus_url)
        reader  = MetricsReader(client, deployment, namespace)
        try:
            metrics = reader.read()
        except NotImplementedError:
            print("\nMetricsReader.read() / PrometheusClient.query() not implemented yet — "
                  "falling back to SIMULATED_METRICS (README Step 3C)")
            metrics = SIMULATED_METRICS

    print(f"\nMetrics for {deployment}:")
    for k, v in metrics.items():
        print(f"  {k:<35} {v}")

    try:
        symptom_class = detect_symptom_class(metrics)
    except NotImplementedError:
        print("\ndetect_symptom_class() not yet implemented")
        return
    action_id     = symptom_to_action(symptom_class)
    print(f"\nSymptom class:   {symptom_class}")
    print(f"Proposed action: {action_id}")

    scorer = ConfidenceScorer()
    try:
        scores = scorer.compute(metrics, symptom_class, action_id)
    except NotImplementedError:
        print("\nConfidenceScorer.compute() not yet implemented")
        return
    print(f"\nConfidence scores:")
    print(f"  S: {scores['S']:.2f}  H: {scores['H']:.2f}  "
          f"R: {scores['R']:.2f}  F: {scores['F']:.2f}")
    print(f"  C = {scores['C']:.4f}  →  {autonomy_level(scores['C'])}")

    proposal = ActionProposal(
        action_id=action_id,
        deployment=deployment,
        namespace=namespace,
        estimated_cost_per_hour_usd=50.0,
        blast_radius_fraction=0.05,
        reversal_time_seconds=120,
        symptom_class=symptom_class,
        confidence_score=scores["C"],
    )

    orchestrator = GateOrchestrator(GateConfig())
    try:
        gate_result = orchestrator.evaluate(proposal)
    except NotImplementedError:
        print("\nGate methods not yet implemented — finish gate_starter.py first")
        return

    print(f"\n{gate_result}")
    for r in gate_result.reasons:
        print(f"  reason: {r}")

    print("\n" + "─" * 65)
    if gate_result.approved and scores["C"] >= 0.60:
        print(f"✅  DECISION: Execute {action_id} on {deployment}")
        print(f"   C={scores['C']:.4f} → {autonomy_level(scores['C'])}")
    elif not gate_result.approved:
        print(f"🚫  DECISION: Block — gate(s) failed: {gate_result.blocked_by}")
    else:
        print(f"🔺  DECISION: Escalate — C={scores['C']:.4f} below 0.60 threshold")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--simulate", action="store_true")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    args = parser.parse_args()
    run(args.prometheus_url, args.simulate)