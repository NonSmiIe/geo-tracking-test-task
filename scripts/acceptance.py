import re
from statistics import median

POLICY = "operational-v3"


def assess_workload(generated: dict) -> dict:
    counts = generated["counters"]
    scheduled = counts.get("scheduled", 0)
    lag = generated["schedule_lag"]
    late_fraction = counts.get("late_over_100ms", 0) / max(1, scheduled)
    offered = generated["offered_reports_per_second"]
    checks = {
        "exact_population": scheduled > 0
        and counts.get("expected_scheduled")
        == scheduled
        == counts.get("sent")
        == counts.get("acked"),
        "no_loss_or_overload": not any(
            counts.get(key, 0)
            for key in ("generator_dropped", "rejected", "transport_errors", "http_503")
        ),
        "sustained_rate": generated["acked_reports_per_second"] >= 0.99 * offered,
        "scheduling_p99": lag.get("p99_ms", 100000) <= 100,
        "scheduling_tail_fraction": late_fraction <= 0.001,
        "scheduling_max": lag.get("max_ms", 100000) < 1000,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "late_fraction": late_fraction,
        "minimum_acked_rate": 0.99 * offered,
    }


def assess_pipeline(acked: int, baseline: dict, final: dict) -> dict:
    def total(snapshot: dict, role: str, name: str) -> float:
        return snapshot["roles"].get(role, {}).get(name, 0)

    committed = total(final, "processor", "reports_committed") - total(
        baseline, "processor", "reports_committed"
    )
    evicted = total(final, "api", "slow_connections_closed") - total(
        baseline, "api", "slow_connections_closed"
    )
    failed = total(final, "processor", "batches_failed") - total(
        baseline, "processor", "batches_failed"
    )
    checks = {
        "every_acked_report_committed": committed == acked,
        "consumer_lag_drained": total(final, "processor", "consumer_lag") == 0,
        "no_session_evicted": evicted == 0,
        "no_failed_batches": failed == 0,
    }
    return {"passed": all(checks.values()), "checks": checks, "committed": committed}


def memory_bytes(value: str) -> float:
    amount, unit = re.fullmatch(r"([\d.]+)(B|KiB|MiB|GiB|TiB)", value.strip()).groups()
    scale = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
    return float(amount) * scale[unit]


def assess_resources(samples: list[dict]) -> dict:
    series: dict[str, list[tuple[float, float, float]]] = {}
    for sample in samples:
        for container in sample.get("containers", []):
            used, limit = container["MemUsage"].split("/")
            series.setdefault(container["Name"], []).append(
                (memory_bytes(used), memory_bytes(limit), float(container["CPUPerc"].rstrip("%")))
            )
    checks = {"samples_present": bool(series) and all(len(rows) >= 4 for rows in series.values())}
    details = {}
    for name, rows in series.items():
        quarter = max(1, len(rows) // 4)
        first = median(used for used, _, _ in rows[quarter : 2 * quarter])
        last = median(used for used, _, _ in rows[-quarter:])
        checks[name + "_memory_headroom"] = all(used <= limit * 0.9 for used, limit, _ in rows)
        checks[name + "_memory_stable"] = last - first <= 64 * 1024**2
        details[name] = {
            "peak_bytes": max(used for used, _, _ in rows),
            "limit_bytes": rows[0][1],
            "growth_bytes": last - first,
            "cpu_median_percent": median(cpu for _, _, cpu in rows),
            "cpu_peak_percent": max(cpu for _, _, cpu in rows),
        }
    return {"passed": all(checks.values()), "checks": checks, "containers": details}
