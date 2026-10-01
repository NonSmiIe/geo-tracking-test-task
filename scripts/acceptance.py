import re
from statistics import median


def assess_workload(generated, devices, interval):
    counts = generated["counters"]
    scheduled = counts.get("scheduled", 0)
    lag = generated["schedule_lag"]
    late_fraction = counts.get("late_over_100ms", 0) / max(1, scheduled)
    checks = {
        "exact_population": scheduled > 0
        and counts.get("expected_scheduled") == scheduled == counts.get("accepted"),
        "no_loss_or_overload": not any(
            counts.get(key, 0)
            for key in ("generator_dropped", "rejected", "failed", "transport_errors", "http_503")
        ),
        "sustained_rate": generated["accepted_reports_per_second"] >= 0.99 * devices / interval,
        "scheduling_p99": lag.get("p99_ms", 100000) <= 100,
        "scheduling_tail_fraction": late_fraction <= 0.001,
        "scheduling_max": lag.get("max_ms", 100000) < 1000,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "late_fraction": late_fraction,
        "minimum_accepted_rate": 0.99 * devices / interval,
    }


def memory_bytes(value):
    amount, unit = re.fullmatch(r"([\d.]+)(B|KiB|MiB|GiB|TiB)", value.strip()).groups()
    return (
        float(amount) * {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}[unit]
    )


def assess_resources(samples, final, memory_required):
    readings = samples + [final]
    checks = {
        "report_backlog_bounded": all(reading["pending_reports"] <= 4096 for reading in readings)
        and final["counters"].get("ingress_high_water", 0) <= 4096,
        "pool_bounded": all(
            reading["database_connections_checked_out"] <= 5 for reading in readings
        ),
        "sessions_bounded": all(reading["connections"] <= 128 for reading in readings),
    }
    resources = {}
    for reading in samples:
        for container in reading.get("resources", []):
            used, limit = container["MemUsage"].split("/")
            resources.setdefault(container["Name"], []).append(
                (memory_bytes(used), memory_bytes(limit))
            )
    details = {}
    if memory_required:
        checks["memory_samples_present"] = len(resources) == 2 and all(
            len(rows) >= 2 for rows in resources.values()
        )
    for name, rows in resources.items():
        quarter = max(1, len(rows) // 4)
        first = median(used for used, _ in rows[:quarter])
        last = median(used for used, _ in rows[-quarter:])
        checks[name + "_memory_headroom"] = all(used <= limit * 0.9 for used, limit in rows)
        checks[name + "_memory_stable"] = last - first <= 32 * 1024**2
        details[name] = {
            "peak_bytes": max(used for used, _ in rows),
            "first_quarter_median_bytes": first,
            "last_quarter_median_bytes": last,
            "growth_bytes": last - first,
        }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "memory_measured": bool(resources),
        "containers": details,
    }
