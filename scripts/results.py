import argparse
import statistics
import sys
from pathlib import Path

import orjson

ROOT = Path(__file__).resolve().parent.parent
LADDER = ROOT / "evidence" / "ladder.json"
README = ROOT / "README.md"
START, END = "<!-- results:start -->", "<!-- results:end -->"
HEADER = (
    "| Run | Build | Reports/s | Acknowledged | Position delivery p50 / p95 / p99 "
    "| api CPU | PostgreSQL | Processors | Verdict |\n"
    "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
)


def latency(value: float) -> str:
    return f"{value / 1000:.1f} s" if value >= 10_000 else f"{value:,.0f} ms"


def cpu(containers: dict, role: str) -> list[float]:
    return [
        usage["cpu_median_percent"]
        for name, usage in sorted(containers.items())
        if name.rsplit("-", 1)[0].endswith(f"-{role}")
    ]


def share(values: list[float]) -> str:
    if not values:
        return "—"
    if len(values) == 1:
        return f"{values[0]:.0f}%"
    return f"{len(values)} × {statistics.fmean(values):.0f}%"


def row(entry: dict) -> str:
    run = orjson.loads((ROOT / entry["file"]).read_bytes())
    configuration = run["generator"]["configuration"]
    counters = run["generator"]["counters"]
    positions = run["delivery_latency"]["positions"]
    containers = run["resources"]["containers"]
    name = f"{configuration['devices']:,} · {configuration['duration']:.0f} s"
    if entry["label"]:
        name += f", {entry['label']}"
    verdict = "passed" if run["acceptance_passed"] else "failed"
    if not run["acceptance_passed"] and entry["reason"]:
        verdict += f": {entry['reason']}"
    database = cpu(containers, "db")
    return (
        f"| {name} | {run.get('commit', '—')} | {run['generator']['acked_reports_per_second']:,.0f}"
        f" | {counters['acked']:,} / {counters['scheduled']:,}"
        f" | {latency(positions['p50_ms'])} / {latency(positions['p95_ms'])}"
        f" / {latency(positions['p99_ms'])}"
        f" | {share(cpu(containers, 'api'))} | {share(database)}"
        f" | {share(cpu(containers, 'processor'))} | [{verdict}]({entry['file']}) |"
    )


def table() -> str:
    entries = orjson.loads(LADDER.read_bytes())
    started = {
        entry["file"]: orjson.loads((ROOT / entry["file"]).read_bytes())["started_at"]
        for entry in entries
    }
    entries.sort(key=lambda entry: started[entry["file"]])
    return "\n".join([HEADER, *map(row, entries)])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    text = README.read_text()
    head, rest = text.split(START, 1)
    _, tail = rest.split(END, 1)
    rendered = f"{head}{START}\n{table()}\n{END}{tail}"
    if args.check:
        if rendered != text:
            sys.exit("README results table differs from evidence; run scripts/results.py")
        return
    README.write_text(rendered)


if __name__ == "__main__":
    main()
