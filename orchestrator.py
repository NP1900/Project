import json
import time
import logging
import importlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)


def load_config() -> dict:
    return json.loads(Path("config/pipelines.json").read_text(encoding="utf-8"))


def run_agent(company_cfg: dict) -> tuple[str, list[dict], str | None]:
    try:
        module = importlib.import_module(company_cfg["agent"])
        agent_class = next(
            v for k, v in vars(module).items()
            if isinstance(v, type) and k.endswith("Agent") and k != "BasePipelineAgent"
        )
        active_pipes = [p for p in company_cfg["pipelines"] if p.get("active")]
        agent = agent_class(
            company_id=company_cfg["id"],
            pipelines=active_pipes,
            cache_path=company_cfg["cache_file"],
        )
        events = agent.scrape()
        return (company_cfg["id"], events, None)
    except BaseException as e:
        logging.error(f"Agent {company_cfg['id']} failed: {e}", exc_info=True)
        return (company_cfg["id"], [], str(e))


def main():
    start = time.time()
    config = load_config()
    enabled = [c for c in config["companies"] if c.get("enabled")]

    Path("data").mkdir(exist_ok=True)

    results = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(run_agent, c): c["id"] for c in enabled}
        for future in as_completed(futures, timeout=120):
            cid = futures[future]
            try:
                cid, events, error = future.result()
            except BaseException as e:
                logging.error(f"Agent {cid} future raised: {e}")
                events, error = [], str(e)
            results[cid] = {"events": events or [], "error": error}
            logging.info(f"{cid}: {len(events or [])} events, error={error}")

    # Merge and deduplicate
    all_events = []
    for r in results.values():
        all_events.extend(r["events"])

    seen = set()
    deduped = []
    for ev in all_events:
        key = (ev.get("pipe", ""), str(ev.get("notice_id", "")))
        if key not in seen:
            seen.add(key)
            deduped.append(ev)

    # Filter truly expired events (end > 2 hours ago)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=2)

    def is_expired(ev):
        end = ev.get("end")
        if not end:
            return False
        try:
            end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
            return end_dt < cutoff
        except Exception:
            return False

    active = [ev for ev in deduped if not is_expired(ev)]

    # Sort by severity asc (HIGH first) then posted_at desc
    sev_order = {"HIGH": 0, "MED": 1, "LOW": 2}
    active.sort(
        key=lambda e: (
            sev_order.get(e.get("severity", "LOW"), 2),
            e.get("posted_at", ""),
        )
    )

    # Atomic write events.json
    tmp = Path("data/events.json.tmp")
    tmp.write_text(json.dumps(active, indent=2, default=str), encoding="utf-8")
    tmp.rename("data/events.json")

    # Write status.json
    duration = round(time.time() - start, 1)
    status = {
        "last_run": now.isoformat(),
        "duration_seconds": duration,
        "total_events": len(active),
        "companies": {
            cid: {
                "status": "error" if r["error"] else "ok",
                "event_count": len(r["events"]),
                "error": r["error"],
            }
            for cid, r in results.items()
        },
    }
    Path("data/status.json").write_text(
        json.dumps(status, indent=2), encoding="utf-8"
    )

    logging.info(f"Done: {len(active)} events in {duration}s")

    # Print summary for local testing
    print(f"\n{'='*60}")
    print(f"SCRAPER RESULTS — {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*60}")
    print(f"Total active events: {len(active)}")
    print(f"Runtime: {duration}s")
    print()
    for cid, r in sorted(results.items()):
        status_str = "ERROR" if r["error"] else "OK"
        print(f"  [{status_str}] {cid}: {len(r['events'])} events", end="")
        if r["error"]:
            print(f" — {r['error'][:80]}")
        else:
            print()
    print()

    sev_counts = {"HIGH": 0, "MED": 0, "LOW": 0}
    for ev in active:
        s = ev.get("severity", "LOW")
        sev_counts[s] = sev_counts.get(s, 0) + 1
    print(f"Severity: HIGH={sev_counts['HIGH']}  MED={sev_counts['MED']}  LOW={sev_counts['LOW']}")

    pdf_count = sum(1 for ev in active if ev.get("source") in ("pdf_outage", "cgt_maint_timeline"))
    print(f"PDF events: {pdf_count}")

    if active:
        print(f"\nFirst 3 events:")
        for ev in active[:3]:
            print(f"  [{ev.get('severity','?')}] {ev.get('pipe','?')} | {ev.get('title','')[:70]}")
    print()


if __name__ == "__main__":
    main()
