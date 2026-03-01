from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests

from app.db import BLOCKED_DOMAIN_SUFFIXES, finish_discovery_run, get_latest_running_discovery_run, init_db, start_discovery_run, upsert_org

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
PLACE_FAMILIES = [
    "independent art centres",
    "contemporary galleries",
    "photography galleries",
    "architecture talk venues",
    "science museums with talks",
    "public lecture venues",
    "literary venues",
    "bookshops with events",
    "cinematheques",
    "independent cinemas",
    "makerspaces",
    "craft workshops",
    "country cultural centres",
    "diaspora arts centres",
    "community arts venues",
    "parks with events",
    "gardens with evening events",
    "small theatres",
    "live music venues",
    "performance spaces",
    "universities with public lectures",
    "learned societies with talks",
]
ONE_OFF_FAMILIES = ["food festivals", "film festivals", "design festivals", "music festivals", "literary festivals", "charity runs", "5K runs", "10K runs", "half marathons"]
AREAS = ["Central London", "East London", "South London", "North London", "West London", "Southbank", "Greenwich", "Camden", "Hackney", "Shoreditch", "Kensington", "Westminster"]
GENERIC_NAME_SNIPPETS = ("what's on", "whats on", "book your tickets", "subscribe", "support", "overview", "events listings")
ORG_TYPES = {"bookshop", "cinema", "gallery", "live_music_venue", "theatre", "museum", "makers_space", "park", "garden", "cultural_centre", "university", "learned_society", "promoter", "festival", "organisation"}
ORG_TYPE_ALIASES = {
    "live music venue": "live_music_venue",
    "makerspace": "makers_space",
    "makers space": "makers_space",
    "maker space": "makers_space",
    "cultural center": "cultural_centre",
    "learned society": "learned_society",
    "one-off event": "festival",
    "one off event": "festival",
    "one_off_event": "festival",
}


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _token(value: Any) -> str:
    t = _clean(value).lower()
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _canonical_url(value: Any) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except Exception:
        return ""
    if parsed.scheme not in {"http", "https"}:
        return ""
    host = (parsed.netloc or "").lower().replace("www.", "")
    if not host:
        return ""
    path = (parsed.path or "/").rstrip("/") or "/"
    return f"{parsed.scheme}://{host}{path}"


def _domain(url: Any) -> str:
    canonical = _canonical_url(url)
    return (urlparse(canonical).netloc or "").lower().replace("www.", "") if canonical else ""


def _blocked_domain(host: str) -> bool:
    h = _clean(host).lower().replace("www.", "")
    return bool(h) and any(h == suffix or h.endswith(f".{suffix}") for suffix in BLOCKED_DOMAIN_SUFFIXES)


def _normalize_org_type(name: str, value: Any, entity_kind: str) -> str:
    raw = _token(value)
    if raw in ORG_TYPE_ALIASES:
        return ORG_TYPE_ALIASES[raw]
    snake = raw.replace(" ", "_") if raw else ""
    if snake in ORG_TYPES:
        return snake

    n = _token(name)
    if "festival" in n or "marathon" in n or re.search(r"\b\d+k\b", n):
        return "festival"
    if "promoter" in n or "productions" in n:
        return "promoter"
    if "cinema" in n:
        return "cinema"
    if "gallery" in n:
        return "gallery"
    if "museum" in n:
        return "museum"
    if "theatre" in n:
        return "theatre"
    if "bookshop" in n:
        return "bookshop"
    if "university" in n:
        return "university"
    if "society" in n or "institute" in n:
        return "learned_society"
    if "park" in n:
        return "park"
    if "garden" in n:
        return "garden"
    if "arts centre" in n or "cultural centre" in n or "cultural center" in n:
        return "cultural_centre"
    return "festival" if entity_kind == "one_off_event" else "organisation"


def _generic_name(name: str) -> bool:
    key = _token(name)
    return not key or any(snippet in key for snippet in GENERIC_NAME_SNIPPETS)


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _extract_output_text(payload: dict[str, Any]) -> str:
    txt = _clean(payload.get("output_text"))
    if txt:
        return txt
    parts: list[str] = []
    for item in payload.get("output") or []:
        if not isinstance(item, dict):
            continue
        for c in item.get("content") or []:
            if isinstance(c, dict) and c.get("type") == "output_text":
                v = _clean(c.get("text"))
                if v:
                    parts.append(v)
    return "\n".join(parts)


def _parse_json(raw: str) -> Any:
    t = _clean(raw)
    if not t:
        return None
    if t.startswith("```") and t.endswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s*```$", "", t)
    try:
        return json.loads(t)
    except Exception:
        pass
    for o, c in (("[", "]"), ("{", "}")):
        s, e = t.find(o), t.rfind(c)
        if s >= 0 and e > s:
            try:
                return json.loads(t[s : e + 1])
            except Exception:
                pass
    return None


def _usage_tokens(payload: dict[str, Any]) -> int:
    usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else {}
    total = usage.get("total_tokens")
    if isinstance(total, int) and total > 0:
        return total
    return int(usage.get("input_tokens") or 0) + int(usage.get("output_tokens") or 0)


def _generate_queries(max_queries: int) -> list[dict[str, str]]:
    target, seen, out = max(10, min(160, int(max_queries))), set(), []

    def add(query: str, kind: str) -> None:
        q = _clean(query)
        if not q:
            return
        k = q.lower()
        if k in seen:
            return
        seen.add(k)
        out.append({"query": q, "entity_kind": kind})

    for f in PLACE_FAMILIES:
        add(f"10 {f} in London", "place")
        add(f"best London {f} official venues", "place")
        for area in AREAS:
            add(f"{f} in {area} London official venue", "place")

    for f in ONE_OFF_FAMILIES:
        add(f"major {f} in London official organizer", "one_off_event")
        add(f"London {f} official website organizer", "one_off_event")
        for area in AREAS[:6]:
            add(f"{f} in {area} London official organizer", "one_off_event")

    return out[:target]


def _search_openai(query: str, *, max_results: int, timeout: int, model: str) -> tuple[list[dict[str, Any]], int]:
    api_key = _clean(os.getenv("OPENAI_API_KEY"))
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    prompt = (
        "Use web search and return legit London venues/institutions/organizers with official sites. "
        "Never return aggregators/listicles/marketplaces/social profiles/media headlines as entities. "
        "For one-off event searches, return the organizer entity (not event title). "
        "Return strict JSON only: {\"entities\":[{\"name\":str,\"homepage\":str|null,\"events_url\":str|null,"
        "\"borough\":str|null,\"org_type\":str|null,\"description\":str|null,"
        "\"entity_kind\":\"place\"|\"one_off_event\",\"confidence\":number}]}. "
        f"Limit to {max_results}. Query: {query}"
    )

    payload = {
        "model": model,
        "tools": [{"type": "web_search_preview"}],
        "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
        "max_output_tokens": max(500, min(2200, max_results * 220)),
    }

    r = requests.post(
        OPENAI_RESPONSES_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=max(20, timeout + 10),
    )
    r.raise_for_status()

    body: dict[str, Any] = r.json()
    parsed = _parse_json(_extract_output_text(body))
    if not isinstance(parsed, dict):
        return [], _usage_tokens(body)
    entities = parsed.get("entities")
    if not isinstance(entities, list):
        return [], _usage_tokens(body)
    return [x for x in entities if isinstance(x, dict)][:max_results], _usage_tokens(body)


def _normalize_candidate(entity: dict[str, Any], default_kind: str) -> dict[str, Any] | None:
    name = _clean(entity.get("name"))
    if not name or _generic_name(name):
        return None

    homepage = _canonical_url(entity.get("homepage")) or ""
    events_url = _canonical_url(entity.get("events_url")) or ""
    if not homepage and not events_url:
        return None

    host = _domain(homepage or events_url)
    if _blocked_domain(host):
        return None

    kind = _token(entity.get("entity_kind")).replace(" ", "_")
    if kind not in {"place", "one_off_event"}:
        kind = default_kind

    org_type = _normalize_org_type(name, entity.get("org_type"), kind)
    desc = _clean(entity.get("description")) or f"{name} is a London {org_type.replace('_', ' ')}."
    try:
        conf = float(entity.get("confidence") or 0)
    except Exception:
        conf = 0.0

    return {
        "name": name,
        "homepage": homepage or None,
        "events_url": events_url or None,
        "borough": _clean(entity.get("borough")) or None,
        "org_type": org_type,
        "description": desc,
        "entity_kind": kind,
        "confidence": max(0.0, min(1.0, conf)),
    }


def _dedupe(items: list[dict[str, Any]], max_candidates: int) -> list[dict[str, Any]]:
    picked: dict[str, dict[str, Any]] = {}
    for row in items:
        d, n = _domain(row.get("homepage") or row.get("events_url")), _token(row.get("name"))
        if not n:
            continue
        key = f"{d}|{n}" if d else f"name|{n}"
        cur = picked.get(key)
        if not cur or float(row.get("confidence") or 0) > float(cur.get("confidence") or 0):
            picked[key] = row
    out = list(picked.values())
    out.sort(key=lambda r: (-float(r.get("confidence") or 0), _clean(r.get("name")).lower()))
    return out[:max_candidates]


def run_discovery_cycle(
    *,
    trigger: str = "scheduled",
    max_queries: int | None = None,
    max_results_per_query: int | None = None,
    max_candidates: int | None = None,
    request_timeout: int | None = None,
    dry_run: bool = False,
    borough: str | None = None,
    category: str | None = None,
    search_provider: str | None = None,
) -> dict[str, Any]:
    _ = borough
    _ = category
    _ = search_provider
    init_db()

    max_queries = max(10, min(160, int(max_queries or os.getenv("DISCOVERY_MAX_QUERIES", "120"))))
    max_results = max(2, min(12, int(max_results_per_query or os.getenv("DISCOVERY_MAX_RESULTS_PER_QUERY", "6"))))
    max_candidates = max(10, min(600, int(max_candidates or os.getenv("DISCOVERY_MAX_CANDIDATES", "300"))))
    timeout = max(8, min(60, int(request_timeout or os.getenv("DISCOVERY_REQUEST_TIMEOUT", "15"))))
    lock_minutes = max(5, int(os.getenv("DISCOVERY_RUN_LOCK_MINUTES", "90")))
    manual_unlock = max(1, int(os.getenv("DISCOVERY_MANUAL_UNLOCK_MINUTES", "5")))

    running = get_latest_running_discovery_run()
    if running:
        started = _parse_dt(running.get("started_at"))
        age = int((datetime.now(timezone.utc) - started).total_seconds() // 60) if started else None
        force_clear = trigger == "manual" and age is not None and age >= manual_unlock
        if age is None or age < lock_minutes:
            if force_clear:
                finish_discovery_run(
                    run_id=int(running["id"]),
                    status="failed",
                    result_count=int(running.get("result_count") or 0),
                    upserted_count=int(running.get("upserted_count") or 0),
                    error=f"Marked stale by manual retry after {age} minutes",
                    details={"status": "failed", "reason": "stale_lock_cleared_by_manual_retry", "age_minutes": age},
                )
            else:
                return {"status": "skipped", "reason": "another discovery run is still active", "query_count": 0, "candidate_count": 0, "upserted_count": 0, "query_errors": 0}
        else:
            finish_discovery_run(
                run_id=int(running["id"]),
                status="failed",
                result_count=int(running.get("result_count") or 0),
                upserted_count=int(running.get("upserted_count") or 0),
                error=f"Marked stale after lock timeout ({lock_minutes} minutes)",
                details={"status": "failed", "reason": "stale_lock_timeout", "age_minutes": age},
            )

    model = _clean(os.getenv("DISCOVERY_OPENAI_WEB_MODEL") or os.getenv("DISCOVERY_OPENAI_MODEL") or "gpt-5-mini")
    queries = _generate_queries(max_queries)
    run_id = start_discovery_run(
        query_count=len(queries),
        trigger=trigger,
        details={
            "max_queries": max_queries,
            "max_results_per_query": max_results,
            "max_candidates": max_candidates,
            "request_timeout": timeout,
            "dry_run": bool(dry_run),
            "search_provider": "openai_web",
            "model": model,
        },
    )

    query_errors, llm_tokens = 0, 0
    query_debug: list[dict[str, Any]] = []
    query_error_debug: list[dict[str, Any]] = []
    accepted: list[dict[str, Any]] = []

    try:
        for item in queries:
            q = _clean(item.get("query"))
            kind = _clean(item.get("entity_kind") or "place")
            if not q:
                continue
            try:
                entities, tokens = _search_openai(q, max_results=max_results, timeout=timeout, model=model)
                llm_tokens += int(tokens or 0)
            except Exception as exc:
                query_errors += 1
                if len(query_error_debug) < 30:
                    query_error_debug.append({"query": q, "error_type": type(exc).__name__, "error": _clean(str(exc))[:280]})
                continue

            rows = [row for row in (_normalize_candidate(raw, kind) for raw in entities) if row]
            accepted.extend(rows)
            if len(query_debug) < 30:
                query_debug.append({"query": q, "result_count": len(entities), "accepted_entity_count": len(rows), "sample_names": [r.get("name") for r in rows[:3]]})
            if len(accepted) >= max_candidates * 2:
                break

        final_rows = _dedupe(accepted, max_candidates)
        upserted = 0
        if not dry_run:
            for row in final_rows:
                try:
                    upsert_org(
                        name=row.get("name"),
                        homepage=row.get("homepage"),
                        events_url=row.get("events_url"),
                        description=row.get("description"),
                        borough=row.get("borough"),
                        org_type=row.get("org_type"),
                        source="auto_discovery",
                    )
                    upserted += 1
                except Exception:
                    continue

        place_count = sum(1 for row in final_rows if row.get("entity_kind") == "place")
        one_off_count = sum(1 for row in final_rows if row.get("entity_kind") == "one_off_event")
        llm_calls = len(query_debug) + query_errors
        summary = {
            "run_id": run_id,
            "status": "success",
            "query_count": len(queries),
            "query_errors": query_errors,
            "candidate_count": len(final_rows),
            "place_count": place_count,
            "one_off_event_count": one_off_count,
            "upserted_count": upserted,
            "llm_calls": llm_calls,
            "llm_tokens": llm_tokens,
            "avg_tokens": round(llm_tokens / max(1, llm_calls), 2),
            "search_provider": "openai_web",
            "query_debug": query_debug,
            "query_error_debug": query_error_debug,
            "dry_run": bool(dry_run),
        }
        finish_discovery_run(run_id, "success", len(final_rows), upserted, details=summary)
        return summary

    except Exception as exc:
        message = _clean(str(exc))[:500]
        finish_discovery_run(run_id, "failed", 0, 0, error=message, details={"status": "failed", "error": message})
        raise
