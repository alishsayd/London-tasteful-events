"""Minimal org discovery pipeline (OpenAI web only)."""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests

from app.db import (
    finish_discovery_run,
    get_latest_running_discovery_run,
    init_db,
    start_discovery_run,
    upsert_org,
)

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"

BOROUGHS = [
    "Hackney",
    "Tower Hamlets",
    "Southwark",
    "Lambeth",
    "Camden",
    "Islington",
    "Westminster",
    "Kensington and Chelsea",
    "Hammersmith and Fulham",
    "Lewisham",
    "Greenwich",
    "Wandsworth",
    "Haringey",
    "Newham",
    "City of London",
    "Waltham Forest",
    "Barking and Dagenham",
    "Croydon",
    "Ealing",
    "Brent",
    "Enfield",
    "Hounslow",
    "Richmond upon Thames",
    "Kingston upon Thames",
    "Bromley",
    "Barnet",
    "Redbridge",
    "Harrow",
    "Havering",
    "Hillingdon",
    "Merton",
    "Sutton",
    "Bexley",
]

REGION_HINTS = [
    "Central London",
    "East London",
    "South London",
    "North London",
    "West London",
    "Southbank",
    "Greenwich",
    "Camden",
    "Hackney",
]

PLACE_FAMILIES = [
    "independent art centres",
    "contemporary art galleries",
    "photography galleries",
    "architecture talk venues",
    "science museums with talks",
    "public lecture venues",
    "literary salons",
    "bookshop event venues",
    "cinematheques",
    "independent cinemas",
    "craft workshops with classes",
    "makerspaces with public workshops",
    "country cultural centres",
    "diaspora arts centres",
    "community arts venues",
    "parks with events programmes",
    "gardens with evening events",
    "small performance spaces",
    "theatre venues",
    "live music venues",
]

ONE_OFF_FAMILIES = [
    "food festivals",
    "design festivals",
    "architecture festivals",
    "film festivals",
    "literary festivals",
    "music festivals",
    "5K runs",
    "10K runs",
    "half marathons",
    "charity runs",
]

ORG_TYPE_VALUES = {
    "bookshop",
    "cinema",
    "gallery",
    "live_music_venue",
    "theatre",
    "museum",
    "makers_space",
    "park",
    "garden",
    "cultural_centre",
    "university",
    "learned_society",
    "promoter",
    "festival",
    "organisation",
}

ORG_TYPE_ALIASES = {
    "live music venue": "live_music_venue",
    "live_music_venue": "live_music_venue",
    "makers space": "makers_space",
    "makerspace": "makers_space",
    "cultural center": "cultural_centre",
    "cultural centre": "cultural_centre",
    "learned society": "learned_society",
    "one off event": "festival",
    "one-off event": "festival",
    "one_off_event": "festival",
}

GENERIC_ENTITY_PATTERNS = (
    "what's on",
    "whats on",
    "book your tickets",
    "subscribe",
    "support",
    "overview",
    "current events",
    "events listings",
)


# -----------------------------
# Helpers
# -----------------------------

def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_token(value: str | None) -> str:
    lowered = _clean_text(value).lower()
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def _normalize_name(value: str | None) -> str:
    normalized = _normalize_token(value)
    if normalized.startswith("the "):
        normalized = normalized[4:]
    return normalized


def _canonicalize_url(value: str | None) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"}:
            return ""
        host = (parsed.netloc or "").lower().replace("www.", "")
        if not host:
            return ""
        path = (parsed.path or "/").rstrip("/") or "/"
        return f"{parsed.scheme}://{host}{path}"
    except Exception:
        return ""


def _domain(url: str | None) -> str:
    canonical = _canonicalize_url(url)
    if not canonical:
        return ""
    return (urlparse(canonical).netloc or "").lower().replace("www.", "")


def _normalize_borough(value: str | None) -> str | None:
    raw = _clean_text(value)
    if not raw:
        return None
    lowered = raw.lower()
    if lowered in {"multiple", "various", "all boroughs", "all"}:
        return None
    if lowered == "city of westminster":
        return "Westminster"
    for borough in BOROUGHS:
        if lowered == borough.lower():
            return borough
    return None


def _normalize_org_type(value: str | None) -> str:
    token = _normalize_token(value).replace(" ", "_")
    if token in ORG_TYPE_VALUES:
        return token
    alias = ORG_TYPE_ALIASES.get(_normalize_token(value))
    if alias:
        return alias
    return "organisation"


def _infer_org_type(name: str, provided: str | None) -> str:
    explicit = _normalize_org_type(provided)
    if explicit != "organisation":
        return explicit

    name_norm = _normalize_name(name)
    if any(term in name_norm for term in ("festival", "biennale", "triennale", "carnival", "marathon", "run")):
        return "festival"
    if any(term in name_norm for term in ("promoter", "presents", "productions")):
        return "promoter"
    if "university" in name_norm or any(key in name_norm for key in (" soas", " lse", " imperial")):
        return "university"
    if any(term in name_norm for term in ("society", "institute", "gresham", "rsa")):
        return "learned_society"
    if "bookshop" in name_norm or "bookstore" in name_norm:
        return "bookshop"
    if "cinema" in name_norm or "film" in name_norm:
        return "cinema"
    if "gallery" in name_norm:
        return "gallery"
    if "museum" in name_norm:
        return "museum"
    if "theatre" in name_norm or "theater" in name_norm:
        return "theatre"
    if "workshop" in name_norm or "makerspace" in name_norm or "maker space" in name_norm:
        return "makers_space"
    if "park" in name_norm:
        return "park"
    if "garden" in name_norm or "conservatory" in name_norm:
        return "garden"
    if "arts centre" in name_norm or "cultural centre" in name_norm or "cultural center" in name_norm:
        return "cultural_centre"
    if any(term in name_norm for term in ("music", "jazz", "orchestra", "club")):
        return "live_music_venue"
    return "organisation"


def _entity_name_is_generic(name: str) -> bool:
    norm = _normalize_name(name)
    if not norm:
        return True
    return any(pattern in norm for pattern in GENERIC_ENTITY_PATTERNS)


def _env_int(key: str, fallback: int) -> int:
    raw = _clean_text(os.getenv(key, str(fallback)))
    try:
        parsed = int(raw)
        return parsed if parsed > 0 else fallback
    except Exception:
        return fallback


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _extract_output_text(payload: dict[str, Any]) -> str:
    output_text = _clean_text(payload.get("output_text"))
    if output_text:
        return output_text

    parts: list[str] = []
    for item in payload.get("output") or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            if content.get("type") == "output_text":
                text_value = _clean_text(content.get("text"))
                if text_value:
                    parts.append(text_value)
    return "\n".join(parts).strip()


def _parse_json_payload(raw: str) -> Any:
    value = _clean_text(raw)
    if not value:
        return None

    if value.startswith("```") and value.endswith("```"):
        value = re.sub(r"^```(?:json)?\s*", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s*```$", "", value)

    try:
        return json.loads(value)
    except Exception:
        pass

    for opener, closer in (("[", "]"), ("{", "}")):
        start = value.find(opener)
        end = value.rfind(closer)
        if start >= 0 and end > start:
            fragment = value[start : end + 1]
            try:
                return json.loads(fragment)
            except Exception:
                continue
    return None


def _usage_tokens(payload: dict[str, Any]) -> int:
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return 0
    total = usage.get("total_tokens")
    if isinstance(total, int) and total > 0:
        return total
    inp = int(usage.get("input_tokens") or 0)
    out = int(usage.get("output_tokens") or 0)
    return max(0, inp + out)


# -----------------------------
# Query generation
# -----------------------------

def _generate_queries(max_queries: int) -> list[dict[str, str]]:
    target = max(10, min(120, int(max_queries)))
    queries: list[dict[str, str]] = []
    seen: set[str] = set()

    def push(query: str, kind: str) -> None:
        clean = _clean_text(query)
        if not clean:
            return
        key = clean.lower()
        if key in seen:
            return
        seen.add(key)
        queries.append({"query": clean, "entity_kind": kind})

    for family in PLACE_FAMILIES:
        push(f"10 {family} in London", "place")
        push(f"best places for {family} events in London", "place")
        for region in REGION_HINTS[:5]:
            push(f"10 {family} in {region}", "place")

    for family in ONE_OFF_FAMILIES:
        push(f"major {family} in London official website organizer", "one_off_event")
        push(f"London {family} official event organizer", "one_off_event")

    return queries[:target]


# -----------------------------
# OpenAI search
# -----------------------------

def _search_entities_openai(
    query: str,
    *,
    max_results: int,
    timeout: int,
    model: str,
) -> tuple[list[dict[str, Any]], int]:
    api_key = _clean_text(os.getenv("OPENAI_API_KEY"))
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    prompt = (
        "Use web search to find legitimate London organisations/venues or organisers. "
        "Avoid listing pages and aggregators as entities. "
        "For one-off event searches, return the organizer organisation behind the event when possible. "
        "Return strict JSON only in this shape: "
        "{\"entities\":[{\"name\":str,\"homepage\":str|null,\"events_url\":str|null,\"borough\":str|null,"
        "\"org_type\":str|null,\"description\":str|null,\"entity_kind\":\"place\"|\"one_off_event\",\"confidence\":number}]}. "
        f"Limit to {max_results} entities. Query: {query}"
    )

    payload = {
        "model": model,
        "tools": [{"type": "web_search_preview"}],
        "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
        "max_output_tokens": max(500, min(2000, max_results * 180)),
    }

    response = requests.post(
        OPENAI_RESPONSES_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=max(20, timeout + 10),
    )
    response.raise_for_status()

    response_payload: dict[str, Any] = response.json()
    parsed = _parse_json_payload(_extract_output_text(response_payload))
    if not isinstance(parsed, dict):
        return [], _usage_tokens(response_payload)

    raw_entities = parsed.get("entities")
    if not isinstance(raw_entities, list):
        return [], _usage_tokens(response_payload)

    entities = [item for item in raw_entities if isinstance(item, dict)]
    return entities[:max_results], _usage_tokens(response_payload)


def _normalize_candidate(entity: dict[str, Any], default_kind: str) -> dict[str, Any] | None:
    name = _clean_text(entity.get("name"))
    if not name or _entity_name_is_generic(name):
        return None

    homepage = _canonicalize_url(entity.get("homepage")) or ""
    events_url = _canonicalize_url(entity.get("events_url")) or ""
    if not homepage and not events_url:
        return None

    borough = _normalize_borough(entity.get("borough"))
    org_type = _infer_org_type(name, entity.get("org_type"))
    if org_type not in ORG_TYPE_VALUES:
        org_type = "organisation"

    entity_kind = _normalize_token(entity.get("entity_kind")).replace(" ", "_")
    if entity_kind not in {"place", "one_off_event"}:
        entity_kind = default_kind

    # Keep one-off entities in explicit org bucket(s), not as ad-hoc categories.
    if entity_kind == "one_off_event" and org_type == "organisation":
        org_type = "festival"

    description = _clean_text(entity.get("description"))
    if not description:
        if borough:
            description = f"{name} is a London {org_type.replace('_', ' ')} in {borough}."
        else:
            description = f"{name} is a London {org_type.replace('_', ' ')}."

    try:
        confidence = float(entity.get("confidence") or 0)
    except Exception:
        confidence = 0.0

    return {
        "name": name,
        "homepage": homepage or None,
        "events_url": events_url or None,
        "borough": borough,
        "org_type": org_type,
        "description": description,
        "entity_kind": entity_kind,
        "confidence": max(0.0, min(1.0, confidence)),
    }


def _dedupe_candidates(items: list[dict[str, Any]], max_candidates: int) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for item in items:
        name_key = _normalize_name(item.get("name"))
        domain = _domain(item.get("homepage") or item.get("events_url"))
        if not name_key:
            continue
        key = f"{domain}|{name_key}" if domain else f"name|{name_key}"
        existing = by_key.get(key)
        if not existing or float(item.get("confidence") or 0) > float(existing.get("confidence") or 0):
            by_key[key] = item

    out = list(by_key.values())
    out.sort(key=lambda row: (-float(row.get("confidence") or 0), _clean_text(row.get("name")).lower()))
    return out[:max_candidates]


# -----------------------------
# Public API
# -----------------------------

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
    """Run one discovery cycle and upsert discovered org entities."""

    # borough/category/search_provider are kept for API compatibility.
    _ = borough
    _ = category
    _ = search_provider

    init_db()

    max_queries = max(10, min(120, int(max_queries or _env_int("DISCOVERY_MAX_QUERIES", 40))))
    max_results_per_query = max(2, min(15, int(max_results_per_query or _env_int("DISCOVERY_MAX_RESULTS_PER_QUERY", 8))))
    max_candidates = max(10, min(500, int(max_candidates or _env_int("DISCOVERY_MAX_CANDIDATES", 240))))
    request_timeout = max(8, min(60, int(request_timeout or _env_int("DISCOVERY_REQUEST_TIMEOUT", 15))))

    lock_window_minutes = _env_int("DISCOVERY_RUN_LOCK_MINUTES", 90)
    manual_unlock_minutes = _env_int("DISCOVERY_MANUAL_UNLOCK_MINUTES", 5)

    running = get_latest_running_discovery_run()
    if running:
        started_at = _parse_dt(running.get("started_at"))
        age_minutes = None
        if started_at:
            age_minutes = int((datetime.now(timezone.utc) - started_at).total_seconds() // 60)

        in_lock = age_minutes is None or age_minutes < lock_window_minutes
        if in_lock:
            can_manual_unlock = trigger == "manual" and age_minutes is not None and age_minutes >= manual_unlock_minutes
            if can_manual_unlock:
                finish_discovery_run(
                    run_id=int(running["id"]),
                    status="failed",
                    result_count=int(running.get("result_count") or 0),
                    upserted_count=int(running.get("upserted_count") or 0),
                    error=f"Marked stale by manual retry after {age_minutes} minutes",
                    details={"status": "failed", "reason": "stale_lock_cleared_by_manual_retry", "age_minutes": age_minutes},
                )
            else:
                return {
                    "status": "skipped",
                    "reason": "another discovery run is still active",
                    "query_count": 0,
                    "candidate_count": 0,
                    "upserted_count": 0,
                    "query_errors": 0,
                }
        else:
            finish_discovery_run(
                run_id=int(running["id"]),
                status="failed",
                result_count=int(running.get("result_count") or 0),
                upserted_count=int(running.get("upserted_count") or 0),
                error=f"Marked stale after lock timeout ({lock_window_minutes} minutes)",
                details={"status": "failed", "reason": "stale_lock_timeout", "age_minutes": age_minutes},
            )

    model = _clean_text(os.getenv("DISCOVERY_OPENAI_WEB_MODEL") or os.getenv("DISCOVERY_OPENAI_MODEL") or "gpt-5-mini")
    queries = _generate_queries(max_queries)

    run_id = start_discovery_run(
        query_count=len(queries),
        trigger=trigger,
        details={
            "max_queries": max_queries,
            "max_results_per_query": max_results_per_query,
            "max_candidates": max_candidates,
            "request_timeout": request_timeout,
            "dry_run": bool(dry_run),
            "search_provider": "openai_web",
            "model": model,
        },
    )

    query_errors = 0
    query_debug: list[dict[str, Any]] = []
    query_error_debug: list[dict[str, Any]] = []

    metrics = {"llm_calls": 0, "llm_tokens": 0}
    accepted: list[dict[str, Any]] = []

    try:
        for item in queries:
            query = _clean_text(item.get("query"))
            entity_kind = _clean_text(item.get("entity_kind") or "place")
            if not query:
                continue

            try:
                entities, tokens = _search_entities_openai(
                    query,
                    max_results=max_results_per_query,
                    timeout=request_timeout,
                    model=model,
                )
                metrics["llm_calls"] += 1
                metrics["llm_tokens"] += int(tokens or 0)
            except Exception as exc:
                query_errors += 1
                if len(query_error_debug) < 24:
                    query_error_debug.append(
                        {
                            "query": query,
                            "error_type": type(exc).__name__,
                            "error": _clean_text(str(exc))[:260],
                        }
                    )
                continue

            normalized_rows: list[dict[str, Any]] = []
            for raw in entities:
                row = _normalize_candidate(raw, default_kind=entity_kind)
                if row:
                    normalized_rows.append(row)

            accepted.extend(normalized_rows)

            if len(query_debug) < 24:
                query_debug.append(
                    {
                        "query": query,
                        "result_count": len(entities),
                        "accepted_entity_count": len(normalized_rows),
                        "sample_names": [item.get("name") for item in normalized_rows[:3]],
                    }
                )

            if len(accepted) >= max_candidates * 2:
                break

        final_candidates = _dedupe_candidates(accepted, max_candidates=max_candidates)

        upserted_count = 0
        if not dry_run:
            for row in final_candidates:
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
                    upserted_count += 1
                except Exception:
                    continue

        place_count = sum(1 for row in final_candidates if row.get("entity_kind") == "place")
        one_off_count = sum(1 for row in final_candidates if row.get("entity_kind") == "one_off_event")

        summary = {
            "run_id": run_id,
            "status": "success",
            "query_count": len(queries),
            "query_errors": query_errors,
            "candidate_count": len(final_candidates),
            "place_count": place_count,
            "one_off_event_count": one_off_count,
            "upserted_count": upserted_count,
            "llm_calls": metrics["llm_calls"],
            "llm_tokens": metrics["llm_tokens"],
            "avg_tokens": round((metrics["llm_tokens"] / metrics["llm_calls"]), 2) if metrics["llm_calls"] else 0.0,
            "search_provider": "openai_web",
            "query_debug": query_debug,
            "query_error_debug": query_error_debug,
            "dry_run": bool(dry_run),
        }

        finish_discovery_run(
            run_id=run_id,
            status="success",
            result_count=len(final_candidates),
            upserted_count=upserted_count,
            details=summary,
        )
        return summary

    except Exception as exc:
        message = _clean_text(str(exc))[:500]
        finish_discovery_run(
            run_id=run_id,
            status="failed",
            result_count=0,
            upserted_count=0,
            error=message,
            details={"status": "failed", "error": message},
        )
        raise


# -----------------------------
# CLI
# -----------------------------

def print_queries(queries: list[dict[str, str]]) -> None:
    for idx, row in enumerate(queries, 1):
        print(f"{idx:4d}. [{row.get('entity_kind', 'place')}] {row.get('query')}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover candidate orgs")
    parser.add_argument("--print-queries", action="store_true", help="Print generated discovery queries")
    parser.add_argument("--run-once", action="store_true", help="Run one discovery cycle")
    parser.add_argument("--dry-run", action="store_true", help="Discover candidates without writing to DB")
    parser.add_argument("--trigger", default="manual", help="Trigger label for run logs")
    parser.add_argument("--max-queries", type=int, default=None)
    parser.add_argument("--max-results-per-query", type=int, default=None)
    parser.add_argument("--max-candidates", type=int, default=None)
    parser.add_argument("--request-timeout", type=int, default=None)
    args = parser.parse_args()

    if args.print_queries:
        print_queries(_generate_queries(args.max_queries or _env_int("DISCOVERY_MAX_QUERIES", 40)))
        return

    if args.run_once:
        summary = run_discovery_cycle(
            trigger=args.trigger,
            max_queries=args.max_queries,
            max_results_per_query=args.max_results_per_query,
            max_candidates=args.max_candidates,
            request_timeout=args.request_timeout,
            dry_run=args.dry_run,
            search_provider="openai_web",
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    parser.print_help()


if __name__ == "__main__":
    main()
