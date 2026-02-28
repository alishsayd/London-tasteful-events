"""Automatic organisation discovery.

This module can:
1. Generate discovery queries.
2. Search the web for candidate organisation sites.
3. Extract org metadata (name, homepage, events URL, description, borough, category).
4. Upsert candidates into the org database.

Usage:
    python -m app.discover --run-once
    python -m app.discover --run-once --max-queries 12 --max-candidates 40
    python -m app.discover --from-file results.json
    python -m app.discover --print-queries
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date
from typing import Any
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from app.db import (
    add_strategy,
    finish_discovery_run,
    get_stats,
    get_strategies,
    has_recent_running_discovery,
    init_db,
    start_discovery_run,
    upsert_org,
)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# London boroughs — focus on dense cultural areas but keep full city coverage
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

CATEGORIES = [
    "independent gallery",
    "community cinema",
    "theatre",
    "live music venue",
    "bookshop events",
    "cultural centre",
    "museum",
    "arts charity",
    "community space",
    "poetry readings",
    "lecture series",
    "workshop space",
    "independent arts venue",
]

AGGREGATOR_QUERIES = [
    "site:ianvisits.co.uk London free art talk",
    "site:ianvisits.co.uk London gallery events",
    "Open House London participating venues",
    "London independent gallery events programme",
    "London independent cinema whats on",
    "London architecture foundation events",
    "London museum talks programme",
    "London bookshop events calendar",
]

EVENT_URL_HINTS = (
    "events",
    "whatson",
    "whats-on",
    "programme",
    "program",
    "calendar",
    "talk",
    "screening",
    "visit/events",
)

BLOCKED_DOMAINS = {
    "duckduckgo.com",
    "google.com",
    "bing.com",
    "yahoo.com",
    "youtube.com",
    "facebook.com",
    "instagram.com",
    "x.com",
    "twitter.com",
    "tiktok.com",
    "linkedin.com",
    "wikipedia.org",
    "eventbrite.com",
    "meetup.com",
    "timeout.com",
}

BOROUGH_ALIASES = {
    "kensington and chelsea": ["kensington and chelsea", "kensington & chelsea"],
}

CATEGORY_MAP = {
    "gallery": ["gallery", "exhibition", "contemporary art"],
    "museum": ["museum", "heritage", "archive"],
    "cinema": ["cinema", "film", "screening", "picturehouse"],
    "bookshop": ["bookshop", "bookshop", "bookstore", "books"],
    "cultural centre": ["cultural centre", "cultural center", "centre", "center"],
    "art centre": ["art centre", "art center", "arts centre", "arts center"],
    "house": ["house museum", "historic house", "house"],
    "social community center": ["community", "social", "charity", "collective"],
}


def _clean_text(value: str | None) -> str:
    text_value = str(value or "")
    text_value = re.sub(r"\s+", " ", text_value)
    return text_value.strip()


def _domain(url: str | None) -> str:
    if not url:
        return ""
    try:
        return (urlparse(url).netloc or "").lower().replace("www.", "")
    except Exception:
        return ""


def _normalize_homepage(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    host = parsed.netloc.lower()
    return f"{scheme}://{host}"


def _looks_like_html_response(response: requests.Response) -> bool:
    content_type = str(response.headers.get("content-type") or "").lower()
    return "text/html" in content_type or "application/xhtml+xml" in content_type


def _unwrap_duckduckgo_href(href: str) -> str | None:
    if not href:
        return None

    href = href.strip()
    if href.startswith("//"):
        href = f"https:{href}"

    parsed = urlparse(href)
    if "duckduckgo.com" not in (parsed.netloc or ""):
        return href if parsed.scheme in {"http", "https"} else None

    query = parse_qs(parsed.query)
    if "uddg" in query and query["uddg"]:
        target = unquote(query["uddg"][0])
        target_parsed = urlparse(target)
        if target_parsed.scheme in {"http", "https"}:
            return target

    return None


def _should_skip_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return True

    domain = (parsed.netloc or "").lower().replace("www.", "")
    if not domain:
        return True

    if domain in BLOCKED_DOMAINS:
        return True

    if parsed.path.lower().endswith((".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".zip")):
        return True

    return False


def _search_duckduckgo(query: str, max_results: int, timeout: int) -> list[str]:
    response = requests.get(
        "https://duckduckgo.com/html/",
        params={"q": query},
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()

    for anchor in soup.select("a.result__a"):
        href = _unwrap_duckduckgo_href(str(anchor.get("href") or ""))
        if not href:
            continue
        if _should_skip_url(href):
            continue
        key = href.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        urls.append(href)
        if len(urls) >= max_results:
            break

    return urls


def _extract_title_and_description(soup: BeautifulSoup) -> tuple[str, str]:
    title_tag = soup.find("title")
    title = _clean_text(title_tag.get_text(" ", strip=True) if title_tag else "")

    desc_meta = soup.find("meta", attrs={"name": "description"})
    if not desc_meta:
        desc_meta = soup.find("meta", attrs={"property": "og:description"})
    description = _clean_text(desc_meta.get("content") if desc_meta else "")

    return title, description


def _extract_name(page_url: str, soup: BeautifulSoup, title: str) -> str | None:
    og_site = soup.find("meta", attrs={"property": "og:site_name"})
    if og_site and _clean_text(og_site.get("content")):
        candidate = _clean_text(og_site.get("content"))
        if len(candidate) >= 3:
            return candidate

    if title:
        for sep in ("|", " - ", "—", "·"):
            if sep in title:
                candidate = _clean_text(title.split(sep)[0])
                if len(candidate) >= 3 and candidate.lower() not in {"home", "events", "what's on", "what’s on"}:
                    return candidate

        if len(title) >= 3 and len(title) <= 90:
            return title

    host = _domain(page_url)
    if host:
        base = host.split(".")[0].replace("-", " ").strip()
        base = _clean_text(base.title())
        if len(base) >= 3:
            return base

    return None


def _infer_borough(text_blob: str) -> str | None:
    lower = text_blob.lower()

    for borough in BOROUGHS:
        borough_lower = borough.lower()
        if borough_lower in lower:
            return borough

        for alias in BOROUGH_ALIASES.get(borough_lower, []):
            if alias in lower:
                return borough

    return None


def _infer_category(text_blob: str) -> str:
    lower = text_blob.lower()
    best = "other"
    best_score = 0

    for category, keywords in CATEGORY_MAP.items():
        score = 0
        for keyword in keywords:
            if keyword in lower:
                score += 1
        if score > best_score:
            best_score = score
            best = category

    return best


def _extract_events_url(page_url: str, soup: BeautifulSoup) -> str | None:
    parsed_page = urlparse(page_url)
    page_path = (parsed_page.path or "").lower()
    if any(hint in page_path for hint in EVENT_URL_HINTS):
        return page_url

    best_url = None
    best_score = -1
    page_domain = _domain(page_url)

    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "").strip()
        if not href:
            continue

        resolved = urljoin(page_url, href)
        if _domain(resolved) != page_domain:
            continue

        parsed = urlparse(resolved)
        path = (parsed.path or "").lower()
        if not any(hint in path for hint in EVENT_URL_HINTS):
            continue

        score = 0
        for hint in EVENT_URL_HINTS:
            if hint in path:
                score += 5

        anchor_text = _clean_text(anchor.get_text(" ", strip=True)).lower()
        if "event" in anchor_text or "what's on" in anchor_text or "what’s on" in anchor_text or "programme" in anchor_text:
            score += 20

        if score > best_score:
            best_score = score
            best_url = resolved

    return best_url


def _is_london_related(text_blob: str) -> bool:
    lower = text_blob.lower()
    if "london" in lower:
        return True
    return _infer_borough(lower) is not None


def _extract_org_candidate(url: str, timeout: int) -> dict[str, Any] | None:
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    response.raise_for_status()

    if not _looks_like_html_response(response):
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    title, description = _extract_title_and_description(soup)

    page_text = _clean_text(soup.get_text(" ", strip=True))
    text_blob = _clean_text(" ".join([title, description, page_text[:8000], url]))
    if not _is_london_related(text_blob):
        return None

    name = _extract_name(url, soup, title)
    if not name:
        return None

    homepage = _normalize_homepage(response.url)
    events_url = _extract_events_url(response.url, soup)
    borough = _infer_borough(text_blob)
    category = _infer_category(text_blob)

    if not description:
        if page_text:
            description = _clean_text(page_text[:220])

    if not description:
        description = f"{name} is a London cultural venue."

    return {
        "name": name,
        "homepage": homepage,
        "events_url": events_url,
        "description": description,
        "borough": borough,
        "category": category,
        "source": "auto_discovery",
    }


def generate_queries(boroughs=None, categories=None):
    """Generate grid + aggregator queries."""
    boroughs = boroughs or BOROUGHS
    categories = categories or CATEGORIES

    queries = []

    for borough in boroughs:
        for category in categories:
            queries.append(
                {
                    "query": f"{category} {borough} London events",
                    "borough": borough,
                    "category": category,
                    "source": "borough_search",
                }
            )

    for q in AGGREGATOR_QUERIES:
        queries.append(
            {
                "query": q,
                "borough": None,
                "category": None,
                "source": "aggregator_search",
            }
        )

    return queries


def _build_discovery_queries(max_queries: int, boroughs=None, categories=None) -> list[dict[str, Any]]:
    active_strategies = [item for item in get_strategies() if item.get("active")]

    query_objects: list[dict[str, Any]] = []

    for strategy in active_strategies:
        text_value = _clean_text(strategy.get("text"))
        if not text_value:
            continue

        query_objects.append(
            {
                "query": f"{text_value} London cultural events venue",
                "source": "strategy",
                "strategy_id": int(strategy["id"]),
            }
        )

    for q in AGGREGATOR_QUERIES:
        query_objects.append({"query": q, "source": "aggregator"})

    grid = [item for item in generate_queries(boroughs=boroughs, categories=categories) if item.get("source") == "borough_search"]
    if grid:
        start = date.today().toordinal() % len(grid)
        rotate_count = min(len(grid), max_queries * 3)
        for idx in range(rotate_count):
            query_objects.append(grid[(start + idx) % len(grid)])

    deduped: list[dict[str, Any]] = []
    seen_queries: set[str] = set()
    for item in query_objects:
        query = _clean_text(item.get("query"))
        if not query:
            continue
        key = query.lower()
        if key in seen_queries:
            continue
        seen_queries.add(key)
        deduped.append({**item, "query": query})
        if len(deduped) >= max_queries:
            break

    return deduped


def import_from_file(filepath):
    """Import orgs from a JSON file.

    Expected format: list of objects with at minimum a 'name' field.
    Optional fields: homepage, events_url, description, borough, category.
    """
    with open(filepath) as f:
        data = json.load(f)

    if not isinstance(data, list):
        print("Error: JSON file must contain a list of objects")
        sys.exit(1)

    count = 0
    for item in data:
        if "name" not in item:
            continue
        upsert_org(
            name=item["name"],
            homepage=item.get("homepage"),
            events_url=item.get("events_url"),
            description=item.get("description"),
            borough=item.get("borough"),
            category=item.get("category"),
            source="file_import",
        )
        count += 1

    print(f"Imported {count} orgs from {filepath}")


def print_queries(queries):
    for idx, item in enumerate(queries, 1):
        print(f"{idx:4d}. [{item.get('source', 'query')}] {item.get('query')}")


def _env_int(key: str, fallback: int) -> int:
    raw = str(os.getenv(key, str(fallback))).strip()
    try:
        parsed = int(raw)
        return parsed if parsed > 0 else fallback
    except Exception:
        return fallback


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
) -> dict[str, Any]:
    """Run one full discovery cycle and upsert discovered organisations."""

    init_db()

    max_queries = max_queries or _env_int("DISCOVERY_MAX_QUERIES", 16)
    max_results_per_query = max_results_per_query or _env_int("DISCOVERY_MAX_RESULTS_PER_QUERY", 8)
    max_candidates = max_candidates or _env_int("DISCOVERY_MAX_CANDIDATES", 60)
    request_timeout = request_timeout or _env_int("DISCOVERY_REQUEST_TIMEOUT", 12)

    if has_recent_running_discovery(max_age_minutes=_env_int("DISCOVERY_RUN_LOCK_MINUTES", 90)):
        return {
            "status": "skipped",
            "reason": "another discovery run is still active",
            "query_count": 0,
            "searched_url_count": 0,
            "candidate_count": 0,
            "upserted_count": 0,
            "query_errors": 0,
        }

    boroughs = [borough] if borough else None
    categories = [category] if category else None
    queries = _build_discovery_queries(max_queries=max_queries, boroughs=boroughs, categories=categories)

    run_id = start_discovery_run(
        query_count=len(queries),
        trigger=trigger,
        details={
            "max_queries": max_queries,
            "max_results_per_query": max_results_per_query,
            "max_candidates": max_candidates,
            "request_timeout": request_timeout,
            "dry_run": dry_run,
        },
    )

    query_errors = 0
    searched_urls: list[str] = []
    seen_urls: set[str] = set()
    seen_domains: set[str] = set()

    try:
        for item in queries:
            query = item["query"]
            try:
                results = _search_duckduckgo(query=query, max_results=max_results_per_query, timeout=request_timeout)
            except Exception:
                query_errors += 1
                continue

            for url in results:
                if _should_skip_url(url):
                    continue
                key = url.lower().strip()
                if key in seen_urls:
                    continue
                domain = _domain(url)
                if not domain or domain in seen_domains:
                    continue
                seen_urls.add(key)
                seen_domains.add(domain)
                searched_urls.append(url)

            if len(searched_urls) >= max_candidates * 2:
                break

        candidates: list[dict[str, Any]] = []
        for url in searched_urls:
            if len(candidates) >= max_candidates:
                break
            try:
                candidate = _extract_org_candidate(url=url, timeout=request_timeout)
            except Exception:
                continue
            if not candidate:
                continue
            candidates.append(candidate)

        upserted_count = 0
        if not dry_run:
            for item in candidates:
                try:
                    upsert_org(
                        name=item.get("name"),
                        homepage=item.get("homepage"),
                        events_url=item.get("events_url"),
                        description=item.get("description"),
                        borough=item.get("borough"),
                        category=item.get("category"),
                        source=item.get("source", "auto_discovery"),
                    )
                    upserted_count += 1
                except Exception:
                    continue

        summary = {
            "run_id": run_id,
            "status": "success",
            "query_count": len(queries),
            "query_errors": query_errors,
            "searched_url_count": len(searched_urls),
            "candidate_count": len(candidates),
            "upserted_count": upserted_count,
            "dry_run": dry_run,
        }
        finish_discovery_run(
            run_id,
            status="success",
            result_count=len(candidates),
            upserted_count=upserted_count,
            details=summary,
        )
        return summary

    except Exception as exc:
        message = str(exc)[:500]
        finish_discovery_run(
            run_id,
            status="failed",
            result_count=0,
            upserted_count=0,
            error=message,
            details={"status": "failed", "error": message},
        )
        raise


def main():
    parser = argparse.ArgumentParser(description="Discover candidate orgs")
    parser.add_argument("--borough", help="Filter to one borough")
    parser.add_argument("--category", help="Filter to one category")
    parser.add_argument("--from-file", help="Import orgs from a JSON file")
    parser.add_argument("--print-queries", action="store_true", help="Print discovery queries")
    parser.add_argument("--export-queries", help="Export discovery queries to JSON file")
    parser.add_argument("--run-once", action="store_true", help="Run one automatic discovery cycle")
    parser.add_argument("--dry-run", action="store_true", help="Discover candidates without writing to DB")
    parser.add_argument("--trigger", default="manual", help="Trigger label for run logs (manual|scheduled)")
    parser.add_argument("--max-queries", type=int)
    parser.add_argument("--max-results-per-query", type=int)
    parser.add_argument("--max-candidates", type=int)
    parser.add_argument("--timeout", type=int)
    parser.add_argument("--add-strategy", help="Optional quick way to add and activate a new strategy note")
    args = parser.parse_args()

    init_db()

    if args.add_strategy:
        add_strategy(_clean_text(args.add_strategy), active=True)

    if args.from_file:
        import_from_file(args.from_file)
        print(f"\nDatabase stats: {get_stats()}")
        return

    boroughs = [args.borough] if args.borough else None
    categories = [args.category] if args.category else None

    queries = _build_discovery_queries(max_queries=args.max_queries or _env_int("DISCOVERY_MAX_QUERIES", 16), boroughs=boroughs, categories=categories)

    if args.print_queries:
        print_queries(queries)
        print(f"\nTotal queries: {len(queries)}")
        return

    if args.export_queries:
        with open(args.export_queries, "w") as handle:
            json.dump(queries, handle, indent=2)
        print(f"Exported {len(queries)} queries to {args.export_queries}")
        return

    if args.run_once:
        summary = run_discovery_cycle(
            trigger=args.trigger,
            max_queries=args.max_queries,
            max_results_per_query=args.max_results_per_query,
            max_candidates=args.max_candidates,
            request_timeout=args.timeout,
            dry_run=args.dry_run,
            borough=args.borough,
            category=args.category,
        )
        print(json.dumps(summary, indent=2))
        return

    print(f"Generated {len(queries)} discovery queries")
    print("Run automatic discovery with: python -m app.discover --run-once")


if __name__ == "__main__":
    main()
