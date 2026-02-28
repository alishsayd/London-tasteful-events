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
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from app.db import (
    add_strategy,
    finish_discovery_run,
    get_latest_running_discovery_run,
    get_stats,
    get_strategies,
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
    "site:lectures.london London lectures",
    "site:lectures.london London talks",
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

AGGREGATOR_SOURCE_DOMAINS = {
    "ianvisits.co.uk",
    "lectures.london",
}

# Domains that are useful for discovery seeding but should never be persisted as org entities.
NON_ENTITY_SOURCE_DOMAINS = {
    "ianvisits.co.uk",
    "lectures.london",
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

ARTICLE_PATH_HINTS = (
    "/article",
    "/articles",
    "/blog",
    "/blogs",
    "/news",
    "/story",
    "/stories",
    "/guide",
    "/guides",
    "/feature",
    "/features",
    "/opinion",
    "/review",
    "/reviews",
    "/category/",
    "/tag/",
)

ARTICLE_TITLE_HINTS = (
    "the best ",
    "top ",
    "things to do",
    "roundup",
    "guide to",
    "list of",
    "best of",
)

PROGRAM_HINTS = (
    "friday lates",
    "late-night",
    "late night",
    "open day",
    "special event",
    "one-off",
    "one off",
)

PROGRAM_PATH_HINTS = (
    "/event/",
    "/events/",
    "/whatson/",
    "/whats-on/",
    "/programme/",
    "/program/",
    "/calendar/",
)

ORG_SCHEMA_TYPES = {
    "organization",
    "localbusiness",
    "performinggroup",
    "artgallery",
    "museum",
    "movietheater",
    "eventvenue",
    "civicstructure",
    "touristattraction",
    "library",
    "placeofworship",
    "educationalorganization",
    "collegeoruniversity",
    "highschool",
    "school",
    "charity",
    "nonprofit",
}


def _clean_text(value: str | None) -> str:
    text_value = str(value or "")
    text_value = re.sub(r"\s+", " ", text_value)
    return text_value.strip()


def _normalize_token(value: str | None) -> str:
    cleaned = _clean_text(value).lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


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


def _domain_root_label(url: str | None) -> str:
    domain = _domain(url)
    if not domain:
        return ""
    return _clean_text(domain.split(".")[0])


def _name_matches_domain(name: str, url: str) -> bool:
    domain_label = _normalize_token(_domain_root_label(url))
    if len(domain_label) < 3:
        return False

    normalized_name = _normalize_token(name)
    compact_name = normalized_name.replace(" ", "")
    if domain_label in normalized_name or normalized_name in domain_label:
        return True
    if 2 <= len(compact_name) <= 4 and compact_name in domain_label:
        return True
    return False


def _word_count(value: str | None) -> int:
    return len([part for part in _normalize_token(value).split(" ") if part])


def _looks_like_article_title(value: str | None) -> bool:
    lower = _normalize_token(value)
    if not lower:
        return False
    return any(hint in lower for hint in ARTICLE_TITLE_HINTS)


def _looks_like_program_text(value: str | None) -> bool:
    lower = _normalize_token(value)
    if not lower:
        return False
    if any(hint in lower for hint in PROGRAM_HINTS):
        return True

    tokens = lower.split(" ")
    return (
        ("friday" in tokens and "lates" in tokens)
        or ("late" in tokens and "night" in tokens)
        or ("open" in tokens and "day" in tokens)
    )


def _looks_like_article_or_listicle(url: str, title: str, description: str, page_text: str) -> bool:
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    if any(hint in path for hint in ARTICLE_PATH_HINTS):
        return True

    if _looks_like_article_title(title):
        return True

    lower_blob = _normalize_token(" ".join([title, description, page_text[:1200]]))
    article_signals = (
        "read more",
        "published",
        "share this article",
        "newsletter",
    )
    score = 0
    if "author" in lower_blob:
        score += 1
    for signal in article_signals:
        if signal in lower_blob:
            score += 1
    return score >= 2


def _looks_like_program_page(url: str, title: str, name: str) -> bool:
    path = (urlparse(url).path or "").lower()
    if any(hint in path for hint in PROGRAM_PATH_HINTS):
        if _looks_like_program_text(title) or _looks_like_program_text(name):
            return True
    return _looks_like_program_text(title) and _word_count(name) >= 2


def _schema_payload_has_org_type(payload: Any) -> bool:
    if isinstance(payload, list):
        return any(_schema_payload_has_org_type(item) for item in payload)

    if not isinstance(payload, dict):
        return False

    item_type = payload.get("@type")
    if isinstance(item_type, str):
        if _normalize_token(item_type).replace(" ", "") in ORG_SCHEMA_TYPES:
            return True
    elif isinstance(item_type, list):
        for value in item_type:
            if isinstance(value, str) and _normalize_token(value).replace(" ", "") in ORG_SCHEMA_TYPES:
                return True

    for key in ("@graph", "mainEntity", "itemListElement", "about", "publisher"):
        if key in payload and _schema_payload_has_org_type(payload[key]):
            return True

    return False


def _has_org_schema_markup(soup: BeautifulSoup) -> bool:
    for script in soup.select("script[type='application/ld+json']"):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if _schema_payload_has_org_type(payload):
            return True
    return False


def _site_name_from_meta(soup: BeautifulSoup) -> str | None:
    tag = soup.find("meta", attrs={"property": "og:site_name"})
    if not tag:
        return None
    value = _clean_text(tag.get("content"))
    return value or None


def _is_valid_org_name(name: str) -> bool:
    cleaned = _clean_text(name)
    if len(cleaned) < 3 or len(cleaned) > 90:
        return False

    if _word_count(cleaned) > 7:
        return False

    lower = _normalize_token(cleaned)
    if not lower:
        return False

    if _looks_like_article_title(lower):
        return False

    if _looks_like_program_text(lower) and _word_count(cleaned) > 2:
        return False

    blocked_exact = {"home", "events", "what s on", "whats on", "calendar", "program", "programme"}
    return lower not in blocked_exact


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

    def _collect_urls(anchors) -> None:
        for anchor in anchors:
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

    primary_selectors = (
        "a.result__a",
        "a[data-testid='result-title-a']",
        ".result__title a[href]",
        "h2 a[href]",
    )
    _collect_urls(soup.select(", ".join(primary_selectors)))

    # DuckDuckGo markup can vary; fall back to scanning all links if primary selectors fail.
    if len(urls) < max_results:
        _collect_urls(soup.select("a[href]"))

    return urls


def _expand_from_aggregator(url: str, timeout: int, max_results: int) -> list[str]:
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    response.raise_for_status()

    if not _looks_like_html_response(response):
        return []

    page_url = response.url
    source_domain = _domain(page_url) or _domain(url)
    if source_domain not in AGGREGATOR_SOURCE_DOMAINS:
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    scored: list[tuple[int, str]] = []
    seen: set[str] = set()

    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "").strip()
        if not href:
            continue

        resolved = urljoin(page_url, href)
        if _should_skip_url(resolved):
            continue

        target_domain = _domain(resolved)
        if not target_domain or target_domain == source_domain:
            continue
        if target_domain in BLOCKED_DOMAINS:
            continue

        key = resolved.strip().lower()
        if key in seen:
            continue
        seen.add(key)

        path = (urlparse(resolved).path or "").lower()
        anchor_text = _clean_text(anchor.get_text(" ", strip=True)).lower()
        score = 0
        if any(hint in path for hint in EVENT_URL_HINTS):
            score += 4
        if any(hint in anchor_text for hint in ("event", "what's on", "what’s on", "programme", "program", "calendar", "talk")):
            score += 3
        if "london" in path or "london" in anchor_text:
            score += 2

        scored.append((score, resolved))

    scored.sort(key=lambda item: (-item[0], item[1]))
    return [url for _, url in scored[:max_results]]


def _extract_title_and_description(soup: BeautifulSoup) -> tuple[str, str]:
    title_tag = soup.find("title")
    title = _clean_text(title_tag.get_text(" ", strip=True) if title_tag else "")

    desc_meta = soup.find("meta", attrs={"name": "description"})
    if not desc_meta:
        desc_meta = soup.find("meta", attrs={"property": "og:description"})
    description = _clean_text(desc_meta.get("content") if desc_meta else "")

    return title, description


def _extract_name(page_url: str, soup: BeautifulSoup, title: str) -> str | None:
    site_name = _site_name_from_meta(soup)
    if site_name and _is_valid_org_name(site_name):
        return site_name

    if title:
        split_parts: list[str] = []
        for sep in ("|", " - ", "—", "·"):
            if sep in title:
                split_parts = [_clean_text(part) for part in title.split(sep) if _clean_text(part)]
                break

        if split_parts:
            if site_name:
                for part in split_parts:
                    if _normalize_token(part) == _normalize_token(site_name) and _is_valid_org_name(part):
                        return part

            if len(split_parts) >= 2:
                left = split_parts[0]
                right = split_parts[-1]
                if (_looks_like_program_text(left) or _looks_like_article_title(left)) and _is_valid_org_name(right):
                    return right

            for part in split_parts:
                if _is_valid_org_name(part) and not _looks_like_program_text(part):
                    return part

        if _is_valid_org_name(title) and not _looks_like_article_title(title):
            return title

    host = _domain(page_url)
    if host:
        base = host.split(".")[0].replace("-", " ").strip()
        base = _clean_text(base.title())
        if _is_valid_org_name(base):
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

    resolved_domain = _domain(response.url)
    if resolved_domain in NON_ENTITY_SOURCE_DOMAINS:
        return None

    if not _looks_like_html_response(response):
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    title, description = _extract_title_and_description(soup)

    page_text = _clean_text(soup.get_text(" ", strip=True))
    text_blob = _clean_text(" ".join([title, description, page_text[:8000], url]))
    if not _is_london_related(text_blob):
        return None

    name = _extract_name(response.url, soup, title)
    if not name:
        return None
    if not _is_valid_org_name(name):
        return None

    homepage = _normalize_homepage(response.url)
    events_url = _extract_events_url(response.url, soup)
    borough = _infer_borough(text_blob)
    category = _infer_category(text_blob)

    article_like = _looks_like_article_or_listicle(response.url, title, description, page_text)
    program_like = _looks_like_program_page(response.url, title, name)
    domain_match = _name_matches_domain(name, response.url)
    has_schema = _has_org_schema_markup(soup)

    if article_like and not domain_match and not has_schema:
        return None
    if program_like and not domain_match and not has_schema:
        return None

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

    def _extract_quoted_phrases(value: str) -> list[str]:
        results: list[str] = []
        seen: set[str] = set()
        for groups in re.findall(r'"([^"]+)"|“([^”]+)”|\'([^\']+)\'', value):
            phrase = _clean_text(next((part for part in groups if part), ""))
            if len(phrase) < 3:
                continue
            key = phrase.lower()
            if key in seen:
                continue
            seen.add(key)
            results.append(phrase)
        return results

    def _center_variants(value: str) -> list[str]:
        variants = {_clean_text(value)}
        replacements = (
            ("centers", "centres"),
            ("center", "centre"),
            ("centres", "centers"),
            ("centre", "center"),
        )
        for old, new in replacements:
            if re.search(rf"\b{old}\b", value, flags=re.IGNORECASE):
                variants.add(re.sub(rf"\b{old}\b", new, value, flags=re.IGNORECASE))
        return [item for item in variants if item]

    def _dedupe_pool(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in items:
            query = _clean_text(item.get("query"))
            if not query:
                continue
            key = query.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append({**item, "query": query})
        return out

    strategy_pools: list[list[dict[str, Any]]] = []
    for strategy in active_strategies:
        text_value = _clean_text(strategy.get("text"))
        if not text_value:
            continue

        strategy_id = int(strategy["id"])
        strategy_queries: list[dict[str, Any]] = []

        # Prioritise quoted entities first (e.g. "Japan House").
        for phrase in _extract_quoted_phrases(text_value):
            strategy_queries.extend(
                [
                    {
                        "query": f"\"{phrase}\" London cultural centre events",
                        "source": "strategy_phrase",
                        "strategy_id": strategy_id,
                    },
                    {
                        "query": f"\"{phrase}\" London cultural center events",
                        "source": "strategy_phrase",
                        "strategy_id": strategy_id,
                    },
                    {
                        "query": f"\"{phrase}\" London what's on",
                        "source": "strategy_phrase",
                        "strategy_id": strategy_id,
                    },
                ]
            )

        for variant in _center_variants(text_value):
            strategy_queries.append(
                {
                    "query": f"{variant} London cultural events venue",
                    "source": "strategy",
                    "strategy_id": strategy_id,
                }
            )

        strategy_queries = _dedupe_pool(strategy_queries)
        if strategy_queries:
            strategy_pools.append(strategy_queries)

    strategy_pool: list[dict[str, Any]] = []
    strategy_positions = [0 for _ in strategy_pools]
    while strategy_pools:
        progressed = False
        for idx, pool in enumerate(strategy_pools):
            position = strategy_positions[idx]
            if position >= len(pool):
                continue
            strategy_pool.append(pool[position])
            strategy_positions[idx] += 1
            progressed = True
        if not progressed:
            break

    strategy_pool = _dedupe_pool(strategy_pool)
    aggregator_pool = _dedupe_pool([{"query": q, "source": "aggregator"} for q in AGGREGATOR_QUERIES])

    grid_pool: list[dict[str, Any]] = []
    grid = [item for item in generate_queries(boroughs=boroughs, categories=categories) if item.get("source") == "borough_search"]
    if grid:
        start = date.today().toordinal() % len(grid)
        rotate_count = min(len(grid), max_queries * 4)
        for idx in range(rotate_count):
            grid_pool.append(grid[(start + idx) % len(grid)])
    grid_pool = _dedupe_pool(grid_pool)

    selected: list[dict[str, Any]] = []
    selected_keys: set[str] = set()

    strategy_idx = 0
    grid_idx = 0
    aggregator_idx = 0

    def _add_from_pool(pool: list[dict[str, Any]], idx: int, target_count: int) -> int:
        added = 0
        while idx < len(pool) and added < target_count and len(selected) < max_queries:
            item = pool[idx]
            idx += 1
            key = item["query"].lower()
            if key in selected_keys:
                continue
            selected.append(item)
            selected_keys.add(key)
            added += 1
        return idx

    floor_strategy = 1 if strategy_pool and max_queries >= 4 else 0
    floor_grid = 2 if grid_pool and max_queries >= 8 else (1 if grid_pool and max_queries >= 4 else 0)
    floor_aggregator = 1 if aggregator_pool and max_queries >= 4 else 0

    while floor_strategy + floor_grid + floor_aggregator > max_queries:
        if floor_aggregator > 0:
            floor_aggregator -= 1
        elif floor_grid > 0:
            floor_grid -= 1
        elif floor_strategy > 0:
            floor_strategy -= 1
        else:
            break

    strategy_idx = _add_from_pool(strategy_pool, strategy_idx, floor_strategy)
    grid_idx = _add_from_pool(grid_pool, grid_idx, floor_grid)
    aggregator_idx = _add_from_pool(aggregator_pool, aggregator_idx, floor_aggregator)

    while len(selected) < max_queries:
        before = len(selected)
        strategy_idx = _add_from_pool(strategy_pool, strategy_idx, 1)
        if len(selected) >= max_queries:
            break
        grid_idx = _add_from_pool(grid_pool, grid_idx, 1)
        if len(selected) >= max_queries:
            break
        aggregator_idx = _add_from_pool(aggregator_pool, aggregator_idx, 1)
        if len(selected) == before:
            break

    return selected[:max_queries]


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


def _parse_discovery_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


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
    lock_window_minutes = _env_int("DISCOVERY_RUN_LOCK_MINUTES", 90)
    manual_unlock_minutes = _env_int("DISCOVERY_MANUAL_UNLOCK_MINUTES", 5)

    running = get_latest_running_discovery_run()
    if running:
        started_at = _parse_discovery_dt(running.get("started_at"))
        age_minutes = None
        if started_at:
            age_minutes = int((datetime.now(timezone.utc) - started_at).total_seconds() // 60)

        is_within_lock = age_minutes is None or age_minutes < lock_window_minutes
        if is_within_lock:
            can_manual_unlock = trigger == "manual" and age_minutes is not None and age_minutes >= manual_unlock_minutes
            if can_manual_unlock:
                finish_discovery_run(
                    run_id=int(running["id"]),
                    status="failed",
                    result_count=int(running.get("result_count") or 0),
                    upserted_count=int(running.get("upserted_count") or 0),
                    error=f"Marked stale by manual retry after {age_minutes} minutes",
                    details={
                        "status": "failed",
                        "reason": "stale_lock_cleared_by_manual_retry",
                        "age_minutes": age_minutes,
                    },
                )
            else:
                return {
                    "status": "skipped",
                    "reason": "another discovery run is still active",
                    "query_count": 0,
                    "searched_url_count": 0,
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
                details={
                    "status": "failed",
                    "reason": "stale_lock_timeout",
                    "lock_window_minutes": lock_window_minutes,
                    "age_minutes": age_minutes,
                },
            )

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
    aggregator_seed_urls: list[str] = []
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
                if domain in AGGREGATOR_SOURCE_DOMAINS:
                    aggregator_seed_urls.append(url)
                else:
                    searched_urls.append(url)

            if len(searched_urls) >= max_candidates * 2:
                break

        for seed_url in aggregator_seed_urls:
            if len(searched_urls) >= max_candidates * 2:
                break

            try:
                expanded = _expand_from_aggregator(seed_url, timeout=request_timeout, max_results=max_results_per_query * 5)
            except Exception:
                continue

            for url in expanded:
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
