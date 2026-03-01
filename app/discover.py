"""Automatic organisation discovery.

This module can:
1. Generate discovery queries for a wide and diverse London net.
2. Retrieve candidate pages from cheap search (DuckDuckGo).
3. Treat listicles/aggregators/marketplaces as lead sources only.
4. Extract and resolve entity candidates to official domains.
5. Classify domains into recurring places vs large one-off events.
6. Upsert valid entities into the org database.

Usage:
    python -m app.discover --run-once
    python -m app.discover --run-once --max-queries 80 --max-candidates 300
    python -m app.discover --from-file results.json
    python -m app.discover --print-queries
"""

from __future__ import annotations

import argparse
import hashlib
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
    get_cached_value,
    get_latest_running_discovery_run,
    get_stats,
    get_strategies,
    init_db,
    set_cached_value,
    start_discovery_run,
    upsert_org,
)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"

CACHE_VERSION = "v4"
QUERY_CACHE_TTL_DAYS = 30
SERP_CACHE_TTL_DAYS = 30
BUNDLE_CACHE_TTL_DAYS = 90
LLM_CLASSIFY_CACHE_TTL_DAYS = 180
LEAD_EXTRACT_CACHE_TTL_DAYS = 180

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

# Regions provide diversity without exploding query volume.
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
    "Shoreditch",
    "King's Cross",
    "Canary Wharf",
]

QUERY_FAMILIES = [
    "independent art centres",
    "contemporary art galleries",
    "photography galleries",
    "architecture centres",
    "architecture talk venues",
    "design museums",
    "historic houses with cultural programmes",
    "public lecture theatres",
    "science museums with talks",
    "science public lecture venues",
    "literary salons",
    "book events venues",
    "writer houses",
    "poetry reading venues",
    "chamber music venues",
    "classical music venues",
    "contemporary dance venues",
    "experimental theatre venues",
    "cinematheques",
    "film institutes",
    "repertory cinemas",
    "independent cinemas",
    "community cinema venues",
    "craft workshops with classes",
    "ceramics studios with classes",
    "printmaking studios",
    "woodworking workshops",
    "textile craft workshops",
    "metalworking workshops",
    "cultural institutes",
    "country cultural centres",
    "diaspora arts centres",
    "community arts venues",
    "creative learning centres",
    "museum late openings venues",
    "galleries with talks programmes",
    "parks with events programmes",
    "public parks with cultural events",
    "friends of park events",
    "park talks and walks programmes",
    "gardens with evening events",
    "conservatories with events",
    "wine tasting venues",
    "food culture institutes",
    "heritage centres",
    "open house architecture venues",
    "music education venues with public concerts",
    "artist-run spaces",
    "small performance spaces",
    "lecture series venues",
    "independent bookshop event venues",
    "philosophy and ideas venues",
    "urbanism and city culture venues",
    "makerspaces with public workshops",
]

ONE_OFF_EVENT_FAMILIES = [
    "food festivals",
    "design festivals",
    "architecture festivals",
    "film festivals",
    "literary festivals",
    "art fairs",
    "craft fairs",
    "music festivals",
    "5K runs",
    "10K runs",
    "half marathons",
    "charity runs",
    "citywide cultural festivals",
    "open house festivals",
    "public outdoor cultural festivals",
]

QUERY_STEMS = [
    "10 {family} in London",
    "best places for {family} events in London",
    "London venues known for {family}",
]

ONE_OFF_QUERY_STEMS = [
    "major {family} in London official site",
    "London {family} official event page",
    "upcoming {family} in London official website",
]

EVENT_LINK_HINTS = (
    "events",
    "whatson",
    "whats-on",
    "what's-on",
    "programme",
    "program",
    "calendar",
    "activities",
    "activities-events",
    "listings",
    "screenings",
    "talks",
    "exhibitions",
    "diary",
    "festival",
)

ABOUT_LINK_HINTS = (
    "about",
    "visit",
    "who-we-are",
    "our-story",
    "location",
    "contact",
)

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

SEARCH_IGNORE_DOMAINS = {
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
    "pinterest.com",
}

# These domains can be used as discovery leads but never persisted as entities.
NON_ENTITY_DOMAIN_SUFFIXES = (
    "ianvisits.co.uk",
    "lectures.london",
    "eventbrite.com",
    "ticketmaster.co.uk",
    "ticketmaster.com",
    "feverup.com",
    "designmynight.com",
    "secretldn.com",
    "timeout.com",
    "culturecalling.com",
    "eventindustrynews.com",
    "theguardian.com",
    "guardian.co.uk",
    "ft.com",
    "wikipedia.org",
    "gov.uk",
    "londonist.com",
    "visitlondon.com",
    "thatsup.co.uk",
)

LEAD_SOURCE_DOMAIN_SUFFIXES = (
    "ianvisits.co.uk",
    "lectures.london",
    "eventbrite.com",
    "ticketmaster.co.uk",
    "ticketmaster.com",
    "feverup.com",
    "designmynight.com",
    "secretldn.com",
    "timeout.com",
    "culturecalling.com",
    "eventindustrynews.com",
    "theguardian.com",
    "guardian.co.uk",
    "ft.com",
    "wikipedia.org",
    "gov.uk",
)

REJECT_ENTITY_DOMAIN_SUFFIXES = (
    "github.com",
    "bsky.app",
    "bsky.social",
    "blueskyweb.xyz",
    "x.com",
    "twitter.com",
    "instagram.com",
    "facebook.com",
    "youtube.com",
    "eventbrite.com",
    "ticketmaster.co.uk",
    "ticketmaster.com",
    "feverup.com",
    "designmynight.com",
    "secretldn.com",
    "timeout.com",
    "culturecalling.com",
    "eventindustrynews.com",
    "ianvisits.co.uk",
    "lectures.london",
    "theguardian.com",
    "guardian.co.uk",
    "ft.com",
    "wikipedia.org",
    "gov.uk",
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
    "educationalorganization",
    "collegeoruniversity",
    "charity",
    "nonprofit",
}

PLACE_KEYWORDS = (
    "museum",
    "gallery",
    "cinema",
    "theatre",
    "theater",
    "cultural centre",
    "cultural center",
    "cultural institute",
    "arts centre",
    "arts center",
    "art centre",
    "art center",
    "foundation",
    "institute",
    "community",
    "bookshop",
    "library",
    "venue",
    "park",
)

ONE_OFF_EVENT_KEYWORDS = (
    "festival",
    "5k",
    "10k",
    "half marathon",
    "marathon",
    "carnival",
    "biennale",
    "triennale",
    "fair",
    "expo",
    "weekender",
    "open house",
)

BOROUGH_ALIASES = {
    "kensington and chelsea": ["kensington & chelsea", "kensington and chelsea"],
}

CATEGORY_MAP = {
    "gallery": ["gallery", "exhibition", "contemporary art", "photography"],
    "museum": ["museum", "heritage", "archive"],
    "cinema": ["cinema", "film", "screening", "cinematheque", "repertory"],
    "theatre": ["theatre", "theater", "dance", "performance"],
    "bookshop": ["bookshop", "bookstore", "literary", "poetry", "writer"],
    "music venue": ["music", "concert", "chamber", "classical"],
    "workshop space": ["workshop", "studio", "classes", "makerspace", "craft"],
    "cultural centre": ["cultural centre", "cultural center", "cultural institute", "diaspora"],
    "community space": ["community", "charity", "social", "collective"],
    "lecture venue": ["lecture", "talk", "ideas", "debate", "public programme"],
    "park": ["park", "friends of", "outdoor events", "walks programme"],
    "garden": ["garden", "conservatory", "horticulture"],
    "one-off event": ["festival", "5k", "10k", "marathon", "fair", "biennale", "expo"],
}

BAD_NAME_PHRASES = {
    "book your tickets",
    "courses and meetings",
    "support",
    "subscribe to read",
    "read more",
    "buy tickets",
    "tickets",
    "event industry news",
    "overview",
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


def _normalize_name(value: str | None) -> str:
    cleaned = _normalize_token(value)
    if cleaned.startswith("the "):
        cleaned = cleaned[4:]
    return cleaned


def _word_count(value: str | None) -> int:
    if not value:
        return 0
    return len([part for part in _normalize_token(value).split(" ") if part])


def _hash_text(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()


def _domain(url: str | None) -> str:
    if not url:
        return ""
    try:
        return (urlparse(url).netloc or "").lower().replace("www.", "")
    except Exception:
        return ""


def _domain_matches_suffix(domain: str | None, suffixes: tuple[str, ...]) -> bool:
    value = str(domain or "").lower().strip().replace("www.", "")
    if not value:
        return False
    return any(value == suffix or value.endswith(f".{suffix}") for suffix in suffixes)


def _canonicalize_url(url: str | None, *, include_path: bool = True) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return ""
        host = (parsed.netloc or "").lower().replace("www.", "")
        if not host:
            return ""
        path = (parsed.path or "/").rstrip("/") if include_path else ""
        path = path or "/"
        return f"{parsed.scheme}://{host}{path}"
    except Exception:
        return ""


def _normalize_homepage(url: str | None) -> str | None:
    host = _domain(url)
    if not host:
        return None
    return f"https://{host}"


def _looks_like_html_response(response: requests.Response) -> bool:
    content_type = str(response.headers.get("content-type") or "").lower()
    return "text/html" in content_type or "application/xhtml+xml" in content_type


def _is_binary_like_path(path: str | None) -> bool:
    lower = str(path or "").lower()
    return lower.endswith((".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".zip", ".mp4", ".mp3"))


def _is_search_ignored_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return True
    domain = (parsed.netloc or "").lower().replace("www.", "")
    if not domain:
        return True
    if domain in SEARCH_IGNORE_DOMAINS:
        return True
    if _is_binary_like_path(parsed.path):
        return True
    return False


def _looks_like_article_title(value: str | None) -> bool:
    lower = _normalize_token(value)
    if not lower:
        return False
    return any(hint in lower for hint in ARTICLE_TITLE_HINTS)


def _extract_output_text_from_response(payload: dict[str, Any]) -> str:
    text_value = _clean_text(payload.get("output_text"))
    if text_value:
        return text_value

    chunks: list[str] = []
    for item in payload.get("output") or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            if content.get("type") == "output_text":
                text = _clean_text(content.get("text"))
                if text:
                    chunks.append(text)

    return "\n".join(chunks).strip()


def _extract_usage_tokens(payload: dict[str, Any]) -> int:
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return 0
    for key in ("total_tokens", "input_tokens", "output_tokens"):
        value = usage.get(key)
        if isinstance(value, int) and value > 0:
            return int(usage.get("total_tokens") or (usage.get("input_tokens", 0) + usage.get("output_tokens", 0)))
    return 0


def _strip_json_fence(raw: str) -> str:
    value = str(raw or "").strip()
    fenced = re.match(r"^```(?:json)?\s*([\s\S]*?)\s*```$", value, flags=re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()
    return value


def _parse_json_payload(raw: str) -> Any:
    text_value = _strip_json_fence(raw)
    if not text_value:
        return None

    try:
        return json.loads(text_value)
    except Exception:
        pass

    for opener, closer in (("[", "]"), ("{", "}")):
        start = text_value.find(opener)
        end = text_value.rfind(closer)
        if start >= 0 and end > start:
            fragment = text_value[start : end + 1]
            try:
                return json.loads(fragment)
            except Exception:
                continue
    return None


def _env_int(key: str, fallback: int) -> int:
    raw = str(os.getenv(key, str(fallback))).strip()
    try:
        parsed = int(raw)
        return parsed if parsed > 0 else fallback
    except Exception:
        return fallback


def _env_bool(key: str, fallback: bool) -> bool:
    raw = str(os.getenv(key, "1" if fallback else "0")).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
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


def _cache_key(prefix: str, *parts: str) -> str:
    joined = "|".join(str(part or "") for part in parts)
    digest = _hash_text(joined)
    return f"{prefix}:{CACHE_VERSION}:{digest}"


def _unwrap_duckduckgo_href(href: str) -> str | None:
    if not href:
        return None

    href = href.strip()
    if href.startswith("//"):
        href = f"https:{href}"

    parsed = urlparse(href)
    if not parsed.netloc and parsed.path.startswith("/l/"):
        query = parse_qs(parsed.query)
        if "uddg" in query and query["uddg"]:
            target = unquote(query["uddg"][0])
            target_parsed = urlparse(target)
            if target_parsed.scheme in {"http", "https"}:
                return target
        return None

    if "duckduckgo.com" not in (parsed.netloc or ""):
        return href if parsed.scheme in {"http", "https"} else None

    query = parse_qs(parsed.query)
    if "uddg" in query and query["uddg"]:
        target = unquote(query["uddg"][0])
        target_parsed = urlparse(target)
        if target_parsed.scheme in {"http", "https"}:
            return target

    return None


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
            if _is_search_ignored_url(href):
                continue
            key = _canonicalize_url(href) or href.lower().strip()
            if key in seen:
                continue
            seen.add(key)
            urls.append(href)
            if len(urls) >= max_results:
                break

    selectors = (
        "a.result__a",
        "a[data-testid='result-title-a']",
        ".result__title a[href]",
        "h2 a[href]",
    )
    _collect_urls(soup.select(", ".join(selectors)))
    if len(urls) < max_results:
        _collect_urls(soup.select("a[href]"))

    return urls[:max_results]


def _search_web(query: str, max_results: int, timeout: int, provider: str) -> list[str]:
    # Search intentionally remains cheap/non-LLM. Legacy provider values are normalized.
    _ = provider
    return _search_duckduckgo(query=query, max_results=max_results, timeout=timeout)


def _cached_serp_search(
    query: str,
    *,
    max_results: int,
    timeout: int,
    provider: str,
    metrics: dict[str, int],
) -> list[str]:
    key = _cache_key("serp", provider, str(max_results), _normalize_token(query))
    cached = get_cached_value(key)
    if isinstance(cached, dict) and isinstance(cached.get("urls"), list):
        metrics["serp_cache_hits"] = metrics.get("serp_cache_hits", 0) + 1
        urls = [str(item) for item in cached.get("urls") if isinstance(item, str)]
        return urls[:max_results]

    metrics["serp_cache_misses"] = metrics.get("serp_cache_misses", 0) + 1
    urls = _search_web(query=query, max_results=max_results, timeout=timeout, provider=provider)
    set_cached_value(key, {"query": query, "urls": urls}, ttl_days=SERP_CACHE_TTL_DAYS)
    return urls[:max_results]


def _openai_request_json(
    *,
    system_prompt: str,
    user_payload: Any,
    max_output_tokens: int,
    timeout: int,
    model: str,
) -> tuple[Any, int]:
    api_key = _clean_text(os.getenv("OPENAI_API_KEY"))
    if not api_key:
        return None, 0

    payload = {
        "model": model,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
            {
                "role": "user",
                "content": [{"type": "input_text", "text": json.dumps(user_payload, ensure_ascii=False)}],
            },
        ],
        "max_output_tokens": max_output_tokens,
    }

    response = requests.post(
        OPENAI_RESPONSES_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=max(20, timeout + 8),
    )
    response.raise_for_status()

    response_payload: dict[str, Any] = response.json()
    raw_text = _extract_output_text_from_response(response_payload)
    parsed = _parse_json_payload(raw_text)
    tokens = _extract_usage_tokens(response_payload)
    return parsed, tokens


def _llm_query_ideation(
    *,
    max_queries: int,
    strategy_notes: list[str],
    boroughs: list[str] | None,
    categories: list[str] | None,
    timeout: int,
) -> tuple[list[dict[str, Any]], int, int]:
    if not _env_bool("DISCOVERY_ENABLE_LLM_QUERY_IDEATION", True):
        return [], 0, 0
    if not _clean_text(os.getenv("OPENAI_API_KEY")):
        return [], 0, 0

    model = _clean_text(os.getenv("DISCOVERY_OPENAI_MODEL") or "gpt-5-mini")
    system_prompt = (
        "Generate diverse London discovery search queries for cultural venues and major one-off events. "
        "Return strict JSON object: {\"queries\": [{\"query\": str, \"entity_kind\": \"place\"|\"one_off_event\", "
        "\"theme\": str}]}. "
        "Target high recall and diversity across arts, culture, science, literature, film, craft, music, community, gardens, "
        "architecture, and country-focused cultural institutes. "
        "Avoid explicit aggregator-domain filters in the query text."
    )
    user_payload = {
        "target_count": max_queries,
        "style_examples": [
            "10 chamber music venues in London",
            "best places for literary salon events in London",
            "London venues known for architecture talks",
            "major 5K runs in London official site",
        ],
        "strategy_notes": strategy_notes[:20],
        "borough_focus": boroughs or REGION_HINTS,
        "category_focus": categories or QUERY_FAMILIES[:20],
    }

    try:
        parsed, tokens = _openai_request_json(
            system_prompt=system_prompt,
            user_payload=user_payload,
            max_output_tokens=2200,
            timeout=timeout,
            model=model,
        )
    except Exception:
        return [], 1, 0
    if not isinstance(parsed, dict):
        return [], 1, tokens

    items = parsed.get("queries")
    if not isinstance(items, list):
        return [], 1, tokens

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        query = _clean_text(item.get("query"))
        if not query:
            continue
        key = query.lower()
        if key in seen:
            continue
        seen.add(key)
        kind = _normalize_token(item.get("entity_kind")).replace(" ", "_")
        if kind not in {"place", "one_off_event"}:
            kind = "place"
        out.append(
            {
                "query": query,
                "source": "llm_query_ideation",
                "entity_kind_hint": kind,
                "theme": _clean_text(item.get("theme")) or None,
            }
        )

    return out, 1, tokens


def _template_queries(
    *,
    max_queries: int,
    boroughs: list[str] | None,
    categories: list[str] | None,
) -> list[dict[str, Any]]:
    families = list(QUERY_FAMILIES)
    one_off_families = list(ONE_OFF_EVENT_FAMILIES)

    if categories:
        category_tokens = {_normalize_token(item) for item in categories if _clean_text(item)}
        if category_tokens:
            families = [
                family
                for family in QUERY_FAMILIES
                if any(token in _normalize_token(family) for token in category_tokens)
            ] or families

    region_pool = boroughs or REGION_HINTS

    generated: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _push(query: str, source: str, entity_kind_hint: str, theme: str | None = None) -> None:
        clean = _clean_text(query)
        if not clean:
            return
        key = clean.lower()
        if key in seen:
            return
        seen.add(key)
        generated.append(
            {
                "query": clean,
                "source": source,
                "entity_kind_hint": entity_kind_hint,
                "theme": _clean_text(theme) or None,
            }
        )

    def _pool_for_place_family(family: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for stem in QUERY_STEMS:
            out.append(
                {
                    "query": stem.format(family=family),
                    "source": "template",
                    "entity_kind_hint": "place",
                    "theme": family,
                }
            )
        for region in region_pool:
            out.append(
                {
                    "query": f"10 {family} in {region}",
                    "source": "template_region",
                    "entity_kind_hint": "place",
                    "theme": family,
                }
            )
            out.append(
                {
                    "query": f"best places for {family} events in {region}",
                    "source": "template_region",
                    "entity_kind_hint": "place",
                    "theme": family,
                }
            )
        return out

    def _pool_for_one_off_family(family: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for stem in ONE_OFF_QUERY_STEMS:
            out.append(
                {
                    "query": stem.format(family=family),
                    "source": "template_one_off",
                    "entity_kind_hint": "one_off_event",
                    "theme": family,
                }
            )
        for region in region_pool[:6]:
            out.append(
                {
                    "query": f"major {family} in {region} official site",
                    "source": "template_one_off_region",
                    "entity_kind_hint": "one_off_event",
                    "theme": family,
                }
            )
        return out

    place_pools = [_pool_for_place_family(family) for family in families]
    one_off_pools = [_pool_for_one_off_family(family) for family in one_off_families]

    # Round-robin across families so small/medium max_queries still include diverse themes.
    all_pools = place_pools + one_off_pools
    positions = [0 for _ in all_pools]
    target = max(max_queries, 1)

    while len(generated) < target:
        progressed = False
        for idx, pool in enumerate(all_pools):
            if len(generated) >= target:
                break
            position = positions[idx]
            if position >= len(pool):
                continue
            positions[idx] += 1
            item = pool[position]
            _push(
                item.get("query", ""),
                item.get("source", "template"),
                item.get("entity_kind_hint", "place"),
                item.get("theme"),
            )
            progressed = True
        if not progressed:
            break

    return generated


def generate_queries(boroughs=None, categories=None):
    """Legacy helper kept for CLI compatibility."""
    return _template_queries(max_queries=120, boroughs=boroughs, categories=categories)


def _build_discovery_queries(max_queries: int, boroughs=None, categories=None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    max_queries = max(10, min(120, int(max_queries)))

    strategy_notes = [_clean_text(item.get("text")) for item in get_strategies() if item.get("active")]
    strategy_notes = [item for item in strategy_notes if item]

    cache_key = _cache_key(
        "query_set",
        str(max_queries),
        json.dumps(sorted(strategy_notes), ensure_ascii=False),
        json.dumps(sorted(boroughs or []), ensure_ascii=False),
        json.dumps(sorted(categories or []), ensure_ascii=False),
    )
    cached = get_cached_value(cache_key)
    if isinstance(cached, dict) and isinstance(cached.get("queries"), list):
        cached_queries = [item for item in cached.get("queries") if isinstance(item, dict) and _clean_text(item.get("query"))]
        if cached_queries:
            return cached_queries[:max_queries], {
                "cache_hit": True,
                "llm_calls": 0,
                "llm_tokens": 0,
                "strategy_count": len(strategy_notes),
            }

    template_pool = _template_queries(max_queries=max_queries * 3, boroughs=boroughs, categories=categories)
    llm_pool, llm_calls, llm_tokens = _llm_query_ideation(
        max_queries=min(120, max_queries * 2),
        strategy_notes=strategy_notes,
        boroughs=boroughs,
        categories=categories,
        timeout=_env_int("DISCOVERY_LLM_TIMEOUT", 25),
    )

    merged: list[dict[str, Any]] = []
    seen: set[str] = set()

    # Interleave LLM and templates for diversity + safety fallback.
    llm_idx = 0
    template_idx = 0
    while len(merged) < max_queries:
        advanced = False
        for source_name in ("llm", "template", "template"):
            if len(merged) >= max_queries:
                break
            if source_name == "llm" and llm_idx < len(llm_pool):
                item = llm_pool[llm_idx]
                llm_idx += 1
            elif source_name.startswith("template") and template_idx < len(template_pool):
                item = template_pool[template_idx]
                template_idx += 1
            else:
                continue

            query = _clean_text(item.get("query"))
            if not query:
                continue
            key = query.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
            advanced = True

        if not advanced:
            break

    if not merged:
        merged = template_pool[:max_queries]

    set_cached_value(
        cache_key,
        {
            "queries": merged,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "strategy_count": len(strategy_notes),
        },
        ttl_days=QUERY_CACHE_TTL_DAYS,
    )

    return merged[:max_queries], {
        "cache_hit": False,
        "llm_calls": llm_calls,
        "llm_tokens": llm_tokens,
        "strategy_count": len(strategy_notes),
    }


def _extract_title_and_description(soup: BeautifulSoup) -> tuple[str, str]:
    title_tag = soup.find("title")
    title = _clean_text(title_tag.get_text(" ", strip=True) if title_tag else "")

    desc_meta = soup.find("meta", attrs={"name": "description"})
    if not desc_meta:
        desc_meta = soup.find("meta", attrs={"property": "og:description"})
    description = _clean_text(desc_meta.get("content") if desc_meta else "")

    return title, description


def _extract_jsonld_types(soup: BeautifulSoup) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def _walk(value: Any) -> None:
        if isinstance(value, dict):
            item_type = value.get("@type")
            if isinstance(item_type, str):
                normalized = _normalize_token(item_type).replace(" ", "")
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    out.append(normalized)
            elif isinstance(item_type, list):
                for part in item_type:
                    if isinstance(part, str):
                        normalized = _normalize_token(part).replace(" ", "")
                        if normalized and normalized not in seen:
                            seen.add(normalized)
                            out.append(normalized)
            for nested in value.values():
                _walk(nested)
        elif isinstance(value, list):
            for item in value:
                _walk(item)

    for script in soup.select("script[type='application/ld+json']"):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        _walk(payload)

    return out[:20]


def _extract_address_snippets(text_blob: str) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set()

    postcode_pattern = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b", flags=re.IGNORECASE)
    for match in postcode_pattern.finditer(text_blob or ""):
        postcode = _clean_text(match.group(1)).upper()
        if not postcode or postcode in seen:
            continue
        seen.add(postcode)
        start = max(0, match.start() - 70)
        end = min(len(text_blob), match.end() + 50)
        snippet = _clean_text((text_blob or "")[start:end])
        if snippet:
            lines.append(snippet)

    return lines[:10]


def _collect_nav_terms(soup: BeautifulSoup) -> list[str]:
    nav_terms: list[str] = []
    seen: set[str] = set()

    nav_anchors = soup.select("nav a, header a")
    if not nav_anchors:
        nav_anchors = soup.select("a")

    for anchor in nav_anchors:
        text_value = _clean_text(anchor.get_text(" ", strip=True))
        if not text_value:
            continue
        lower = text_value.lower()
        if lower in seen:
            continue
        seen.add(lower)
        nav_terms.append(text_value)
        if len(nav_terms) >= 40:
            break

    return nav_terms


def _collect_internal_links(page_url: str, soup: BeautifulSoup, max_links: int = 80) -> list[dict[str, str]]:
    page_domain = _domain(page_url)
    links: list[dict[str, str]] = []
    seen: set[str] = set()

    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "").strip()
        if not href:
            continue
        absolute = urljoin(page_url, href)
        if _domain(absolute) != page_domain:
            continue
        canonical = _canonicalize_url(absolute)
        if not canonical:
            continue
        if canonical in seen:
            continue
        seen.add(canonical)

        links.append(
            {
                "url": canonical,
                "text": _clean_text(anchor.get_text(" ", strip=True))[:120],
            }
        )
        if len(links) >= max_links:
            break

    return links


def _fetch_page_bundle(url: str, timeout: int, metrics: dict[str, int]) -> dict[str, Any] | None:
    canonical = _canonicalize_url(url)
    if not canonical:
        return None

    key = _cache_key("bundle", canonical)
    cached = get_cached_value(key)
    if isinstance(cached, dict) and cached.get("url"):
        metrics["bundle_cache_hits"] = metrics.get("bundle_cache_hits", 0) + 1
        return cached

    metrics["bundle_cache_misses"] = metrics.get("bundle_cache_misses", 0) + 1

    response = requests.get(canonical, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    response.raise_for_status()
    if not _looks_like_html_response(response):
        return None

    final_url = _canonicalize_url(response.url)
    if not final_url:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    title, description = _extract_title_and_description(soup)

    h1_tag = soup.find("h1")
    h1 = _clean_text(h1_tag.get_text(" ", strip=True) if h1_tag else "")

    headings: list[str] = []
    for heading in soup.select("h2, h3"):
        text_value = _clean_text(heading.get_text(" ", strip=True))
        if text_value and text_value.lower() not in {item.lower() for item in headings}:
            headings.append(text_value)
            if len(headings) >= 18:
                break

    body_text = _clean_text(soup.get_text(" ", strip=True))
    footer_text = _clean_text(" ".join(node.get_text(" ", strip=True) for node in soup.select("footer")[:2]))

    site_bundle = {
        "url": final_url,
        "domain": _domain(final_url),
        "title": title,
        "meta_description": description,
        "h1": h1,
        "headings": headings,
        "nav_terms": _collect_nav_terms(soup),
        "footer_snippet": footer_text[:500],
        "jsonld_types": _extract_jsonld_types(soup),
        "address_snippets": _extract_address_snippets(body_text[:12000]),
        "internal_links": _collect_internal_links(final_url, soup, max_links=90),
        "text_preview": body_text[:2200],
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    set_cached_value(key, site_bundle, ttl_days=BUNDLE_CACHE_TTL_DAYS)
    return site_bundle


def _is_lead_source_url(url: str, title: str = "", description: str = "") -> bool:
    domain = _domain(url)
    if _domain_matches_suffix(domain, LEAD_SOURCE_DOMAIN_SUFFIXES):
        return True

    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    if any(hint in path for hint in ARTICLE_PATH_HINTS):
        return True

    lower_blob = _normalize_token(" ".join([title, description, path]))
    if _looks_like_article_title(lower_blob):
        return True

    lead_markers = ("list", "best", "top", "roundup", "things to do", "near me")
    return any(marker in lower_blob for marker in lead_markers)


def _name_candidates_from_text(text: str, limit: int = 20) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    # Title-ish phrases catch many venue names from heading/link text.
    for match in re.findall(r"\b([A-Z][A-Za-z0-9&'\-]{1,}(?:\s+[A-Z][A-Za-z0-9&'\-]{1,}){0,6})\b", text or ""):
        name = _clean_text(match)
        if not name:
            continue
        if _word_count(name) < 2:
            continue
        if len(name) > 90:
            continue
        lower = name.lower()
        if lower in seen:
            continue
        seen.add(lower)
        out.append(name)
        if len(out) >= limit:
            break

    return out


def _extract_lead_entities_with_llm(
    lead_pages: list[dict[str, Any]],
    *,
    timeout: int,
    batch_size: int,
) -> tuple[dict[str, list[dict[str, Any]]], int, int]:
    if not lead_pages:
        return {}, 0, 0
    if not _clean_text(os.getenv("OPENAI_API_KEY")):
        return {}, 0, 0

    model = _clean_text(os.getenv("DISCOVERY_OPENAI_MODEL") or "gpt-5-mini")
    calls = 0
    tokens_total = 0
    extracted: dict[str, list[dict[str, Any]]] = {}

    system_prompt = (
        "You extract candidate London entities from messy lead pages. "
        "Return strict JSON object: {\"items\": [{\"lead_url\": str, \"candidates\": ["
        "{\"name\": str, \"entity_kind\": \"place\"|\"one_off_event\", \"category_hint\": str|null, "
        "\"borough_hint\": str|null, \"confidence\": number, \"reason\": str}]}]}. "
        "Entity rules: keep only real venues/institutions/organizers or major one-off events with official landing pages. "
        "Do not return aggregators, marketplaces, directories, social networks, publishers, or article headings."
    )

    for start in range(0, len(lead_pages), max(1, batch_size)):
        batch = lead_pages[start : start + max(1, batch_size)]
        user_payload = {
            "pages": [
                {
                    "lead_url": item.get("url"),
                    "domain": item.get("domain"),
                    "title": item.get("title"),
                    "meta_description": item.get("meta_description"),
                    "h1": item.get("h1"),
                    "headings": item.get("headings", [])[:10],
                    "top_links": item.get("internal_links", [])[:25],
                    "text_preview": item.get("text_preview", "")[:1200],
                }
                for item in batch
            ]
        }

        try:
            parsed, tokens = _openai_request_json(
                system_prompt=system_prompt,
                user_payload=user_payload,
                max_output_tokens=2600,
                timeout=timeout,
                model=model,
            )
            calls += 1
            tokens_total += tokens
        except Exception:
            continue

        if not isinstance(parsed, dict) or not isinstance(parsed.get("items"), list):
            # Retry once with a stricter compact prompt.
            retry_prompt = system_prompt + " Output must be valid JSON only."
            try:
                parsed, retry_tokens = _openai_request_json(
                    system_prompt=retry_prompt,
                    user_payload=user_payload,
                    max_output_tokens=2600,
                    timeout=timeout,
                    model=model,
                )
                calls += 1
                tokens_total += retry_tokens
            except Exception:
                continue

        if not isinstance(parsed, dict) or not isinstance(parsed.get("items"), list):
            continue

        for item in parsed.get("items", []):
            if not isinstance(item, dict):
                continue
            lead_url = _canonicalize_url(item.get("lead_url"))
            if not lead_url:
                continue
            candidates = item.get("candidates")
            if not isinstance(candidates, list):
                continue
            cleaned: list[dict[str, Any]] = []
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                name = _clean_text(candidate.get("name"))
                if not name or len(name) < 3 or len(name) > 100:
                    continue
                kind = _normalize_token(candidate.get("entity_kind")).replace(" ", "_")
                if kind not in {"place", "one_off_event"}:
                    kind = "place"
                cleaned.append(
                    {
                        "name": name,
                        "entity_kind": kind,
                        "category_hint": _clean_text(candidate.get("category_hint")) or None,
                        "borough_hint": _clean_text(candidate.get("borough_hint")) or None,
                        "confidence": float(candidate.get("confidence") or 0),
                        "reason": _clean_text(candidate.get("reason")) or None,
                    }
                )
            if cleaned:
                extracted[lead_url] = cleaned

    return extracted, calls, tokens_total


def _extract_entities_from_leads(
    lead_bundles: list[dict[str, Any]],
    *,
    timeout: int,
    llm_batch_size: int,
    metrics: dict[str, int],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not lead_bundles:
        return results

    uncached_for_llm: list[dict[str, Any]] = []

    for bundle in lead_bundles:
        cache_fingerprint = _hash_text(
            json.dumps(
                {
                    "u": bundle.get("url"),
                    "t": bundle.get("title"),
                    "h1": bundle.get("h1"),
                    "h2": bundle.get("headings", [])[:8],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        key = _cache_key("lead_extract", str(bundle.get("url")), cache_fingerprint)
        cached = get_cached_value(key)
        if isinstance(cached, dict) and isinstance(cached.get("candidates"), list):
            metrics["lead_extract_cache_hits"] = metrics.get("lead_extract_cache_hits", 0) + 1
            for item in cached.get("candidates", []):
                if not isinstance(item, dict):
                    continue
                name = _clean_text(item.get("name"))
                if not name:
                    continue
                results.append(
                    {
                        "name": name,
                        "entity_kind": _normalize_token(item.get("entity_kind")).replace(" ", "_") or "place",
                        "category_hint": _clean_text(item.get("category_hint")) or None,
                        "borough_hint": _clean_text(item.get("borough_hint")) or None,
                        "source_url": bundle.get("url"),
                    }
                )
            continue

        metrics["lead_extract_cache_misses"] = metrics.get("lead_extract_cache_misses", 0) + 1
        uncached_for_llm.append({"bundle": bundle, "cache_key": key})

    llm_input = [item["bundle"] for item in uncached_for_llm]
    llm_results, llm_calls, llm_tokens = _extract_lead_entities_with_llm(
        llm_input,
        timeout=timeout,
        batch_size=llm_batch_size,
    )
    metrics["llm_calls"] = metrics.get("llm_calls", 0) + llm_calls
    metrics["llm_tokens"] = metrics.get("llm_tokens", 0) + llm_tokens

    for wrapped in uncached_for_llm:
        bundle = wrapped["bundle"]
        key = wrapped["cache_key"]
        lead_url = _canonicalize_url(bundle.get("url"))

        llm_candidates = llm_results.get(lead_url, []) if lead_url else []
        heuristic_candidates = _name_candidates_from_text(
            " ".join(
                [
                    _clean_text(bundle.get("title")),
                    _clean_text(bundle.get("h1")),
                    " ".join(bundle.get("headings", [])[:12]),
                    " ".join(item.get("text", "") for item in bundle.get("internal_links", [])[:40]),
                ]
            ),
            limit=18,
        )

        merged: list[dict[str, Any]] = []
        seen: set[str] = set()

        for item in llm_candidates:
            name = _clean_text(item.get("name"))
            if not name:
                continue
            lower = name.lower()
            if lower in seen:
                continue
            seen.add(lower)
            merged.append(item)

        for name in heuristic_candidates:
            lower = name.lower()
            if lower in seen:
                continue
            seen.add(lower)
            merged.append(
                {
                    "name": name,
                    "entity_kind": "place",
                    "category_hint": None,
                    "borough_hint": None,
                    "confidence": 0.35,
                    "reason": "heuristic_heading_link_name",
                }
            )

        set_cached_value(key, {"candidates": merged}, ttl_days=LEAD_EXTRACT_CACHE_TTL_DAYS)
        for item in merged:
            name = _clean_text(item.get("name"))
            if not name:
                continue
            results.append(
                {
                    "name": name,
                    "entity_kind": _normalize_token(item.get("entity_kind")).replace(" ", "_") or "place",
                    "category_hint": _clean_text(item.get("category_hint")) or None,
                    "borough_hint": _clean_text(item.get("borough_hint")) or None,
                    "source_url": bundle.get("url"),
                }
            )

    deduped: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    for item in results:
        key = _normalize_name(item.get("name"))
        if not key or len(key) < 3:
            continue
        if key in seen_names:
            continue
        seen_names.add(key)
        deduped.append(item)

    return deduped


def _select_best_resolved_url(urls: list[str]) -> str | None:
    for url in urls:
        domain = _domain(url)
        if not domain:
            continue
        if _domain_matches_suffix(domain, REJECT_ENTITY_DOMAIN_SUFFIXES):
            continue
        return _canonicalize_url(url)
    return None


def _resolve_lead_candidates_to_urls(
    candidates: list[dict[str, Any]],
    *,
    max_results_per_query: int,
    timeout: int,
    provider: str,
    max_candidates: int,
    metrics: dict[str, int],
) -> list[dict[str, Any]]:
    resolved: list[dict[str, Any]] = []
    seen_names: set[str] = set()

    for item in candidates:
        if len(resolved) >= max_candidates:
            break

        name = _clean_text(item.get("name"))
        if not name:
            continue
        name_key = _normalize_name(name)
        if not name_key or name_key in seen_names:
            continue
        seen_names.add(name_key)

        query = f'"{name}" London official site events'
        try:
            urls = _cached_serp_search(
                query,
                max_results=max_results_per_query,
                timeout=timeout,
                provider=provider,
                metrics=metrics,
            )
        except Exception:
            continue

        selected = _select_best_resolved_url(urls)
        if not selected:
            continue

        resolved.append(
            {
                "url": selected,
                "seed_name": name,
                "entity_kind_hint": _normalize_token(item.get("entity_kind")).replace(" ", "_") or "place",
                "category_hint": _clean_text(item.get("category_hint")) or None,
                "borough_hint": _clean_text(item.get("borough_hint")) or None,
                "source": "lead_resolve",
            }
        )

    return resolved


def _pick_internal_urls(bundle: dict[str, Any], max_urls: int) -> list[str]:
    domain = _domain(bundle.get("url"))
    if not domain:
        return []

    ranked: list[tuple[int, str]] = []
    seen: set[str] = set()
    for link in bundle.get("internal_links") or []:
        if not isinstance(link, dict):
            continue
        url_value = _canonicalize_url(link.get("url"))
        if not url_value:
            continue
        if _domain(url_value) != domain:
            continue
        if url_value in seen:
            continue
        seen.add(url_value)

        path = (urlparse(url_value).path or "").lower()
        text_value = _normalize_token(link.get("text"))
        score = 0
        for hint in EVENT_LINK_HINTS:
            if hint in path or hint in text_value:
                score += 8
        for hint in ABOUT_LINK_HINTS:
            if hint in path or hint in text_value:
                score += 5
        if path in {"", "/"}:
            score += 1
        ranked.append((score, url_value))

    ranked.sort(key=lambda item: (-item[0], item[1]))
    return [url for _, url in ranked[:max_urls]]


def _build_domain_bundle(
    *,
    domain: str,
    seed_urls: list[str],
    timeout: int,
    max_urls_per_domain: int,
    metrics: dict[str, int],
) -> dict[str, Any] | None:
    canonical_seed_urls: list[str] = []
    seen: set[str] = set()

    homepage = _normalize_homepage(seed_urls[0] if seed_urls else f"https://{domain}")
    if homepage:
        canonical_seed_urls.append(homepage)
        seen.add(homepage)

    for url in seed_urls:
        canonical = _canonicalize_url(url)
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        canonical_seed_urls.append(canonical)

    page_bundles: list[dict[str, Any]] = []

    for url in canonical_seed_urls:
        if len(page_bundles) >= max(1, max_urls_per_domain):
            break
        try:
            bundle = _fetch_page_bundle(url, timeout=timeout, metrics=metrics)
        except Exception:
            continue
        if not bundle:
            continue
        page_bundles.append(bundle)

        for expanded in _pick_internal_urls(bundle, max_urls=max(1, max_urls_per_domain)):
            if len(page_bundles) >= max(1, max_urls_per_domain):
                break
            if expanded in seen:
                continue
            seen.add(expanded)
            try:
                extra = _fetch_page_bundle(expanded, timeout=timeout, metrics=metrics)
            except Exception:
                continue
            if not extra:
                continue
            page_bundles.append(extra)

    if not page_bundles:
        return None

    primary = page_bundles[0]

    merged_nav: list[str] = []
    merged_headings: list[str] = []
    merged_addresses: list[str] = []
    merged_jsonld: list[str] = []
    merged_event_candidates: list[str] = []
    seen_nav: set[str] = set()
    seen_headings: set[str] = set()
    seen_addr: set[str] = set()
    seen_types: set[str] = set()
    seen_event_urls: set[str] = set()

    for page in page_bundles:
        for item in page.get("nav_terms", [])[:25]:
            normalized = _normalize_token(item)
            if not normalized or normalized in seen_nav:
                continue
            seen_nav.add(normalized)
            merged_nav.append(_clean_text(item))

        for item in page.get("headings", [])[:16]:
            normalized = _normalize_token(item)
            if not normalized or normalized in seen_headings:
                continue
            seen_headings.add(normalized)
            merged_headings.append(_clean_text(item))

        for item in page.get("address_snippets", [])[:12]:
            normalized = _normalize_token(item)
            if not normalized or normalized in seen_addr:
                continue
            seen_addr.add(normalized)
            merged_addresses.append(_clean_text(item))

        for item in page.get("jsonld_types", [])[:20]:
            normalized = _normalize_token(item).replace(" ", "")
            if not normalized or normalized in seen_types:
                continue
            seen_types.add(normalized)
            merged_jsonld.append(normalized)

        for link in page.get("internal_links", [])[:60]:
            if not isinstance(link, dict):
                continue
            url_value = _canonicalize_url(link.get("url"))
            if not url_value:
                continue
            path = (urlparse(url_value).path or "").lower()
            text_value = _normalize_token(link.get("text"))
            if any(hint in path or hint in text_value for hint in EVENT_LINK_HINTS):
                if url_value not in seen_event_urls:
                    seen_event_urls.add(url_value)
                    merged_event_candidates.append(url_value)

    site_bundle = {
        "domain": domain,
        "homepage": _normalize_homepage(primary.get("url")) or homepage,
        "seed_urls": canonical_seed_urls[: max(1, max_urls_per_domain * 2)],
        "sample_urls": [item.get("url") for item in page_bundles[: max(1, max_urls_per_domain)] if item.get("url")],
        "title": _clean_text(primary.get("title")),
        "meta_description": _clean_text(primary.get("meta_description")),
        "h1": _clean_text(primary.get("h1")),
        "headings": merged_headings[:30],
        "nav_terms": merged_nav[:40],
        "footer_snippet": _clean_text(primary.get("footer_snippet"))[:500],
        "jsonld_types": merged_jsonld[:30],
        "address_snippets": merged_addresses[:16],
        "events_link_candidates": merged_event_candidates[:25],
        "text_preview": _clean_text(primary.get("text_preview"))[:1800],
    }

    return site_bundle


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


def _infer_category(text_blob: str, entity_kind: str = "place") -> str:
    if entity_kind == "one_off_event":
        return "one-off event"

    lower = _normalize_token(text_blob)
    best = "other"
    best_score = 0
    for category, keywords in CATEGORY_MAP.items():
        if category == "one-off event":
            continue
        score = 0
        for keyword in keywords:
            if _normalize_token(keyword) in lower:
                score += 1
        if score > best_score:
            best = category
            best_score = score
    return best


def _guess_name_from_bundle(bundle: dict[str, Any]) -> str | None:
    title = _clean_text(bundle.get("title"))
    h1 = _clean_text(bundle.get("h1"))

    candidates: list[str] = []
    if h1:
        candidates.append(h1)
    if title:
        for sep in ("|", " - ", "—", "·", ":"):
            if sep in title:
                parts = [_clean_text(part) for part in title.split(sep) if _clean_text(part)]
                candidates.extend(parts)
            else:
                candidates.append(title)
    for heading in bundle.get("headings", [])[:8]:
        if heading:
            candidates.append(_clean_text(heading))

    domain_label = (bundle.get("domain") or "").split(".")[0].replace("-", " ").strip()
    if domain_label:
        candidates.append(_clean_text(domain_label.title()))

    seen: set[str] = set()
    for name in candidates:
        if not name:
            continue
        normalized = _normalize_name(name)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)

        if _word_count(name) == 0 or _word_count(name) > 8:
            continue
        if len(name) < 3 or len(name) > 100:
            continue
        if _looks_like_article_title(name):
            continue
        if any(phrase in normalized for phrase in BAD_NAME_PHRASES):
            continue
        if normalized in {"home", "events", "calendar", "about", "visit", "tickets", "book tickets"}:
            continue
        return name

    return None


def _pick_events_url(bundle: dict[str, Any]) -> str | None:
    for url in bundle.get("events_link_candidates", []):
        path = (urlparse(url).path or "").lower()
        if any(hint in path for hint in EVENT_LINK_HINTS):
            return url
    homepage = _normalize_homepage(bundle.get("homepage") or bundle.get("sample_urls", [""])[0])
    return homepage


def _is_valid_entity_name(name: str | None) -> bool:
    clean = _clean_text(name)
    if len(clean) < 3 or len(clean) > 100:
        return False
    if _word_count(clean) > 8:
        return False

    lower = _normalize_name(clean)
    if not lower:
        return False
    if _looks_like_article_title(lower):
        return False
    if any(phrase in lower for phrase in BAD_NAME_PHRASES):
        return False
    if lower.endswith(" overview"):
        return False
    if lower.startswith(("book ", "support ", "subscribe ", "read ", "view ")):
        return False
    return True


def _heuristic_gate(bundle: dict[str, Any]) -> dict[str, Any]:
    domain = str(bundle.get("domain") or "").strip().lower()
    if not domain:
        return {"decision": "reject", "reason_codes": ["missing_domain"]}

    if _domain_matches_suffix(domain, REJECT_ENTITY_DOMAIN_SUFFIXES):
        return {"decision": "reject", "reason_codes": ["blocked_domain_suffix"]}

    blob = " ".join(
        [
            _clean_text(bundle.get("title")),
            _clean_text(bundle.get("meta_description")),
            _clean_text(bundle.get("h1")),
            " ".join(bundle.get("headings", [])[:12]),
            " ".join(bundle.get("nav_terms", [])[:20]),
            _clean_text(bundle.get("footer_snippet")),
            _clean_text(bundle.get("text_preview")),
        ]
    )
    normalized_blob = _normalize_token(blob)

    reason_codes: list[str] = []
    positive = 0
    negative = 0

    if "london" in normalized_blob:
        positive += 2
        reason_codes.append("mentions_london")

    inferred_borough = _infer_borough(blob)
    if inferred_borough:
        positive += 1
        reason_codes.append("mentions_borough")

    if bundle.get("address_snippets"):
        positive += 2
        reason_codes.append("has_address_signal")

    jsonld_types = {str(item).lower() for item in bundle.get("jsonld_types", []) if item}
    if jsonld_types & ORG_SCHEMA_TYPES:
        positive += 2
        reason_codes.append("has_org_schema")

    if any(keyword in normalized_blob for keyword in (_normalize_token(item) for item in PLACE_KEYWORDS)):
        positive += 2
        reason_codes.append("institution_keywords")

    if bundle.get("events_link_candidates"):
        positive += 2
        reason_codes.append("has_events_links")

    if any(marker in normalized_blob for marker in ("marketplace", "ticket marketplace", "book tickets", "buy tickets")):
        negative += 3
        reason_codes.append("marketplace_language")

    homepage = _normalize_homepage(bundle.get("homepage") or bundle.get("sample_urls", [""])[0])
    if homepage and _is_lead_source_url(homepage, bundle.get("title", ""), bundle.get("meta_description", "")):
        negative += 2
        reason_codes.append("lead_source_pattern")

    if _looks_like_article_title(bundle.get("title")):
        negative += 2
        reason_codes.append("article_title_pattern")

    name_guess = _guess_name_from_bundle(bundle)
    if not _is_valid_entity_name(name_guess):
        negative += 2
        reason_codes.append("weak_name_signal")

    one_off_score = 0
    if any(keyword in normalized_blob for keyword in ONE_OFF_EVENT_KEYWORDS):
        one_off_score += 2
    if "official" in normalized_blob and "festival" in normalized_blob:
        one_off_score += 1

    entity_kind = "one_off_event" if one_off_score >= 2 and positive >= 2 else "place"

    confidence = max(0.0, min(0.99, 0.45 + positive * 0.08 - negative * 0.1 + (0.05 if entity_kind == "one_off_event" else 0.0)))

    entity = {
        "name": name_guess,
        "entity_kind": entity_kind,
        "homepage": homepage,
        "events_url": _pick_events_url(bundle),
        "borough": inferred_borough,
        "category": _infer_category(blob, entity_kind=entity_kind),
        "description": _clean_text(bundle.get("meta_description")) or None,
        "confidence": confidence,
        "reason_codes": reason_codes,
    }

    margin = positive - negative
    if margin >= 4 and confidence >= 0.7 and _is_valid_entity_name(entity.get("name")):
        return {"decision": "accept", "entity": entity, "reason_codes": reason_codes}
    if margin <= -1:
        return {"decision": "reject", "reason_codes": reason_codes}
    return {"decision": "ambiguous", "entity": entity, "reason_codes": reason_codes}


def _domain_fingerprint(bundle: dict[str, Any]) -> str:
    payload = {
        "domain": bundle.get("domain"),
        "title": bundle.get("title"),
        "meta_description": bundle.get("meta_description"),
        "h1": bundle.get("h1"),
        "headings": bundle.get("headings", [])[:15],
        "nav_terms": bundle.get("nav_terms", [])[:20],
        "jsonld_types": bundle.get("jsonld_types", [])[:20],
        "events_link_candidates": bundle.get("events_link_candidates", [])[:8],
    }
    return _hash_text(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _classify_site_bundles_with_llm(
    bundles: list[dict[str, Any]],
    *,
    timeout: int,
    batch_size: int,
) -> tuple[dict[str, dict[str, Any]], int, int]:
    if not bundles:
        return {}, 0, 0
    if not _clean_text(os.getenv("OPENAI_API_KEY")):
        return {}, 0, 0

    model = _clean_text(os.getenv("DISCOVERY_OPENAI_MODEL") or "gpt-5-mini")
    results: dict[str, dict[str, Any]] = {}
    calls = 0
    tokens_total = 0

    system_prompt = (
        "You classify compact site bundles into valid London entities. "
        "Return strict JSON object: {\"items\": [{\"domain\": str, \"is_entity\": bool, "
        "\"entity_kind\": \"place\"|\"one_off_event\"|\"reject\", \"name\": str|null, "
        "\"homepage\": str|null, \"events_url\": str|null, \"borough\": str|null, \"category\": str|null, "
        "\"description\": str|null, \"confidence\": number, \"reason_codes\": [str]}]}. "
        "Exclusions (never entity): aggregators/listicles/marketplaces/directories/social networks/publishers/ticket pages. "
        "A place is a recurring London venue/institution/organizer with a real-world presence. "
        "one_off_event is a major standalone event series or annual event with official landing page."
    )

    for start in range(0, len(bundles), max(1, batch_size)):
        batch = bundles[start : start + max(1, batch_size)]
        user_payload = {
            "bundles": [
                {
                    "domain": item.get("domain"),
                    "homepage": item.get("homepage"),
                    "sample_urls": item.get("sample_urls", [])[:4],
                    "title": item.get("title"),
                    "meta_description": item.get("meta_description"),
                    "h1": item.get("h1"),
                    "headings": item.get("headings", [])[:14],
                    "nav_terms": item.get("nav_terms", [])[:18],
                    "footer_snippet": item.get("footer_snippet", "")[:220],
                    "jsonld_types": item.get("jsonld_types", [])[:20],
                    "address_snippets": item.get("address_snippets", [])[:6],
                    "events_link_candidates": item.get("events_link_candidates", [])[:8],
                    "text_preview": item.get("text_preview", "")[:900],
                    "seed_name_hints": item.get("seed_name_hints", [])[:5],
                }
                for item in batch
            ]
        }

        try:
            parsed, tokens = _openai_request_json(
                system_prompt=system_prompt,
                user_payload=user_payload,
                max_output_tokens=3200,
                timeout=timeout,
                model=model,
            )
            calls += 1
            tokens_total += tokens
        except Exception:
            continue

        if not isinstance(parsed, dict) or not isinstance(parsed.get("items"), list):
            retry_prompt = system_prompt + " Output must be JSON only and parseable."
            try:
                parsed, retry_tokens = _openai_request_json(
                    system_prompt=retry_prompt,
                    user_payload=user_payload,
                    max_output_tokens=3200,
                    timeout=timeout,
                    model=model,
                )
                calls += 1
                tokens_total += retry_tokens
            except Exception:
                continue

        if not isinstance(parsed, dict) or not isinstance(parsed.get("items"), list):
            continue

        for item in parsed.get("items", []):
            if not isinstance(item, dict):
                continue
            domain = str(item.get("domain") or "").strip().lower().replace("www.", "")
            if not domain:
                continue
            kind = _normalize_token(item.get("entity_kind")).replace(" ", "_")
            if kind not in {"place", "one_off_event", "reject"}:
                kind = "reject"
            results[domain] = {
                "is_entity": bool(item.get("is_entity", False)),
                "entity_kind": kind,
                "name": _clean_text(item.get("name")) or None,
                "homepage": _canonicalize_url(item.get("homepage"), include_path=False) or _normalize_homepage(item.get("homepage")),
                "events_url": _canonicalize_url(item.get("events_url")) if item.get("events_url") else None,
                "borough": _clean_text(item.get("borough")) or None,
                "category": _clean_text(item.get("category")) or None,
                "description": _clean_text(item.get("description")) or None,
                "confidence": float(item.get("confidence") or 0),
                "reason_codes": [
                    _clean_text(code)
                    for code in (item.get("reason_codes") or [])
                    if _clean_text(code)
                ][:10],
            }

    return results, calls, tokens_total


def _normalize_borough(value: str | None) -> str | None:
    clean = _clean_text(value)
    if not clean:
        return None
    for borough in BOROUGHS:
        if _normalize_token(clean) == _normalize_token(borough):
            return borough
        for alias in BOROUGH_ALIASES.get(borough.lower(), []):
            if _normalize_token(clean) == _normalize_token(alias):
                return borough
    return None


def _finalize_entity_record(
    *,
    bundle: dict[str, Any],
    entity: dict[str, Any],
    default_source: str,
) -> dict[str, Any] | None:
    name = _clean_text(entity.get("name")) or _guess_name_from_bundle(bundle)
    if not _is_valid_entity_name(name):
        return None

    entity_kind = _normalize_token(entity.get("entity_kind")).replace(" ", "_")
    if entity_kind not in {"place", "one_off_event"}:
        entity_kind = "place"

    homepage = (
        _canonicalize_url(entity.get("homepage"), include_path=False)
        or _normalize_homepage(bundle.get("homepage") or bundle.get("sample_urls", [""])[0])
        or _normalize_homepage(f"https://{bundle.get('domain')}")
    )
    if not homepage:
        return None

    events_url = _canonicalize_url(entity.get("events_url")) or _pick_events_url(bundle) or homepage

    blob = " ".join(
        [
            _clean_text(bundle.get("title")),
            _clean_text(bundle.get("meta_description")),
            _clean_text(bundle.get("h1")),
            " ".join(bundle.get("headings", [])[:12]),
            " ".join(bundle.get("nav_terms", [])[:12]),
        ]
    )

    borough = _normalize_borough(entity.get("borough")) or _infer_borough(blob)

    category_raw = _clean_text(entity.get("category"))
    category = (
        "one-off event"
        if entity_kind == "one_off_event"
        else (category_raw if category_raw else _infer_category(blob, entity_kind=entity_kind))
    )

    description = _clean_text(entity.get("description"))
    if not description:
        if entity_kind == "one_off_event":
            description = f"{name} is a major London one-off event source."
        elif borough:
            description = f"{name} is a London cultural place in {borough}."
        else:
            description = f"{name} is a London cultural place."

    confidence = float(entity.get("confidence") or 0)
    source = "auto_discovery_one_off" if entity_kind == "one_off_event" else default_source

    return {
        "name": name,
        "homepage": homepage,
        "events_url": events_url,
        "description": description,
        "borough": borough,
        "category": category,
        "source": source,
        "entity_kind": entity_kind,
        "confidence": max(0.0, min(1.0, confidence)),
    }


def _dedupe_records(records: list[dict[str, Any]], max_items: int) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for record in records:
        domain = _domain(record.get("homepage") or record.get("events_url"))
        name_key = _normalize_name(record.get("name"))
        if not domain or not name_key:
            continue
        key = f"{domain}|{name_key}"
        existing = by_key.get(key)
        if not existing or float(record.get("confidence") or 0) > float(existing.get("confidence") or 0):
            by_key[key] = record

    out = list(by_key.values())
    out.sort(key=lambda item: (-float(item.get("confidence") or 0), _clean_text(item.get("name")).lower()))
    return out[:max_items]


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
    """Run one full discovery cycle and upsert discovered entities."""

    init_db()

    max_queries = max_queries or _env_int("DISCOVERY_MAX_QUERIES", 120)
    max_queries = max(10, min(120, max_queries))

    max_results_per_query = max_results_per_query or _env_int("DISCOVERY_MAX_RESULTS_PER_QUERY", 8)
    max_candidates = max_candidates or _env_int("DISCOVERY_MAX_CANDIDATES", 320)
    request_timeout = request_timeout or _env_int("DISCOVERY_REQUEST_TIMEOUT", 12)

    max_urls_per_domain = _env_int("DISCOVERY_MAX_URLS_PER_DOMAIN", 3)
    max_lead_pages = _env_int("DISCOVERY_MAX_LEAD_PAGES", 36)
    max_resolved_from_leads = _env_int("DISCOVERY_MAX_RESOLVED_FROM_LEADS", max_candidates)
    llm_batch_size = _env_int("DISCOVERY_LLM_BATCH_SIZE", 12)

    lock_window_minutes = _env_int("DISCOVERY_RUN_LOCK_MINUTES", 90)
    manual_unlock_minutes = _env_int("DISCOVERY_MANUAL_UNLOCK_MINUTES", 5)

    search_provider = _clean_text(search_provider or os.getenv("DISCOVERY_SEARCH_PROVIDER") or "duckduckgo")

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
    queries, query_meta = _build_discovery_queries(max_queries=max_queries, boroughs=boroughs, categories=categories)

    run_id = start_discovery_run(
        query_count=len(queries),
        trigger=trigger,
        details={
            "max_queries": max_queries,
            "max_results_per_query": max_results_per_query,
            "max_candidates": max_candidates,
            "request_timeout": request_timeout,
            "max_urls_per_domain": max_urls_per_domain,
            "max_lead_pages": max_lead_pages,
            "max_resolved_from_leads": max_resolved_from_leads,
            "llm_batch_size": llm_batch_size,
            "dry_run": dry_run,
            "search_provider": search_provider,
        },
    )

    query_errors = 0
    query_debug: list[dict[str, Any]] = []
    metrics: dict[str, int] = {
        "llm_calls": int(query_meta.get("llm_calls") or 0),
        "llm_tokens": int(query_meta.get("llm_tokens") or 0),
    }

    all_result_urls: set[str] = set()
    direct_seed_urls: list[str] = []
    lead_urls: list[str] = []
    seen_seed: set[str] = set()

    try:
        for item in queries:
            query = _clean_text(item.get("query"))
            if not query:
                continue

            try:
                urls = _cached_serp_search(
                    query,
                    max_results=max_results_per_query,
                    timeout=request_timeout,
                    provider=search_provider,
                    metrics=metrics,
                )
            except Exception:
                query_errors += 1
                continue

            accepted_count = 0
            if len(query_debug) < 24:
                query_debug.append(
                    {
                        "query": query,
                        "result_count": len(urls),
                        "accepted_url_count": 0,
                        "sample_urls": urls[:3],
                    }
                )

            for raw_url in urls:
                canonical = _canonicalize_url(raw_url)
                if not canonical:
                    continue
                if canonical in all_result_urls:
                    continue
                all_result_urls.add(canonical)

                if _is_search_ignored_url(canonical):
                    continue

                if canonical in seen_seed:
                    continue
                seen_seed.add(canonical)

                if _is_lead_source_url(canonical):
                    lead_urls.append(canonical)
                else:
                    direct_seed_urls.append(canonical)
                accepted_count += 1

                if len(direct_seed_urls) + len(lead_urls) >= max_candidates * 6:
                    break

            if query_debug:
                query_debug[-1]["accepted_url_count"] = accepted_count

            if len(direct_seed_urls) + len(lead_urls) >= max_candidates * 6:
                break

        lead_urls = lead_urls[:max_lead_pages]

        lead_bundles: list[dict[str, Any]] = []
        for url in lead_urls:
            try:
                bundle = _fetch_page_bundle(url, timeout=request_timeout, metrics=metrics)
            except Exception:
                continue
            if not bundle:
                continue
            lead_bundles.append(bundle)

        lead_candidates = _extract_entities_from_leads(
            lead_bundles,
            timeout=_env_int("DISCOVERY_LLM_TIMEOUT", 25),
            llm_batch_size=llm_batch_size,
            metrics=metrics,
        )

        resolved_from_leads = _resolve_lead_candidates_to_urls(
            lead_candidates,
            max_results_per_query=max_results_per_query,
            timeout=request_timeout,
            provider=search_provider,
            max_candidates=max_resolved_from_leads,
            metrics=metrics,
        )

        for resolved in resolved_from_leads:
            url_value = _canonicalize_url(resolved.get("url"))
            if not url_value:
                continue
            if url_value in seen_seed:
                continue
            seen_seed.add(url_value)
            direct_seed_urls.append(url_value)

        domain_map: dict[str, dict[str, Any]] = {}
        for url in direct_seed_urls:
            domain = _domain(url)
            if not domain:
                continue
            if _domain_matches_suffix(domain, REJECT_ENTITY_DOMAIN_SUFFIXES):
                continue

            bucket = domain_map.setdefault(
                domain,
                {
                    "seed_urls": [],
                    "seed_url_set": set(),
                    "seed_name_hints": [],
                },
            )
            if url not in bucket["seed_url_set"]:
                bucket["seed_url_set"].add(url)
                bucket["seed_urls"].append(url)

        for resolved in resolved_from_leads:
            url_value = _canonicalize_url(resolved.get("url"))
            domain = _domain(url_value)
            if not url_value or not domain:
                continue
            if domain not in domain_map:
                continue
            seed_name = _clean_text(resolved.get("seed_name"))
            if seed_name and seed_name not in domain_map[domain]["seed_name_hints"]:
                domain_map[domain]["seed_name_hints"].append(seed_name)

        ranked_domains = sorted(
            domain_map.items(),
            key=lambda kv: (-len(kv[1].get("seed_urls", [])), kv[0]),
        )

        site_bundles: list[dict[str, Any]] = []
        for domain, payload in ranked_domains:
            if len(site_bundles) >= max_candidates:
                break
            try:
                site_bundle = _build_domain_bundle(
                    domain=domain,
                    seed_urls=payload.get("seed_urls", [])[: max(1, max_urls_per_domain * 3)],
                    timeout=request_timeout,
                    max_urls_per_domain=max_urls_per_domain,
                    metrics=metrics,
                )
            except Exception:
                continue
            if not site_bundle:
                continue
            site_bundle["seed_name_hints"] = payload.get("seed_name_hints", [])[:6]
            site_bundles.append(site_bundle)

        heuristic_accept_count = 0
        llm_accept_count = 0
        rejected_count = 0
        manual_review_count = 0

        accepted_records: list[dict[str, Any]] = []
        ambiguous_for_llm: list[dict[str, Any]] = []

        for bundle in site_bundles:
            gate = _heuristic_gate(bundle)
            decision = gate.get("decision")

            if decision == "reject":
                rejected_count += 1
                continue

            if decision == "accept":
                finalized = _finalize_entity_record(
                    bundle=bundle,
                    entity=gate.get("entity") or {},
                    default_source="auto_discovery",
                )
                if finalized:
                    accepted_records.append(finalized)
                    heuristic_accept_count += 1
                else:
                    manual_review_count += 1
                continue

            fingerprint = _domain_fingerprint(bundle)
            cache_key = _cache_key("classify", bundle.get("domain"), fingerprint)
            cached = get_cached_value(cache_key)
            if isinstance(cached, dict):
                is_entity = bool(cached.get("is_entity"))
                kind = _normalize_token(cached.get("entity_kind")).replace(" ", "_")
                if is_entity and kind in {"place", "one_off_event"}:
                    finalized = _finalize_entity_record(
                        bundle=bundle,
                        entity=cached,
                        default_source="auto_discovery",
                    )
                    if finalized and float(cached.get("confidence") or 0) >= 0.5:
                        accepted_records.append(finalized)
                        llm_accept_count += 1
                    else:
                        manual_review_count += 1
                else:
                    rejected_count += 1
                continue

            ambiguous_for_llm.append({"bundle": bundle, "cache_key": cache_key})

        if ambiguous_for_llm:
            llm_inputs = [item["bundle"] for item in ambiguous_for_llm]
            llm_results, llm_calls, llm_tokens = _classify_site_bundles_with_llm(
                llm_inputs,
                timeout=_env_int("DISCOVERY_LLM_TIMEOUT", 25),
                batch_size=llm_batch_size,
            )
            metrics["llm_calls"] = metrics.get("llm_calls", 0) + llm_calls
            metrics["llm_tokens"] = metrics.get("llm_tokens", 0) + llm_tokens

            for wrapped in ambiguous_for_llm:
                bundle = wrapped["bundle"]
                cache_key = wrapped["cache_key"]
                domain = str(bundle.get("domain") or "").lower()

                result = llm_results.get(domain)
                if result is None:
                    manual_review_count += 1
                    continue

                set_cached_value(cache_key, result, ttl_days=LLM_CLASSIFY_CACHE_TTL_DAYS)

                is_entity = bool(result.get("is_entity"))
                kind = _normalize_token(result.get("entity_kind")).replace(" ", "_")
                confidence = float(result.get("confidence") or 0)

                if not is_entity or kind not in {"place", "one_off_event"}:
                    rejected_count += 1
                    continue

                finalized = _finalize_entity_record(
                    bundle=bundle,
                    entity=result,
                    default_source="auto_discovery",
                )
                if not finalized:
                    manual_review_count += 1
                    continue

                if confidence < 0.5:
                    manual_review_count += 1
                    continue

                accepted_records.append(finalized)
                llm_accept_count += 1

        final_records = _dedupe_records(accepted_records, max_items=max_candidates)

        upserted_count = 0
        if not dry_run:
            for item in final_records:
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

        place_count = sum(1 for item in final_records if item.get("entity_kind") == "place")
        one_off_count = sum(1 for item in final_records if item.get("entity_kind") == "one_off_event")

        llm_calls_total = int(metrics.get("llm_calls", 0))
        llm_tokens_total = int(metrics.get("llm_tokens", 0))

        summary = {
            "run_id": run_id,
            "status": "success",
            "query_count": len(queries),
            "query_errors": query_errors,
            "searched_url_count": len(all_result_urls),
            "lead_url_count": len(lead_urls),
            "lead_candidate_name_count": len(lead_candidates),
            "lead_resolved_count": len(resolved_from_leads),
            "domain_count": len(site_bundles),
            "candidate_count": len(final_records),
            "place_count": place_count,
            "one_off_event_count": one_off_count,
            "upserted_count": upserted_count,
            "rejected_count": rejected_count,
            "manual_review_count": manual_review_count,
            "heuristic_accept_count": heuristic_accept_count,
            "llm_accept_count": llm_accept_count,
            "accept_rate": round((len(final_records) / len(site_bundles)) if site_bundles else 0.0, 4),
            "llm_calls": llm_calls_total,
            "llm_tokens": llm_tokens_total,
            "avg_tokens": round((llm_tokens_total / llm_calls_total), 2) if llm_calls_total else 0.0,
            "query_generation_cache_hit": bool(query_meta.get("cache_hit")),
            "strategy_count": int(query_meta.get("strategy_count") or 0),
            "dry_run": dry_run,
            "search_provider": "duckduckgo",
            "max_urls_per_domain": max_urls_per_domain,
            "cache_metrics": {
                "serp_cache_hits": metrics.get("serp_cache_hits", 0),
                "serp_cache_misses": metrics.get("serp_cache_misses", 0),
                "bundle_cache_hits": metrics.get("bundle_cache_hits", 0),
                "bundle_cache_misses": metrics.get("bundle_cache_misses", 0),
                "lead_extract_cache_hits": metrics.get("lead_extract_cache_hits", 0),
                "lead_extract_cache_misses": metrics.get("lead_extract_cache_misses", 0),
            },
            "query_debug": query_debug,
        }

        finish_discovery_run(
            run_id,
            status="success",
            result_count=len(final_records),
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
    parser.add_argument("--search-provider", choices=["duckduckgo", "openai_web"], help="Discovery search provider")
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

    queries, _ = _build_discovery_queries(max_queries=args.max_queries or _env_int("DISCOVERY_MAX_QUERIES", 120), boroughs=boroughs, categories=categories)

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
            search_provider=args.search_provider,
        )
        print(json.dumps(summary, indent=2))
        return

    print(f"Generated {len(queries)} discovery queries")
    print("Run automatic discovery with: python -m app.discover --run-once")


if __name__ == "__main__":
    main()
