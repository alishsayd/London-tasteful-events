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
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"

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

COUNTRY_FOCUS_TERMS = [
    "japanese",
    "korean",
    "indian",
    "hungarian",
    "irish",
    "mexican",
    "russian",
    "polish",
    "turkish",
    "arab",
    "chinese",
    "french",
    "german",
    "italian",
    "spanish",
    "portuguese",
    "greek",
    "ukrainian",
    "african",
    "latin american",
]

LOW_SIGNAL_STRATEGY_PHRASES = {
    "lates",
    "friday lates",
    "late night",
    "events",
    "what s on",
    "whats on",
    "calendar",
    "program",
    "programme",
}

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

# Suffix-based non-entity filters catch subdomains and common publishing/platform sites.
NON_ENTITY_SOURCE_DOMAIN_SUFFIXES = (
    "ianvisits.co.uk",
    "lectures.london",
    "github.com",
    "bsky.app",
    "bsky.social",
    "blueskyweb.xyz",
    "theguardian.com",
    "guardian.co.uk",
    "ft.com",
    "eventindustrynews.com",
    "culturecalling.com",
    "london.com",
)

SEARCH_REJECT_DOMAIN_SUFFIXES = (
    "github.com",
    "bsky.app",
    "bsky.social",
    "blueskyweb.xyz",
    "theguardian.com",
    "guardian.co.uk",
    "ft.com",
    "eventindustrynews.com",
)

BAD_NAME_PHRASES = {
    "book your tickets",
    "courses and meetings",
    "support ianvisits",
    "subscribe to read",
    "event industry news",
    "arts events listings",
    "events listings",
    "overview",
}

INSTITUTION_KEYWORDS = (
    "museum",
    "gallery",
    "cinema",
    "theatre",
    "cultural centre",
    "cultural center",
    "cultural institute",
    "art centre",
    "art center",
    "arts centre",
    "arts center",
    "foundation",
    "institute",
    "house",
    "bookshop",
    "community arts",
)

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


def _domain_matches_suffix(domain: str | None, suffixes: tuple[str, ...]) -> bool:
    value = str(domain or "").lower().strip().replace("www.", "")
    if not value:
        return False
    return any(value == suffix or value.endswith(f".{suffix}") for suffix in suffixes)


def _domain_is_non_entity_source(domain: str | None) -> bool:
    value = str(domain or "").lower().strip().replace("www.", "")
    if not value:
        return True
    return value in NON_ENTITY_SOURCE_DOMAINS or _domain_matches_suffix(value, NON_ENTITY_SOURCE_DOMAIN_SUFFIXES)


def _is_event_detail_path(path: str | None) -> bool:
    segments = [segment for segment in str(path or "").lower().split("/") if segment]
    if not segments:
        return False

    markers = {"event", "events", "programme", "program", "calendar", "whatson", "whats-on"}
    for idx, segment in enumerate(segments):
        if segment in markers and idx + 1 < len(segments):
            return True
    return False


def _seed_url_is_likely_org_entry(url: str) -> bool:
    parsed = urlparse(url)
    domain = _domain(url)
    if _domain_matches_suffix(domain, SEARCH_REJECT_DOMAIN_SUFFIXES):
        return False

    path = (parsed.path or "").lower()
    if domain in AGGREGATOR_SOURCE_DOMAINS:
        return True

    if any(hint in path for hint in ARTICLE_PATH_HINTS):
        return False
    if _is_event_detail_path(path):
        return False
    return True


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

    blocked_exact = {
        "home",
        "events",
        "what s on",
        "whats on",
        "calendar",
        "program",
        "programme",
        "github",
        "bluesky",
        "bluesky social",
    }
    if lower in blocked_exact:
        return False

    if any(phrase in lower for phrase in BAD_NAME_PHRASES):
        return False

    if lower.endswith(" overview"):
        return False
    if lower.startswith(("book ", "support ", "subscribe ")):
        return False

    return True


def _unwrap_duckduckgo_href(href: str) -> str | None:
    if not href:
        return None

    href = href.strip()
    if href.startswith("//"):
        href = f"https:{href}"

    parsed = urlparse(href)
    # DuckDuckGo often returns relative redirect links such as `/l/?uddg=...`.
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


def _should_skip_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return True

    domain = (parsed.netloc or "").lower().replace("www.", "")
    if not domain:
        return True

    if domain in BLOCKED_DOMAINS:
        return True
    if _domain_matches_suffix(domain, SEARCH_REJECT_DOMAIN_SUFFIXES):
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


def _extract_output_text_from_response(payload: dict[str, Any]) -> str:
    # `output_text` is SDK-only, but some API responses can still include it.
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


def _extract_urls_from_annotations(payload: dict[str, Any], max_results: int) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    def _walk(value: Any) -> None:
        if len(urls) >= max_results:
            return
        if isinstance(value, dict):
            annotations = value.get("annotations")
            if isinstance(annotations, list):
                for annotation in annotations:
                    if not isinstance(annotation, dict):
                        continue
                    direct_url = annotation.get("url")
                    nested_url = None
                    nested = annotation.get("url_citation")
                    if isinstance(nested, dict):
                        nested_url = nested.get("url")
                    for candidate in (direct_url, nested_url):
                        if not isinstance(candidate, str):
                            continue
                        if _should_skip_url(candidate):
                            continue
                        key = candidate.lower().strip()
                        if key in seen:
                            continue
                        seen.add(key)
                        urls.append(candidate)
                        if len(urls) >= max_results:
                            return
            for item in value.values():
                _walk(item)
            return
        if isinstance(value, list):
            for item in value:
                _walk(item)

    _walk(payload)
    return urls


def _extract_urls_from_text_blob(text_blob: str, max_results: int) -> list[str]:
    text_blob = _clean_text(text_blob)
    if not text_blob:
        return []

    candidate_urls: list[str] = []

    def _push(url_value: str | None) -> None:
        if not url_value:
            return
        clean = url_value.strip().strip(".,);]}>\"'")
        if clean and clean not in candidate_urls:
            candidate_urls.append(clean)

    raw = text_blob
    fenced_match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", raw, flags=re.IGNORECASE)
    if fenced_match:
        raw = fenced_match.group(1).strip()

    parsed_json = None
    try:
        parsed_json = json.loads(raw)
    except Exception:
        parsed_json = None

    if parsed_json is not None:
        if isinstance(parsed_json, list):
            for item in parsed_json:
                if isinstance(item, str):
                    _push(item)
                elif isinstance(item, dict):
                    for key in ("url", "homepage", "events_url", "link"):
                        if isinstance(item.get(key), str):
                            _push(item.get(key))
        elif isinstance(parsed_json, dict):
            for key in ("urls", "results", "orgs", "organizations", "items", "links"):
                value = parsed_json.get(key)
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, str):
                            _push(item)
                        elif isinstance(item, dict):
                            for item_key in ("url", "homepage", "events_url", "link"):
                                if isinstance(item.get(item_key), str):
                                    _push(item.get(item_key))

    if not candidate_urls:
        for found in re.findall(r"https?://[^\s\"'<>]+", text_blob):
            _push(found)

    urls: list[str] = []
    seen: set[str] = set()
    for item in candidate_urls:
        if _should_skip_url(item):
            continue
        key = item.lower().strip()
        if key in seen:
            continue
        seen.add(key)
        urls.append(item)
        if len(urls) >= max_results:
            break
    return urls


def _extract_urls_from_web_sources(payload: dict[str, Any], max_results: int) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    def _walk(value: Any) -> None:
        if len(urls) >= max_results:
            return
        if isinstance(value, dict):
            source_list = value.get("sources")
            if isinstance(source_list, list):
                for source in source_list:
                    if not isinstance(source, dict):
                        continue
                    source_url = source.get("url")
                    if not isinstance(source_url, str):
                        continue
                    if _should_skip_url(source_url):
                        continue
                    key = source_url.lower().strip()
                    if key in seen:
                        continue
                    seen.add(key)
                    urls.append(source_url)
                    if len(urls) >= max_results:
                        return
            for item in value.values():
                _walk(item)
            return
        if isinstance(value, list):
            for item in value:
                _walk(item)

    _walk(payload)
    return urls


def _search_openai_web(query: str, max_results: int, timeout: int) -> list[str]:
    api_key = _clean_text(os.getenv("OPENAI_API_KEY"))
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for DISCOVERY_SEARCH_PROVIDER=openai_web")

    model = _clean_text(os.getenv("DISCOVERY_OPENAI_MODEL")) or "gpt-5"
    external_web_access = _env_bool("DISCOVERY_OPENAI_EXTERNAL_WEB_ACCESS", True)

    prompt = (
        f"Use web search to find London organisation entities for query: {query}\n"
        "Keep only institution entities (museum, gallery, cultural centre/institute/house, community arts venue).\n"
        "Exclude aggregator/directory/article/listicle pages and one-off program pages.\n"
        f"Return up to {max_results} unique absolute URLs."
    )

    payload = {
        "model": model,
        "input": prompt,
        "tool_choice": "auto",
        "tools": [
            {
                "type": "web_search",
                "external_web_access": external_web_access,
                "user_location": {
                    "type": "approximate",
                    "country": "GB",
                    "city": "London",
                    "region": "London",
                },
            }
        ],
        "max_output_tokens": 700,
        "include": ["web_search_call.action.sources"],
    }

    response = requests.post(
        OPENAI_RESPONSES_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=max(20, timeout + 10),
    )
    response.raise_for_status()

    response_payload: dict[str, Any] = response.json()
    from_sources = _extract_urls_from_web_sources(response_payload, max_results=max_results)
    if from_sources:
        return from_sources
    from_annotations = _extract_urls_from_annotations(response_payload, max_results=max_results)
    if from_annotations:
        return from_annotations
    response_text = _extract_output_text_from_response(response_payload)
    from_text = _extract_urls_from_text_blob(response_text, max_results=max_results)
    if from_text:
        return from_text
    return []


def _search_web(query: str, max_results: int, timeout: int, provider: str) -> list[str]:
    provider_key = _normalize_token(provider).replace(" ", "_") or "duckduckgo"
    if provider_key in {"openai_web", "openai"}:
        try:
            return _search_openai_web(query=query, max_results=max_results, timeout=timeout)
        except Exception:
            if _env_bool("DISCOVERY_OPENAI_FALLBACK_TO_DUCKDUCKGO", True):
                return _search_duckduckgo(query=query, max_results=max_results, timeout=timeout)
            raise

    return _search_duckduckgo(query=query, max_results=max_results, timeout=timeout)


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
    if any(hint in page_path for hint in EVENT_URL_HINTS) and not _is_event_detail_path(page_path):
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
        if _is_event_detail_path(path):
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


def _contains_institution_keyword(value: str | None) -> bool:
    blob = _normalize_token(value)
    if not blob:
        return False
    return any(_normalize_token(keyword) in blob for keyword in INSTITUTION_KEYWORDS)


def _looks_like_non_cultural_academic(name: str, text_blob: str) -> bool:
    normalized_name = _normalize_token(name)
    if not normalized_name:
        return False

    academic_markers = ("university", "college", "school", "lse", "soas")
    if not any(marker in normalized_name.split(" ") or marker in normalized_name for marker in academic_markers):
        return False

    culture_markers = ("museum", "gallery", "cinema", "theatre", "cultural centre", "cultural center", "art centre", "arts center")
    normalized_blob = _normalize_token(text_blob)
    return not any(marker in normalized_blob for marker in culture_markers)


def _extract_org_candidate(url: str, timeout: int) -> dict[str, Any] | None:
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    response.raise_for_status()

    resolved_domain = _domain(response.url)
    if _domain_is_non_entity_source(resolved_domain):
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
    category_blob = _clean_text(" ".join([name, title, description, response.url, page_text[:2000]]))
    category = _infer_category(category_blob)

    article_like = _looks_like_article_or_listicle(response.url, title, description, page_text)
    program_like = _looks_like_program_page(response.url, title, name)
    domain_match = _name_matches_domain(name, response.url)
    has_schema = _has_org_schema_markup(soup)
    has_institution_keyword = _contains_institution_keyword(" ".join([name, title, description, page_text[:2000]]))

    if _looks_like_non_cultural_academic(name, text_blob):
        return None
    if not has_institution_keyword and not (has_schema and domain_match):
        return None
    if article_like:
        return None
    if program_like:
        return None
    if _is_event_detail_path(urlparse(response.url).path):
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

    def _is_low_signal_phrase(value: str) -> bool:
        normalized = _normalize_token(value)
        if not normalized:
            return True
        if normalized in LOW_SIGNAL_STRATEGY_PHRASES:
            return True
        if _looks_like_program_text(value) or _looks_like_article_title(value):
            return True
        return _word_count(value) < 2

    def _extract_quoted_phrases(value: str) -> list[str]:
        results: list[str] = []
        seen: set[str] = set()
        for groups in re.findall(r'"([^"]+)"|“([^”]+)”|\'([^\']+)\'', value):
            phrase = _clean_text(next((part for part in groups if part), ""))
            if len(phrase) < 3:
                continue
            if _is_low_signal_phrase(phrase):
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
        out = []
        for item in variants:
            clean = _clean_text(item)
            if not clean:
                continue
            if _word_count(clean) > 7:
                continue
            if _looks_like_program_text(clean):
                continue
            out.append(clean)
        return out

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

    def _has_country_focus_hint(value: str) -> bool:
        lower = _normalize_token(value)
        hints = (
            "country cultural",
            "cultural center",
            "cultural centre",
            "cultural institute",
            "japan house",
            "korean cultural",
            "country focused",
        )
        return any(hint in lower for hint in hints)

    def _country_focus_queries(strategy_id: int) -> list[dict[str, Any]]:
        queries: list[dict[str, Any]] = [
            {
                "query": "country cultural centres in London",
                "source": "strategy_country",
                "strategy_id": strategy_id,
            },
            {
                "query": "national cultural institutes in London",
                "source": "strategy_country",
                "strategy_id": strategy_id,
            },
            {
                "query": "country themed cultural houses in London",
                "source": "strategy_country",
                "strategy_id": strategy_id,
            },
        ]
        for term in COUNTRY_FOCUS_TERMS:
            queries.extend(
                [
                    {
                        "query": f"{term} cultural centre London",
                        "source": "strategy_country",
                        "strategy_id": strategy_id,
                    },
                    {
                        "query": f"{term} cultural center London",
                        "source": "strategy_country",
                        "strategy_id": strategy_id,
                    },
                ]
            )
        return queries

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
                        "query": f"{phrase} London cultural centre",
                        "source": "strategy_phrase",
                        "strategy_id": strategy_id,
                    },
                    {
                        "query": f"{phrase} London cultural center",
                        "source": "strategy_phrase",
                        "strategy_id": strategy_id,
                    },
                    {
                        "query": f"{phrase} London events programme",
                        "source": "strategy_phrase",
                        "strategy_id": strategy_id,
                    },
                ]
            )

        for variant in _center_variants(text_value):
            if _is_low_signal_phrase(variant):
                continue
            strategy_queries.append(
                {
                    "query": f"{variant} London cultural venue",
                    "source": "strategy",
                    "strategy_id": strategy_id,
                }
            )

        if _has_country_focus_hint(text_value):
            strategy_queries.extend(_country_focus_queries(strategy_id))

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

    floor_strategy = 0
    if strategy_pool:
        if max_queries >= 10:
            floor_strategy = 4
        elif max_queries >= 8:
            floor_strategy = 3
        elif max_queries >= 4:
            floor_strategy = 2
        else:
            floor_strategy = 1

    floor_grid = 1 if grid_pool and max_queries >= 6 else (1 if grid_pool and max_queries >= 3 else 0)
    floor_aggregator = 1 if aggregator_pool and max_queries >= 8 else 0

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
        aggregator_idx = _add_from_pool(aggregator_pool, aggregator_idx, 1)
        if len(selected) >= max_queries:
            break
        grid_idx = _add_from_pool(grid_pool, grid_idx, 1)
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
    """Run one full discovery cycle and upsert discovered organisations."""

    init_db()

    max_queries = max_queries or _env_int("DISCOVERY_MAX_QUERIES", 16)
    max_results_per_query = max_results_per_query or _env_int("DISCOVERY_MAX_RESULTS_PER_QUERY", 8)
    max_candidates = max_candidates or _env_int("DISCOVERY_MAX_CANDIDATES", 60)
    request_timeout = request_timeout or _env_int("DISCOVERY_REQUEST_TIMEOUT", 12)
    max_urls_per_domain = _env_int("DISCOVERY_MAX_URLS_PER_DOMAIN", 3)
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
    queries = _build_discovery_queries(max_queries=max_queries, boroughs=boroughs, categories=categories)

    run_id = start_discovery_run(
        query_count=len(queries),
        trigger=trigger,
        details={
            "max_queries": max_queries,
            "max_results_per_query": max_results_per_query,
            "max_candidates": max_candidates,
            "request_timeout": request_timeout,
            "max_urls_per_domain": max_urls_per_domain,
            "dry_run": dry_run,
            "search_provider": search_provider,
        },
    )

    query_errors = 0
    searched_urls: list[str] = []
    aggregator_seed_urls: list[str] = []
    query_debug: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    domain_counts: dict[str, int] = {}

    try:
        for item in queries:
            query = item["query"]
            try:
                results = _search_web(
                    query=query,
                    max_results=max_results_per_query,
                    timeout=request_timeout,
                    provider=search_provider,
                )
            except Exception:
                query_errors += 1
                continue

            accepted_count = 0
            debug_entry = None
            if len(query_debug) < 12:
                debug_entry = {
                    "query": query,
                    "result_count": len(results),
                    "accepted_url_count": 0,
                    "sample_urls": results[:3],
                }
                query_debug.append(debug_entry)

            for url in results:
                if _should_skip_url(url):
                    continue
                if not _seed_url_is_likely_org_entry(url):
                    continue
                key = url.lower().strip()
                if key in seen_urls:
                    continue
                domain = _domain(url)
                if not domain:
                    continue
                if domain_counts.get(domain, 0) >= max_urls_per_domain:
                    continue
                seen_urls.add(key)
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
                if domain in AGGREGATOR_SOURCE_DOMAINS:
                    aggregator_seed_urls.append(url)
                else:
                    searched_urls.append(url)
                accepted_count += 1

            if debug_entry is not None:
                debug_entry["accepted_url_count"] = accepted_count

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
                if not _seed_url_is_likely_org_entry(url):
                    continue
                key = url.lower().strip()
                if key in seen_urls:
                    continue
                domain = _domain(url)
                if not domain:
                    continue
                if domain_counts.get(domain, 0) >= max_urls_per_domain:
                    continue

                seen_urls.add(key)
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
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
            "search_provider": search_provider,
            "max_urls_per_domain": max_urls_per_domain,
            "query_debug": query_debug,
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
            search_provider=args.search_provider,
        )
        print(json.dumps(summary, indent=2))
        return

    print(f"Generated {len(queries)} discovery queries")
    print("Run automatic discovery with: python -m app.discover --run-once")


if __name__ == "__main__":
    main()
