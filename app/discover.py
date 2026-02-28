"""
Org discovery engine.

Generates search queries from borough × category combinations,
then parses results into candidate orgs for review.

Usage:
    python -m app.discover                  # run all strategies
    python -m app.discover --borough Hackney # one borough
    python -m app.discover --category gallery # one category
    python -m app.discover --from-file seeds.json  # import a JSON file
"""

import argparse
import json
import sys

from app.db import init_db, upsert_org, get_stats

# London boroughs — focus on the ones with the densest cultural scenes first
BOROUGHS = [
    # Inner London — high density
    "Hackney", "Tower Hamlets", "Southwark", "Lambeth", "Camden",
    "Islington", "Westminster", "Kensington and Chelsea",
    "Hammersmith and Fulham", "Lewisham", "Greenwich", "Wandsworth",
    "Haringey", "Newham", "City of London",
    # Outer London — selective
    "Waltham Forest", "Barking and Dagenham", "Croydon", "Ealing",
    "Brent", "Enfield", "Hounslow", "Richmond upon Thames",
    "Kingston upon Thames", "Bromley", "Barnet", "Redbridge",
    "Harrow", "Havering", "Hillingdon", "Merton", "Sutton",
    "Bexley",
]

# Categories of cultural org we're looking for
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
    "supper club",
    "film club",
    "lecture series",
    "workshop space",
    "makers space",
    "independent arts venue",
]

# Known aggregator sites to mine for org names
AGGREGATOR_QUERIES = [
    "site:ianvisits.co.uk free events London",
    "site:timeout.com/london free things to do",
    "Open House London participating venues",
    "Arts Council England funded organisations London",
    "London independent cinema list",
    "London independent gallery list",
    "London community arts organisations",
    "London free cultural events listings",
    "London pop-up events cultural",
    "London supper club underground dining",
    "London poetry open mic nights",
    "London independent theatre fringe",
    "London maker space hackspace",
    "London DIY art space",
    "London zine fair bookshop events",
]


def generate_queries(boroughs=None, categories=None):
    """Generate search queries from borough × category grid."""
    boroughs = boroughs or BOROUGHS
    categories = categories or CATEGORIES

    queries = []

    # Borough × category combinations (the main grid)
    for borough in boroughs:
        for category in categories:
            queries.append({
                "query": f"{category} {borough} London events",
                "borough": borough,
                "category": category,
                "source": "borough_search",
            })

    # Aggregator queries (supplementary)
    for q in AGGREGATOR_QUERIES:
        queries.append({
            "query": q,
            "borough": None,
            "category": None,
            "source": "aggregator_search",
        })

    return queries


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
    """Print queries for manual or external search."""
    for i, q in enumerate(queries, 1):
        print(f"{i:4d}. [{q['source']}] {q['query']}")
        if q["borough"]:
            print(f"      borough={q['borough']} category={q['category']}")


def main():
    parser = argparse.ArgumentParser(description="Discover candidate orgs")
    parser.add_argument("--borough", help="Filter to one borough")
    parser.add_argument("--category", help="Filter to one category")
    parser.add_argument("--from-file", help="Import orgs from a JSON file")
    parser.add_argument("--print-queries", action="store_true",
                        help="Print search queries without executing")
    parser.add_argument("--export-queries", help="Export queries to JSON file")
    args = parser.parse_args()

    init_db()

    if args.from_file:
        import_from_file(args.from_file)
        stats = get_stats()
        print(f"\nDatabase stats: {stats}")
        return

    boroughs = [args.borough] if args.borough else None
    categories = [args.category] if args.category else None
    queries = generate_queries(boroughs, categories)

    if args.print_queries:
        print_queries(queries)
        print(f"\nTotal queries: {len(queries)}")
        return

    if args.export_queries:
        with open(args.export_queries, "w") as f:
            json.dump(queries, f, indent=2)
        print(f"Exported {len(queries)} queries to {args.export_queries}")
        return

    # Default: print summary and instructions
    print(f"Generated {len(queries)} search queries")
    print(f"  - {len(boroughs or BOROUGHS)} boroughs × {len(categories or CATEGORIES)} categories = {len(boroughs or BOROUGHS) * len(categories or CATEGORIES)} grid queries")
    print(f"  - {len(AGGREGATOR_QUERIES)} aggregator queries")
    print()
    print("Next steps:")
    print("  1. Run with --print-queries to see all queries")
    print("  2. Run with --export-queries queries.json to export")
    print("  3. Use Claude to run searches and save results as JSON")
    print("  4. Import results with --from-file results.json")
    print("  5. Review in admin panel: python -m app.admin")


if __name__ == "__main__":
    main()
