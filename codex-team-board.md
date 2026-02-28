# London Tasteful Events: Team Board (No Tickets Yet)

This board is intentionally empty and ready for tickets later.

## Board Name
`London Tasteful Events - Team Board`

## Status Columns

1. `Inbox`
2. `Ready`
3. `In Progress`
4. `Blocked`
5. `In Review`
6. `Done`

## Thread Lanes

1. `CTO`
2. `Curation`
3. `Crawler`
4. `Extraction`
5. `Data Quality`
6. `Frontend/API`
7. `Release`

## Required Fields

1. `Status` (single select): Inbox, Ready, In Progress, Blocked, In Review, Done
2. `Thread` (single select): CTO, Curation, Crawler, Extraction, Data Quality, Frontend/API, Release
3. `Priority` (single select): P0, P1, P2, P3
4. `Severity` (single select): S1, S2, S3, S4
5. `Org ID` (text)
6. `Crawl Run ID` (text)
7. `Event ID` (text)
8. `Owner` (people)
9. `Target Milestone` (text)

## Labels

1. `thread:cto`
2. `thread:curation`
3. `thread:crawler`
4. `thread:extraction`
5. `thread:qa`
6. `thread:frontend`
7. `thread:release`
8. `type:bug`
9. `type:task`
10. `type:decision`
11. `type:spike`
12. `blocked`
13. `needs-manual-review`
14. `source-site`
15. `data-quality`

## WIP Limits

1. `Curation`: max 5 in progress
2. `Crawler`: max 2 in progress
3. `Extraction`: max 3 in progress
4. `Data Quality`: max 2 in progress
5. `Frontend/API`: max 3 in progress
6. `Release`: max 1 in progress

## Debug Rule

Any bug ticket must include at least one of:

1. `Org ID`
2. `Crawl Run ID`
3. `Event ID`
