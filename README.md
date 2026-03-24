# Stack Overflow Analysis — BigQuery

Exploring what drives answers (and good answers) on Stack Overflow using `bigquery-public-data.stackoverflow`.

Three questions, four queries.

## Dataset

- **Source**: `bigquery-public-data.stackoverflow.posts_questions` + `users`
- **Engine**: Google BigQuery (Standard SQL)
- **Note**: Public dataset goes up to September 2022 — queries use the latest available year dynamically

## Questions & Queries

### Q1: What tags lead to the most (and least) answers?

**Part 1** — [`question_1_part_1.sql`](question_1_part_1.sql)

Individual tags, top and bottom 20 by average answers per question.

Tags are stored as pipe-delimited strings (`python|pandas|csv`), so we `unnest(split(tags, '|'))` to explode them into rows and aggregate per tag. Tags with 75+ questions only — that's the p90 cutoff we found by checking the distribution. Below that, averages are unreliable (one question with 5 answers doesn't mean the tag "performs well").

**Part 2** — [`question_1_part_2.sql`](question_1_part_2.sql)

Tag *combinations* (pairs). Same structure but with a self-cross join on the unnested tags. `where t1 < t2` deduplicates pairs alphabetically — so `python + pandas` and `pandas + python` only count once, and no self-pairs.

**What came out**: shell scripting pairs (`awk + sed`, `awk + bash`) dominate the top — high answer counts, ~60-70% accepted rate. Bottom is niche platform integrations (`ckeditor + ckeditor5`, `facebook + facebook-graph-api`) where questions are harder to answer and more config-specific.

### Q2: Python vs dbt — how has the answer rate changed over time?

[`question_2.sql`](question_2.sql)

Year-over-year change in avg answers per question and accepted answer rate, last 10 years.

Tags matched with `regexp_contains(lower(tags), r'(^|\|)python(\||$)')` — exact match within the pipe-delimited string, not a substring. So `python` matches but `python-3.x` or `cpython` don't. `lower()` just in case.

YoY computed with `lag()` partitioned by tag — absolute difference, not percentage.

### Q3: Beyond tags, what makes a question get answered?

[`question_3.sql`](question_3.sql)

Seven features bucketed and analyzed in a single query using `union all`:

- **Title word count** — shorter titles → higher accepted rate
- **Body length** — inverse correlation, shorter posts do better
- **Has code** (`<code>` tag in body) — stable, not a strong signal
- **Tag count** — number of tags on the question
- **Hour posted** (UTC)
- **Day of week**
- **Author reputation** — joined from `users` table, poor correlation

Runs against the full dataset (no year filter). Each feature is independently bucketed with avg answers and accepted rate side by side.

The takeaway: write a clear, focused question. Short title + short body = you know what you're asking, and someone can answer it fast.

## Run it

```bash
bq query --use_legacy_sql=false < question_1_part_1.sql
```

Requires `gcloud` CLI authenticated with access to BigQuery public datasets.
