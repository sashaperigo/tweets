# SF Politician Tweet Tracker

Collects and analyzes tweets mentioning San Francisco politicians using the X (Twitter) API v2. Currently tracking Jackie Fielder and Daniel Lurie.

## What it does

For each politician, the pipeline:
1. Downloads tweets via the X API search endpoint
2. Classifies each tweet by how it references the politician (reply type)
3. Scores sentiment using VADER
4. Flags tweets from known institutional/bot accounts
5. Stores results in a JSON database and a flat CSV for analysis

## Data collected

Each tweet record contains:

| Field | Source | Description |
|---|---|---|
| `id` | API | Unique tweet ID (Twitter snowflake) |
| `created_at` | API | UTC timestamp |
| `author_id` | API | Numeric user ID of the tweet author |
| `username` | API (via user expansion) | Author's @handle |
| `name` | API (via user expansion) | Author's display name |
| `text` | API | Full tweet text |
| `likes` | API | Like count at time of download |
| `retweets` | API | Retweet count at time of download |
| `replies` | API | Reply count at time of download |
| `quotes` | API | Quote tweet count at time of download |
| `impressions` | API | Impression count at time of download |
| `reply_type` | Computed | See classification below |
| `sentiment` | Computed | `positive`, `negative`, or `neutral` |
| `sentiment_score` | Computed | VADER compound score, range [-1, 1] |
| `is_excluded` | Computed | `True` if author is an institutional/bot account |

## Search queries

Each politician is searched with:
```
"<Full Name>" OR @<handle> -is:retweet
```

For example: `"Jackie Fielder" OR @JackieFielder_ -is:retweet`

Retweets are also filtered out client-side before saving.

## Reply type classification

Each tweet is classified into one of four categories based on its structure:

| Reply type | Meaning |
|---|---|
| **Direct reply** | Tweet starts with `@<handle>` — addressed directly to the politician |
| **Thread mention** | Tweet starts with one or more other `@handles` — the politician is tagged in a thread but is not the primary addressee |
| **Mention** | `@<handle>` appears mid-tweet, embedded in regular text |
| **Not tagged** | Matched the search query (e.g. name was mentioned) but the handle doesn't appear |

Classification is case-insensitive and uses word-boundary regex matching.

## Sentiment analysis

Sentiment is computed using [VADER](https://github.com/cjhutto/vaderSentiment) (Valence Aware Dictionary and sEntiment Reasoner) via `nltk.sentiment.vader`. VADER returns a compound score from -1.0 to 1.0:

- **Positive**: compound ≥ 0.05
- **Negative**: compound ≤ -0.05
- **Neutral**: otherwise

## Excluded accounts

Tweets from the following accounts are flagged `is_excluded = True` in the CSV. They are kept in the dataset but can be filtered out for analysis:

- `sfpdcallsbot` — SFPD calls bot
- `sfchronicle` — San Francisco Chronicle
- `kqednews` — KQED News
- `mlnow` — Mission Local
- `sfist` — SFist
- `sfstandard` — SF Standard
- `48hills` — 48 Hills
- `grok` — xAI Grok

## File structure

```
download_tweets.py       # Main data collection script
backfill_usernames.py    # One-off script to backfill username/name from author_id
categorize_tweets.py     # Harassment/abuse categorization (44 categories, hostility score)
test_download_tweets.py  # Test suite (99 tests, runs as pre-commit hook)

jackie_fielder_tweets.json   # Full tweet database for Jackie Fielder
jackie_fielder_tweets.csv    # Flat CSV for Jackie Fielder
daniel_lurie_tweets.json     # Full tweet database for Daniel Lurie
daniel_lurie_tweets.csv      # Flat CSV for Daniel Lurie
```

JSON files are the source of truth. CSVs are always rebuilt from the JSON.

## Running the collector

```bash
# Download new tweets (resumes from latest saved tweet ID)
python download_tweets.py jackie
python download_tweets.py daniel

# Backfill historical tweets (requires paid API access)
python download_tweets.py jackie --backfill
python download_tweets.py daniel --backfill
```

## Running tests

```bash
python -m pytest test_download_tweets.py
```

Tests run automatically as a pre-commit hook. They include unit tests for all transformation logic and data integrity tests that validate every field in the real JSON and CSV files on disk.

---

## Methodology limitations

### Coverage

- **Recent search only goes back 7 days.** The `/search/recent` endpoint (included in the Basic API tier) only returns tweets from the past week. Historical data beyond that requires the `/search/all` endpoint, which is on a paid tier (~$100/month as of 2025).
- **The search query may miss some mentions.** Tweets that refer to a politician by an abbreviated name, nickname, or misspelling without using their exact full name or @handle won't be captured. For example, a tweet mentioning "Jackie" without her last name or handle would not be collected.
- **Deleted and suspended accounts are silently dropped.** If a user deletes their account or is suspended after a tweet is collected, the tweet text remains in the database but the author's username/name cannot be resolved via the users lookup endpoint.

### Engagement metrics

- **Metrics are a snapshot, not a time series.** Likes, retweets, replies, quotes, and impressions are captured at download time. A tweet that goes viral after being downloaded won't have its updated metrics reflected unless re-downloaded.
- **Impression counts require elevated API access.** On some API tiers, `impression_count` may not be returned and will appear as 0.

### Reply type classification

- **Classification is structural, not semantic.** A tweet starting with `@SomeoneElse @JackieFielder_` is classified as a "Thread mention" purely based on handle position in the text, not on whether the content is actually about the politician.
- **Quote tweets are treated as regular tweets.** A quote tweet of a post mentioning the politician is collected and classified the same as any other tweet. The quoted content is not analyzed.

### Sentiment analysis

- **VADER is a general-purpose lexicon, not domain-specific.** It was not trained on political Twitter and may misclassify politically charged language. Phrases that are positive or negative in a political context may not score as expected (e.g., "Jackie Fielder is a socialist" scores as neutral).
- **Sentiment reflects tone, not stance.** A tweet enthusiastically supporting the politician and a tweet enthusiastically attacking them may both score as "positive" if the language used is energetic and positive in tone.
- **Irony and sarcasm are not detected.** VADER cannot reliably detect sarcasm, which is common on political Twitter.
- **The compound score aggregates the full tweet.** For a tweet that mentions multiple people or topics, the score reflects the overall tone of the tweet, not specifically how it relates to the politician.

### Institutional account exclusion

- **The exclusion list is manually maintained.** New bots or news accounts are not automatically detected — they need to be added to `EXCLUDED_ACCOUNTS` manually. Excluded tweets are retained in the dataset and flagged rather than deleted, so the list can always be updated and applied retroactively.
