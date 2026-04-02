#!/usr/bin/env python3
"""
Download tweets mentioning "@JackieFielder_" or "Jackie Fielder" (case insensitive)
using the X (Twitter) API v2 recent search endpoint.
"""

import re
import requests
import json
import csv
import time
import os

from nltk.sentiment.vader import SentimentIntensityAnalyzer

_vader = SentimentIntensityAnalyzer()

# Load credentials from xapi-keys file
def load_bearer_token(keys_file="xapi-keys"):
    with open(keys_file) as f:
        for line in f:
            if line.startswith("BEARER_TOKEN"):
                return line.split("=", 1)[1].strip()
    raise ValueError("BEARER_TOKEN not found in keys file")

BEARER_TOKEN = load_bearer_token()

SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"
SEARCH_URL_ALL = "https://api.twitter.com/2/tweets/search/all"  # requires paid API access

# Case-insensitive OR search — Twitter API v2 query syntax
# The API is case-insensitive by default for keyword/mention searches
QUERY = '"Jackie Fielder" OR @JackieFielder_ -is:retweet'

TWEET_FIELDS = "created_at,public_metrics,author_id"
USER_FIELDS = "username,name"
EXPANSIONS = "author_id"
MAX_RESULTS = 100  # max per page for recent search

EXCLUDED_ACCOUNTS = {
    "sfpdcallsbot", "sfchronicle", "kqednews",
    "mlnow", "sfist", "sfstandard", "48hills", "grok",
}

_RE_JACKIE_START   = re.compile(r"^@JackieFielder_\b", re.IGNORECASE)
_RE_JACKIE_ANYWHERE = re.compile(r"@JackieFielder_\b", re.IGNORECASE)
_RE_FIRST_MENTION  = re.compile(r"@(\w+)")


def get_is_excluded(username):
    """Return True if username is in the excluded accounts list (case-insensitive)."""
    return username.lower() in EXCLUDED_ACCOUNTS


def get_sentiment(text):
    """Return (label, compound_score) for text using VADER.

    label is 'positive' (compound >= 0.05), 'negative' (compound <= -0.05),
    or 'neutral' otherwise.
    """
    scores = _vader.polarity_scores(text)
    compound = scores["compound"]
    if compound >= 0.05:
        return "positive", compound
    if compound <= -0.05:
        return "negative", compound
    return "neutral", compound


def get_reply_type(text):
    """Classify a tweet's relationship to @JackieFielder_.

    Returns:
        "Direct reply"   — tweet starts with @JackieFielder_
        "Thread mention" — tweet starts with two or more @handles, Jackie is among them but not first
        "Mention"        — tweet contains @JackieFielder_ embedded in regular text (not a reply)
        "Not tagged"     — tweet doesn't contain @JackieFielder_ at all
    """
    if _RE_JACKIE_START.match(text):
        return "Direct reply"
    if is_reply_to_other(text):
        return "Thread mention"
    if _RE_JACKIE_ANYWHERE.search(text):
        return "Mention"
    return "Not tagged"


def is_reply_to_other(text):
    """Return True for type-2 tweets: replies where Jackie is tagged but isn't
    the first @mention (i.e. the tweet is directed at someone else)."""
    if not text.startswith("@"):
        return False
    first_mention = _RE_FIRST_MENTION.match(text)
    return first_mention and first_mention.group(1).lower() != "jackiefielder_"


def fetch_page(next_token=None, since_id=None, until_id=None, start_time=None, end_time=None, url=None):
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    params = {
        "query": QUERY,
        "tweet.fields": TWEET_FIELDS,
        "user.fields": USER_FIELDS,
        "expansions": EXPANSIONS,
        "max_results": MAX_RESULTS,
    }
    if next_token:
        params["next_token"] = next_token
    else:
        if since_id:
            params["since_id"] = since_id
        if until_id:
            params["until_id"] = until_id
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

    for attempt in range(5):
        print(f"  Fetching from API{' (retry ' + str(attempt) + ')' if attempt else ''}...")
        resp = requests.get(url or SEARCH_URL, headers=headers, params=params)
        if resp.status_code == 429:
            reset_ts = resp.headers.get("x-rate-limit-reset")
            if reset_ts:
                wait = max(0, int(reset_ts) - int(time.time())) + 5
            else:
                wait = 60 * (attempt + 1)
            print(f"Rate limited. Waiting {wait}s before retrying...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()


def get_latest_id(csv_path):
    """Return the highest tweet ID already saved, or None if file doesn't exist."""
    if not os.path.exists(csv_path):
        return None
    with open(csv_path, newline="", encoding="utf-8") as f:
        ids = [int(row["id"]) for row in csv.DictReader(f) if row.get("id")]
    return str(max(ids)) if ids else None


def download_all_tweets(since_id=None):
    users_by_id = {}
    next_token = None
    total = 0

    while True:
        data = fetch_page(next_token, since_id=since_id)

        for user in data.get("includes", {}).get("users", []):
            users_by_id[user["id"]] = user

        batch = []
        past_boundary = False
        for tweet in data.get("data", []):
            if since_id and int(tweet["id"]) <= int(since_id):
                past_boundary = True
                continue
            if tweet["text"].startswith("RT @"):
                continue
            user = users_by_id.get(tweet.get("author_id"), {})
            tweet["username"] = user.get("username", "")
            tweet["name"] = user.get("name", "")
            print(f"  [{tweet['created_at']}] @{tweet['username']}: {tweet['text']}")
            batch.append(tweet)

        if batch:
            save_json(batch)
            total += len(batch)

        meta = data.get("meta", {})
        next_token = meta.get("next_token")
        result_count = meta.get("result_count", 0)
        print(f"--- page done: {result_count} fetched, {total} new saved so far ---")

        if not next_token or past_boundary:
            break

        time.sleep(1)  # stay within rate limits

    return total


def get_earliest_time(csv_path):
    """Return the earliest created_at timestamp in the CSV, or None if file doesn't exist."""
    if not os.path.exists(csv_path):
        return None
    with open(csv_path, newline="", encoding="utf-8") as f:
        dates = [row["created_at"] for row in csv.DictReader(f) if row.get("created_at")]
    return min(dates) if dates else None


def backfill_tweets(end_time, start_time):
    """Backfill tweets older than end_time, stopping at start_time.

    Passes end_time to the API so results begin just before our earliest
    saved tweet and go backwards. Enforces start_time client-side: filters
    out tweets older than start_time and stops paginating when we hit one.
    Uses search/all — requires a paid X API tier.
    """
    from datetime import datetime

    start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))

    users_by_id = {}
    next_token = None
    total = 0

    while True:
        data = fetch_page(next_token, end_time=end_time, url=SEARCH_URL_ALL)

        for user in data.get("includes", {}).get("users", []):
            users_by_id[user["id"]] = user

        batch = []
        hit_boundary = False
        for tweet in data.get("data", []):
            tweet_dt = datetime.fromisoformat(tweet["created_at"].replace("Z", "+00:00"))
            if tweet_dt < start_dt:
                hit_boundary = True
                continue
            if tweet["text"].startswith("RT @"):
                continue
            user = users_by_id.get(tweet.get("author_id"), {})
            tweet["username"] = user.get("username", "")
            tweet["name"] = user.get("name", "")
            print(f"  [{tweet['created_at']}] @{tweet['username']}: {tweet['text']}")
            batch.append(tweet)

        if batch:
            save_json(batch)
            total += len(batch)

        meta = data.get("meta", {})
        next_token = meta.get("next_token")
        result_count = meta.get("result_count", 0)
        print(f"--- page done: {result_count} fetched, {total} new saved so far ---")

        if not next_token or hit_boundary:
            break

        time.sleep(1)

    return total


STAGING_PATH = "jackie_fielder_tweets_staging.jsonl"


def save_json(tweets, path="jackie_fielder_tweets.json"):
    """Append new tweets to a staging JSONL file (fast, O(1) per batch)."""
    print(f"  Appending {len(tweets)} tweets to staging file...")
    with open(STAGING_PATH, "a", encoding="utf-8") as f:
        for t in tweets:
            f.write(json.dumps({
                "created_at": t["created_at"],
                "author_id": t["author_id"],
                "edit_history_tweet_ids": t.get("edit_history_tweet_ids", [t["id"]]),
                "text": t["text"],
                "public_metrics": t["public_metrics"],
                "id": t["id"],
            }) + "\n")
    print(f"  Staging file updated.")


def finalize_json(path="jackie_fielder_tweets.json"):
    """Merge staging file into main JSON, deduplicate, and clean up staging file."""
    print(f"\nFinalizing {path}...")

    print("  Loading existing tweets...")
    if os.path.exists(path):
        with open(path) as f:
            existing = json.load(f)["tweets"]
    else:
        existing = []
    print(f"  Loaded {len(existing)} existing tweets.")

    print("  Loading staged tweets...")
    staged = []
    if os.path.exists(STAGING_PATH):
        with open(STAGING_PATH, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    staged.append(json.loads(line))
        os.remove(STAGING_PATH)
    if not staged:
        print("  No new tweets were found this run.")
        return 0
    print(f"  Loaded {len(staged)} staged tweets.")

    print("  Deduplicating...")
    seen_ids = {t["id"] for t in existing}
    new_tweets = []
    for t in staged:
        if t["id"] not in seen_ids:
            seen_ids.add(t["id"])
            new_tweets.append(t)
    merged = existing + new_tweets
    print(f"  {len(new_tweets)} new unique tweets. Total: {len(merged)}.")

    print(f"  Writing {path}...")
    with open(path, "w") as f:
        json.dump({"tweets": merged}, f, indent=2)

    print(f"  Done.")
    return len(new_tweets)


def save_csv(tweets, path="jackie_fielder_tweets.csv"):
    append = os.path.exists(path)
    with open(path, "a" if append else "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not append:
            writer.writerow(["id", "created_at", "username", "name", "text",
                             "likes", "retweets", "replies", "quotes", "impressions",
                             "reply_type", "sentiment", "sentiment_score", "is_excluded"])
        for t in tweets:
            m = t.get("public_metrics", {})
            sentiment_label, sentiment_score = get_sentiment(t["text"])
            writer.writerow([
                t["id"],
                t["created_at"],
                t.get("username", ""),
                t.get("name", ""),
                t["text"],
                m.get("like_count", 0),
                m.get("retweet_count", 0),
                m.get("reply_count", 0),
                m.get("quote_count", 0),
                m.get("impression_count", 0),
                get_reply_type(t["text"]),
                sentiment_label,
                sentiment_score,
                get_is_excluded(t.get("username", "")),
            ])
    print(f"Saved CSV to {path}")


def finalize_csv(json_path="jackie_fielder_tweets.json", csv_path="jackie_fielder_tweets.csv"):
    """Rebuild CSV from scratch using the deduplicated JSON as the source of truth."""
    print(f"\nFinalizing {csv_path} from {json_path}...")
    with open(json_path) as f:
        tweets = json.load(f)["tweets"]
    if os.path.exists(csv_path):
        os.remove(csv_path)
    save_csv(tweets, path=csv_path)
    print(f"  Wrote {len(tweets)} rows to {csv_path}.")


if __name__ == "__main__":
    import sys

    if "--backfill" in sys.argv:
        end_time = get_earliest_time("jackie_fielder_tweets.csv")
        if not end_time:
            print("No existing data found — run without --backfill first.")
            sys.exit(1)
        start_time = "2025-10-01T00:00:00Z"
        print(f"Backfilling tweets from {start_time} to {end_time}")
        total = backfill_tweets(end_time=end_time, start_time=start_time)
    else:
        since_id = get_latest_id("jackie_fielder_tweets.csv")
        if since_id:
            print(f"Resuming from tweet ID {since_id} (skipping already-downloaded tweets)")
        total = download_all_tweets(since_id=since_id)

    print(f"\nTotal tweets fetched this run: {total}")
    new_count = finalize_json()
    print(f"Total new unique tweets added to database: {new_count}")
    if new_count > 0:
        finalize_csv()
