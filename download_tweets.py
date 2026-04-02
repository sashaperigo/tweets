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


def is_reply_to_other(text):
    """Return True for type-2 tweets: replies where Jackie is tagged but isn't
    the first @mention (i.e. the tweet is directed at someone else)."""
    if not text.startswith("@"):
        return False
    first_mention = re.match(r"@(\w+)", text)
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
            if is_reply_to_other(tweet["text"]) or tweet["text"].startswith("RT @"):
                continue
            user = users_by_id.get(tweet.get("author_id"), {})
            tweet["username"] = user.get("username", "")
            tweet["name"] = user.get("name", "")
            print(f"  [{tweet['created_at']}] @{tweet['username']}: {tweet['text']}")
            batch.append(tweet)

        if batch:
            save_json(batch)
            save_csv(batch)
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


def backfill_chunk(start_time, end_time):
    """Fetch all tweets in a single time window, paginating until exhausted."""
    users_by_id = {}
    next_token = None
    total = 0

    while True:
        data = fetch_page(next_token, start_time=start_time, end_time=end_time, url=SEARCH_URL_ALL)

        for user in data.get("includes", {}).get("users", []):
            users_by_id[user["id"]] = user

        batch = []
        for tweet in data.get("data", []):
            if is_reply_to_other(tweet["text"]) or tweet["text"].startswith("RT @"):
                continue
            user = users_by_id.get(tweet.get("author_id"), {})
            tweet["username"] = user.get("username", "")
            tweet["name"] = user.get("name", "")
            print(f"  [{tweet['created_at']}] @{tweet['username']}: {tweet['text']}")
            batch.append(tweet)

        if batch:
            save_json(batch)
            save_csv(batch)
            total += len(batch)

        meta = data.get("meta", {})
        next_token = meta.get("next_token")
        result_count = meta.get("result_count", 0)
        print(f"--- page done: {result_count} fetched, {total} saved for this chunk ---")

        if not next_token:
            break

        time.sleep(1)

    return total


def backfill_tweets(end_time, start_time):
    """Backfill tweets from start_time to end_time by querying one month at a time.
    Uses search/all — requires a paid X API tier.
    """
    from datetime import datetime, timezone, timedelta

    start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))

    total = 0
    chunk_start = start_dt
    while chunk_start < end_dt:
        # Advance one month at a time
        if chunk_start.month == 12:
            chunk_end = chunk_start.replace(year=chunk_start.year + 1, month=1)
        else:
            chunk_end = chunk_start.replace(month=chunk_start.month + 1)
        chunk_end = min(chunk_end, end_dt)

        cs = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        ce = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"\n=== Backfilling {cs} → {ce} ===")
        total += backfill_chunk(cs, ce)
        chunk_start = chunk_end

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
    if not os.path.exists(STAGING_PATH):
        raise FileNotFoundError(
            f"Staging file '{STAGING_PATH}' not found. "
            "Was save_json() called during this run?"
        )
    staged = []
    with open(STAGING_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                staged.append(json.loads(line))
    if not staged:
        print("  Staging file is empty — no new tweets were found this run.")
        os.remove(STAGING_PATH)
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

    os.remove(STAGING_PATH)
    print(f"  Done. Staging file removed.")
    return len(new_tweets)


def save_csv(tweets, path="jackie_fielder_tweets.csv"):
    append = os.path.exists(path)
    with open(path, "a" if append else "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not append:
            writer.writerow(["id", "created_at", "username", "name", "text",
                             "likes", "retweets", "replies", "quotes", "impressions"])
        for t in tweets:
            m = t.get("public_metrics", {})
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
            ])
    print(f"Saved CSV to {path}")


if __name__ == "__main__":
    import sys

    if "--backfill" in sys.argv:
        end_time = get_earliest_time("jackie_fielder_tweets.csv")
        if not end_time:
            print("No existing data found — run without --backfill first.")
            sys.exit(1)
        start_time = "2025-11-01T00:00:00Z"
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
