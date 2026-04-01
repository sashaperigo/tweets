#!/usr/bin/env python3
"""
Download tweets mentioning "@JackieFielder_" or "Jackie Fielder" (case insensitive)
using the X (Twitter) API v2 recent search endpoint.
"""

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

# Case-insensitive OR search — Twitter API v2 query syntax
# The API is case-insensitive by default for keyword/mention searches
QUERY = '"Jackie Fielder" OR @JackieFielder_ -is:retweet'

TWEET_FIELDS = "created_at,public_metrics,author_id"
USER_FIELDS = "username,name"
EXPANSIONS = "author_id"
MAX_RESULTS = 100  # max per page for recent search


def fetch_page(next_token=None):
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

    resp = requests.get(SEARCH_URL, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()


def download_all_tweets():
    all_tweets = []
    users_by_id = {}
    next_token = None

    while True:
        data = fetch_page(next_token)

        # Index user info from expansions
        for user in data.get("includes", {}).get("users", []):
            users_by_id[user["id"]] = user

        for tweet in data.get("data", []):
            user = users_by_id.get(tweet.get("author_id"), {})
            tweet["username"] = user.get("username", "")
            tweet["name"] = user.get("name", "")
            all_tweets.append(tweet)

        meta = data.get("meta", {})
        next_token = meta.get("next_token")
        result_count = meta.get("result_count", 0)
        print(f"Fetched {result_count} tweets (total so far: {len(all_tweets)})")

        if not next_token:
            break

        time.sleep(1)  # stay within rate limits

    return all_tweets


def save_json(tweets, path="jackie_fielder_tweets.json"):
    with open(path, "w") as f:
        json.dump({"tweets": [{
            "created_at": t["created_at"],
            "author_id": t["author_id"],
            "edit_history_tweet_ids": t.get("edit_history_tweet_ids", [t["id"]]),
            "text": t["text"],
            "public_metrics": t["public_metrics"],
            "id": t["id"],
        } for t in tweets]}, f, indent=2)
    print(f"Saved JSON to {path}")


def save_csv(tweets, path="jackie_fielder_tweets.csv"):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
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
    tweets = download_all_tweets()
    print(f"\nTotal tweets downloaded: {len(tweets)}")
    save_json(tweets)
    save_csv(tweets)
