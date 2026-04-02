#!/usr/bin/env python3
"""
Backfill username/name into existing tweet JSON files by resolving author_ids
via the X API users lookup endpoint (GET /2/users, max 100 ids per call).

Progress is saved to a cache file after every batch, so the script can be
safely interrupted and resumed — already-resolved IDs are skipped on re-run.

Usage:
    python backfill_usernames.py          # dry run: one batch of 100, prints results
    python backfill_usernames.py --all    # full run: fetch all, patch JSONs, rebuild CSVs
    python backfill_usernames.py --apply  # skip fetching, apply existing cache to JSONs/CSVs
"""

import json
import sys
import time
import requests

from download_tweets import BEARER_TOKEN, JACKIE, DANIEL, finalize_csv

USERS_URL  = "https://api.twitter.com/2/users"
BATCH_SIZE = 100
CACHE_PATH = "user_cache.json"
JSON_PATHS = [JACKIE.json_path, DANIEL.json_path]

# How long to wait when we hit a 403 (likely a 15-min rate limit window)
RATE_LIMIT_WAIT = 16 * 60  # 16 minutes


def collect_author_ids():
    """Return sorted deduplicated list of author_ids across all politician JSON files."""
    ids = set()
    for path in JSON_PATHS:
        with open(path) as f:
            for t in json.load(f)["tweets"]:
                ids.add(t["author_id"])
    return sorted(ids)


def load_cache():
    """Load previously resolved users from cache file."""
    try:
        with open(CACHE_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_cache(user_map):
    """Persist resolved users to cache file."""
    with open(CACHE_PATH, "w") as f:
        json.dump(user_map, f)


def fetch_users(author_ids):
    """Resolve a batch of author_ids (max 100) to {id: {username, name}}.
    Handles both 429 and 403 as rate limits."""
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    params  = {"ids": ",".join(author_ids), "user.fields": "username,name"}

    for attempt in range(5):
        resp = requests.get(USERS_URL, headers=headers, params=params)

        if resp.status_code == 429:
            reset_ts = resp.headers.get("x-rate-limit-reset")
            wait = max(0, int(reset_ts) - int(time.time())) + 5 if reset_ts else 60 * (attempt + 1)
            print(f"  429 rate limit. Waiting {wait}s...")
            time.sleep(wait)
            continue

        if resp.status_code == 403:
            print(f"  403 rate limit (15-min window). Waiting {RATE_LIMIT_WAIT}s...")
            time.sleep(RATE_LIMIT_WAIT)
            continue

        resp.raise_for_status()
        return {u["id"]: {"username": u["username"], "name": u["name"]}
                for u in resp.json().get("data", [])}

    resp.raise_for_status()


def patch_json(json_path, user_map):
    """Write username/name into every tweet in a JSON file using user_map."""
    with open(json_path) as f:
        data = json.load(f)
    tweets  = data["tweets"]
    patched = 0
    for t in tweets:
        user = user_map.get(t["author_id"])
        if user:
            t["username"] = user["username"]
            t["name"]     = user["name"]
            patched += 1
        else:
            t.setdefault("username", "")
            t.setdefault("name", "")
    with open(json_path, "w") as f:
        json.dump({"tweets": tweets}, f, indent=2)
    print(f"  Patched {patched}/{len(tweets)} tweets in {json_path}.")


def apply_cache():
    """Patch JSONs and rebuild CSVs using whatever is in the cache file."""
    user_map = load_cache()
    if not user_map:
        print("Cache is empty — nothing to apply.")
        return
    print(f"Applying {len(user_map)} cached users to JSON files...\n")
    for politician, path in [(JACKIE, JACKIE.json_path), (DANIEL, DANIEL.json_path)]:
        print(f"Patching {path}...")
        patch_json(path, user_map)
        print(f"Rebuilding {politician.csv_path}...")
        finalize_csv(politician)
    print("\nDone.")


def batches(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


if __name__ == "__main__":
    args = sys.argv[1:]

    if "--apply" in args:
        apply_cache()
        sys.exit(0)

    author_ids = collect_author_ids()
    print(f"Total unique author_ids: {len(author_ids)}")

    if "--all" not in args:
        # Dry run: one batch, print results
        print(f"Batches needed: {-(-len(author_ids) // BATCH_SIZE)}")
        batch = author_ids[:BATCH_SIZE]
        print(f"\nDry run: fetching first {len(batch)} author_ids...")
        user_map = fetch_users(batch)
        print(f"Resolved {len(user_map)} users:\n")
        for uid, u in sorted(user_map.items(), key=lambda x: x[1]["username"].lower()):
            print(f"  {uid:22s}  @{u['username']:30s}  {u['name']}")
        print(f"\nRun with --all to apply all {-(-len(author_ids) // BATCH_SIZE)} batches.")
        sys.exit(0)

    # Full run — resume from cache
    user_map   = load_cache()
    remaining  = [uid for uid in author_ids if uid not in user_map]
    all_batches = list(batches(remaining, BATCH_SIZE))

    print(f"Already cached: {len(user_map)}  |  Remaining: {len(remaining)}  |  Batches: {len(all_batches)}")

    if not remaining:
        print("All IDs already cached. Running --apply...")
        apply_cache()
        sys.exit(0)

    for i, batch in enumerate(all_batches):
        print(f"Batch {i + 1}/{len(all_batches)} ({len(batch)} ids)...")
        user_map.update(fetch_users(batch))
        save_cache(user_map)  # persist after every batch
        if i < len(all_batches) - 1:
            time.sleep(1)

    print(f"\nResolved {len(user_map)} / {len(author_ids)} users total.\n")
    apply_cache()
