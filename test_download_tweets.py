"""
Tests for download_tweets.py that do not hit the X API.
"""

import csv
import json
import os
import tempfile
import unittest
from unittest.mock import patch

# Stub out BEARER_TOKEN so the module can be imported without the xapi-keys file
with patch("builtins.open", side_effect=lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError)):
    pass

import unittest.mock as mock

# Patch load_bearer_token before importing the module
with mock.patch("builtins.open", mock.mock_open(read_data="BEARER_TOKEN = dummy\n")):
    import download_tweets as dt


# ---------------------------------------------------------------------------
# is_reply_to_other
# ---------------------------------------------------------------------------

class TestIsReplyToOther(unittest.TestCase):

    def test_type1_direct_reply_to_jackie(self):
        # Starts with @JackieFielder_ → NOT a type-2, should return False
        self.assertFalse(dt.is_reply_to_other("@JackieFielder_ thanks for sharing"))

    def test_type1_case_insensitive(self):
        self.assertFalse(dt.is_reply_to_other("@JACKIEFIELDER_ great post"))
        self.assertFalse(dt.is_reply_to_other("@jackiefielder_ ok"))

    def test_type2_other_person_first(self):
        # Starts with @someoneelse → type-2, should return True
        self.assertTrue(dt.is_reply_to_other("@someoneelse @JackieFielder_ check this out"))

    def test_type2_multiple_mentions_before_jackie(self):
        self.assertTrue(dt.is_reply_to_other("@alice @bob @JackieFielder_ agreed"))

    def test_type3_no_leading_mention(self):
        # Doesn't start with @ → not a reply at all, should return False
        self.assertFalse(dt.is_reply_to_other("Jackie Fielder just announced something big"))
        self.assertFalse(dt.is_reply_to_other("Did you see what @JackieFielder_ said?"))

    def test_retweet_style(self):
        # RT @ doesn't start with @ so is not a reply
        self.assertFalse(dt.is_reply_to_other("RT @JackieFielder_ this is a retweet"))

    def test_rt_filter(self):
        # The RT check used in download loops
        self.assertTrue("RT @someone great tweet".startswith("RT @"))
        self.assertFalse("@JackieFielder_ nice".startswith("RT @"))
        self.assertFalse("Jackie Fielder said something".startswith("RT @"))

    def test_type1_handle_with_extra_characters(self):
        # @JackieFielder_something is a different handle — should be treated as type-2
        self.assertTrue(dt.is_reply_to_other("@JackieFielder_something hey"))

    def test_empty_string(self):
        self.assertFalse(dt.is_reply_to_other(""))


# ---------------------------------------------------------------------------
# get_latest_id / get_earliest_id / get_earliest_time
# ---------------------------------------------------------------------------

def _write_csv(path, rows):
    """Write a minimal tweets CSV with given rows (list of dicts)."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "created_at", "username", "name",
                                                "text", "likes", "retweets", "replies",
                                                "quotes", "impressions"])
        writer.writeheader()
        writer.writerows(rows)


class TestGetIds(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        self.tmp.close()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _rows(self):
        return [
            {"id": "1000", "created_at": "2025-11-01T10:00:00.000Z", "username": "a",
             "name": "A", "text": "hello", "likes": 0, "retweets": 0, "replies": 0,
             "quotes": 0, "impressions": 0},
            {"id": "2000", "created_at": "2025-12-01T10:00:00.000Z", "username": "b",
             "name": "B", "text": "world", "likes": 0, "retweets": 0, "replies": 0,
             "quotes": 0, "impressions": 0},
            {"id": "3000", "created_at": "2026-01-15T10:00:00.000Z", "username": "c",
             "name": "C", "text": "foo",   "likes": 0, "retweets": 0, "replies": 0,
             "quotes": 0, "impressions": 0},
        ]

    def test_get_latest_id(self):
        _write_csv(self.tmp.name, self._rows())
        self.assertEqual(dt.get_latest_id(self.tmp.name), "3000")

    def test_get_earliest_time(self):
        _write_csv(self.tmp.name, self._rows())
        self.assertEqual(dt.get_earliest_time(self.tmp.name), "2025-11-01T10:00:00.000Z")

    def test_missing_file_returns_none(self):
        self.assertIsNone(dt.get_latest_id("/nonexistent/path.csv"))
        self.assertIsNone(dt.get_earliest_time("/nonexistent/path.csv"))

    def test_get_latest_id_with_duplicates(self):
        rows = self._rows() + [
            {"id": "3000", "created_at": "2026-01-15T10:00:00.000Z", "username": "c",
             "name": "C", "text": "dup", "likes": 0, "retweets": 0, "replies": 0,
             "quotes": 0, "impressions": 0},
        ]
        _write_csv(self.tmp.name, rows)
        self.assertEqual(dt.get_latest_id(self.tmp.name), "3000")

    def test_empty_csv_returns_none(self):
        _write_csv(self.tmp.name, [])
        self.assertIsNone(dt.get_latest_id(self.tmp.name))
        self.assertIsNone(dt.get_earliest_time(self.tmp.name))


# ---------------------------------------------------------------------------
# backfill_tweets chunking logic
# ---------------------------------------------------------------------------

def _make_api_tweet(tweet_id, created_at, text="hello Jackie Fielder", author_id="1"):
    return {
        "id": tweet_id,
        "created_at": created_at,
        "author_id": author_id,
        "text": text,
        "edit_history_tweet_ids": [tweet_id],
        "public_metrics": {"like_count": 0, "retweet_count": 0,
                           "reply_count": 0, "quote_count": 0, "impression_count": 0},
    }


def _fake_fetch_page(pages):
    """Returns a fetch_page side-effect that yields pages in order."""
    pages = list(pages)
    call_count = [0]
    def _fetch(*args, **kwargs):
        i = call_count[0]
        call_count[0] += 1
        if i < len(pages):
            return pages[i]
        return {"data": [], "meta": {"result_count": 0}}
    return _fetch


class TestBackfillClientSideBoundary(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.staging_path = os.path.join(self.tmpdir, "staging.jsonl")
        self._orig_staging = dt.STAGING_PATH
        dt.STAGING_PATH = self.staging_path

    def tearDown(self):
        dt.STAGING_PATH = self._orig_staging
        for f in os.listdir(self.tmpdir):
            os.unlink(os.path.join(self.tmpdir, f))
        os.rmdir(self.tmpdir)

    def _run_backfill(self, pages):
        with mock.patch.object(dt, "fetch_page", side_effect=_fake_fetch_page(pages)), \
             mock.patch.object(dt, "save_csv"):
            return dt.backfill_tweets(
                end_time="2025-11-07T00:00:00Z",
                start_time="2025-10-01T00:00:00Z",
            )

    def test_tweets_within_range_are_saved(self):
        pages = [{"data": [
            _make_api_tweet("1", "2025-11-06T12:00:00Z"),
            _make_api_tweet("2", "2025-10-15T12:00:00Z"),
        ], "includes": {"users": [{"id": "1", "username": "u", "name": "U"}]},
           "meta": {"result_count": 2, "next_token": None}}]
        count = self._run_backfill(pages)
        self.assertEqual(count, 2)

    def test_stops_when_tweet_older_than_start_time(self):
        page1 = {"data": [
            _make_api_tweet("1", "2025-10-20T00:00:00Z"),
        ], "includes": {"users": [{"id": "1", "username": "u", "name": "U"}]},
           "meta": {"result_count": 1, "next_token": "tok"}}
        page2 = {"data": [
            _make_api_tweet("2", "2025-09-15T00:00:00Z"),  # before start_time — stop
        ], "includes": {"users": [{"id": "1", "username": "u", "name": "U"}]},
           "meta": {"result_count": 1, "next_token": "tok2"}}
        fetch = _fake_fetch_page([page1, page2])
        call_count = [0]
        def counting_fetch(*args, **kwargs):
            call_count[0] += 1
            return fetch(*args, **kwargs)
        with mock.patch.object(dt, "fetch_page", side_effect=counting_fetch), \
             mock.patch.object(dt, "save_csv"):
            dt.backfill_tweets(end_time="2025-11-07T00:00:00Z", start_time="2025-10-01T00:00:00Z")
        # Should have stopped after page 2, not fetched a third page
        self.assertEqual(call_count[0], 2)


# ---------------------------------------------------------------------------
# save_json (staging) + finalize_json
# ---------------------------------------------------------------------------

SAMPLE_TWEETS = [
    {
        "id": "100", "created_at": "2025-11-01T00:00:00.000Z",
        "author_id": "99", "text": "tweet one", "username": "user1", "name": "User 1",
        "edit_history_tweet_ids": ["100"],
        "public_metrics": {"like_count": 1, "retweet_count": 0,
                           "reply_count": 0, "quote_count": 0, "impression_count": 10},
    },
    {
        "id": "200", "created_at": "2025-11-02T00:00:00.000Z",
        "author_id": "88", "text": "tweet two", "username": "user2", "name": "User 2",
        "edit_history_tweet_ids": ["200"],
        "public_metrics": {"like_count": 2, "retweet_count": 1,
                           "reply_count": 0, "quote_count": 0, "impression_count": 20},
    },
]


class TestStagingAndFinalize(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.json_path = os.path.join(self.tmpdir, "tweets.json")
        self.staging_path = os.path.join(self.tmpdir, "tweets_staging.jsonl")
        # Point the module at our temp paths
        self._orig_staging = dt.STAGING_PATH
        dt.STAGING_PATH = self.staging_path

    def tearDown(self):
        dt.STAGING_PATH = self._orig_staging
        for f in os.listdir(self.tmpdir):
            os.unlink(os.path.join(self.tmpdir, f))
        os.rmdir(self.tmpdir)

    def test_save_json_appends_to_staging(self):
        dt.save_json(SAMPLE_TWEETS, path=self.json_path)
        self.assertTrue(os.path.exists(self.staging_path))
        with open(self.staging_path) as f:
            lines = [l for l in f if l.strip()]
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])["id"], "100")
        self.assertEqual(json.loads(lines[1])["id"], "200")

    def test_finalize_merges_staging_into_json(self):
        dt.save_json(SAMPLE_TWEETS, path=self.json_path)
        dt.finalize_json(path=self.json_path)
        with open(self.json_path) as f:
            data = json.load(f)
        self.assertEqual(len(data["tweets"]), 2)
        ids = {t["id"] for t in data["tweets"]}
        self.assertEqual(ids, {"100", "200"})

    def test_finalize_removes_staging_file(self):
        dt.save_json(SAMPLE_TWEETS, path=self.json_path)
        dt.finalize_json(path=self.json_path)
        self.assertFalse(os.path.exists(self.staging_path))

    def test_finalize_deduplicates(self):
        # Save the same tweets twice (simulates duplicate pages)
        dt.save_json(SAMPLE_TWEETS, path=self.json_path)
        dt.save_json(SAMPLE_TWEETS, path=self.json_path)
        dt.finalize_json(path=self.json_path)
        with open(self.json_path) as f:
            data = json.load(f)
        self.assertEqual(len(data["tweets"]), 2)

    def test_finalize_merges_with_existing_json(self):
        # Pre-populate the JSON with tweet 100
        with open(self.json_path, "w") as f:
            json.dump({"tweets": [{"id": "100", "created_at": "2025-11-01T00:00:00.000Z",
                                   "author_id": "99", "text": "tweet one",
                                   "edit_history_tweet_ids": ["100"],
                                   "public_metrics": {}}]}, f)
        # Stage tweet 200 (new) and tweet 100 (duplicate)
        dt.save_json(SAMPLE_TWEETS, path=self.json_path)
        dt.finalize_json(path=self.json_path)
        with open(self.json_path) as f:
            data = json.load(f)
        self.assertEqual(len(data["tweets"]), 2)

    def test_finalize_returns_new_count(self):
        dt.save_json(SAMPLE_TWEETS, path=self.json_path)
        new_count = dt.finalize_json(path=self.json_path)
        self.assertEqual(new_count, 2)

    def test_finalize_returns_zero_if_no_staging_file(self):
        # Staging file never created (0 tweets found) — should return 0, not raise
        result = dt.finalize_json(path=self.json_path)
        self.assertEqual(result, 0)

    def test_finalize_empty_staging_returns_zero_without_error(self):
        # Staging file exists but is empty — no tweets found this run
        open(self.staging_path, "w").close()
        result = dt.finalize_json(path=self.json_path)
        self.assertEqual(result, 0)
        self.assertFalse(os.path.exists(self.staging_path))

    def test_finalize_returns_zero_for_all_duplicates(self):
        # Put both tweets in main JSON already
        records = [{"id": t["id"], "created_at": t["created_at"], "author_id": t["author_id"],
                    "text": t["text"], "edit_history_tweet_ids": t["edit_history_tweet_ids"],
                    "public_metrics": t["public_metrics"]} for t in SAMPLE_TWEETS]
        with open(self.json_path, "w") as f:
            json.dump({"tweets": records}, f)
        dt.save_json(SAMPLE_TWEETS, path=self.json_path)
        new_count = dt.finalize_json(path=self.json_path)
        self.assertEqual(new_count, 0)


# ---------------------------------------------------------------------------
# save_csv
# ---------------------------------------------------------------------------

class TestSaveCsv(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        self.tmp.close()
        os.unlink(self.tmp.name)  # start with no file

    def tearDown(self):
        if os.path.exists(self.tmp.name):
            os.unlink(self.tmp.name)

    def test_creates_file_with_header(self):
        dt.save_csv(SAMPLE_TWEETS, path=self.tmp.name)
        with open(self.tmp.name, newline="") as f:
            rows = list(csv.reader(f))
        self.assertEqual(rows[0], ["id", "created_at", "username", "name", "text",
                                   "likes", "retweets", "replies", "quotes", "impressions",
                                   "reply_type"])
        self.assertEqual(len(rows), 3)  # header + 2 tweets

    def test_appends_without_duplicate_header(self):
        dt.save_csv(SAMPLE_TWEETS[:1], path=self.tmp.name)
        dt.save_csv(SAMPLE_TWEETS[1:], path=self.tmp.name)
        with open(self.tmp.name, newline="") as f:
            rows = list(csv.reader(f))
        headers = [r for r in rows if r[0] == "id"]
        self.assertEqual(len(headers), 1)
        self.assertEqual(len(rows), 3)  # 1 header + 2 data rows

    def test_correct_field_values(self):
        dt.save_csv(SAMPLE_TWEETS[:1], path=self.tmp.name)
        with open(self.tmp.name, newline="") as f:
            reader = csv.DictReader(f)
            row = next(reader)
        self.assertEqual(row["id"], "100")
        self.assertEqual(row["username"], "user1")
        self.assertEqual(row["likes"], "1")
        self.assertEqual(row["impressions"], "10")


# ---------------------------------------------------------------------------
# get_reply_type
# ---------------------------------------------------------------------------

class TestGetReplyType(unittest.TestCase):

    def test_direct_reply_starts_with_jackiefielder(self):
        self.assertEqual(dt.get_reply_type("@JackieFielder_ great point"), "Direct reply")

    def test_direct_reply_case_insensitive(self):
        self.assertEqual(dt.get_reply_type("@JACKIEFIELDER_ ok"), "Direct reply")
        self.assertEqual(dt.get_reply_type("@jackiefielder_ ok"), "Direct reply")

    def test_mention_contains_handle_mid_tweet(self):
        self.assertEqual(
            dt.get_reply_type("Did you see what @JackieFielder_ said?"), "Mention")

    def test_mention_name_only_no_handle(self):
        # Plain name with no @ is "Not tagged"
        self.assertEqual(dt.get_reply_type("Jackie Fielder made a statement today"), "Not tagged")

    def test_not_tagged_no_reference(self):
        self.assertEqual(dt.get_reply_type("San Francisco politics are wild"), "Not tagged")

    def test_mention_handle_after_other_text(self):
        self.assertEqual(
            dt.get_reply_type("Someone should ask @JackieFielder_ about this"), "Mention")

    def test_every_csv_row_has_valid_reply_type(self):
        valid = {"Direct reply", "Mention", "Not tagged"}
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        self.assertIn("reply_type", rows[0].keys(),
                      "CSV is missing 'reply_type' column")
        invalid = [r for r in rows if r.get("reply_type") not in valid]
        self.assertEqual(invalid, [],
                         f"{len(invalid)} rows have missing or invalid reply_type")


# ---------------------------------------------------------------------------
# Data integrity — no filtered tweets should exist in the saved files
# ---------------------------------------------------------------------------

JSON_PATH = os.path.join(os.path.dirname(__file__), "jackie_fielder_tweets.json")
CSV_PATH  = os.path.join(os.path.dirname(__file__), "jackie_fielder_tweets.csv")


class TestDataIntegrity(unittest.TestCase):

    def _load_json_texts(self):
        with open(JSON_PATH) as f:
            return [t["text"] for t in json.load(f)["tweets"]]

    def _load_csv_texts(self):
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            return [row["text"] for row in csv.DictReader(f)]

    def test_json_contains_no_retweets(self):
        rts = [t for t in self._load_json_texts() if t.startswith("RT @")]
        self.assertEqual(rts, [], f"{len(rts)} RT tweets found in JSON")

    def test_csv_contains_no_retweets(self):
        rts = [t for t in self._load_csv_texts() if t.startswith("RT @")]
        self.assertEqual(rts, [], f"{len(rts)} RT tweets found in CSV")

    def test_json_contains_no_type2_replies(self):
        type2 = [t for t in self._load_json_texts() if dt.is_reply_to_other(t)]
        self.assertEqual(type2, [], f"{len(type2)} type-2 tweets found in JSON")

    def test_csv_contains_no_type2_replies(self):
        type2 = [t for t in self._load_csv_texts() if dt.is_reply_to_other(t)]
        self.assertEqual(type2, [], f"{len(type2)} type-2 tweets found in CSV")

    def test_json_contains_no_duplicate_ids(self):
        with open(JSON_PATH) as f:
            ids = [t["id"] for t in json.load(f)["tweets"]]
        dupes = [i for i in set(ids) if ids.count(i) > 1]
        self.assertEqual(dupes, [], f"{len(dupes)} duplicate IDs found in JSON")

    def test_csv_contains_no_duplicate_ids(self):
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            ids = [row["id"] for row in csv.DictReader(f)]
        dupes = [i for i in set(ids) if ids.count(i) > 1]
        self.assertEqual(dupes, [], f"{len(dupes)} duplicate IDs found in CSV")

    def test_json_and_csv_have_same_count(self):
        with open(JSON_PATH) as f:
            json_count = len(json.load(f)["tweets"])
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            csv_count = sum(1 for _ in csv.DictReader(f))
        self.assertEqual(json_count, csv_count,
                         f"JSON has {json_count} tweets but CSV has {csv_count} rows")


if __name__ == "__main__":
    unittest.main()
