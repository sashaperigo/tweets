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

# Pre-import vader so it's in sys.modules before builtins.open is mocked.
# (mock.patch resolves module paths by importing them; if nltk is not yet imported,
# that import chain hits dateutil which reads binary timezone files — and fails if
# open is already mocked to return strings.)
import nltk.sentiment.vader  # noqa: E402

# Patch load_bearer_token and SentimentIntensityAnalyzer before importing the module.
# The open mock injects a fake bearer token; the SIA mock prevents lexicon file reads
# (the real _vader is replaced per-test via _mock_vader).
with mock.patch("builtins.open", mock.mock_open(read_data="BEARER_TOKEN = dummy\n")), \
     mock.patch("nltk.sentiment.vader.SentimentIntensityAnalyzer"):
    import download_tweets as dt


def _make_test_politician(tmpdir):
    """Create a Politician pointing at temp files for use in tests."""
    return dt.Politician(
        name="Test Person",
        handle="TestHandle",
        json_path=os.path.join(tmpdir, "tweets.json"),
        csv_path=os.path.join(tmpdir, "tweets.csv"),
        staging_path=os.path.join(tmpdir, "tweets_staging.jsonl"),
    )


# ---------------------------------------------------------------------------
# is_reply_to_other
# ---------------------------------------------------------------------------

class TestIsReplyToOther(unittest.TestCase):

    def test_type1_direct_reply_to_jackie(self):
        # Starts with @JackieFielder_ → NOT a type-2, should return False
        self.assertFalse(dt.is_reply_to_other("@JackieFielder_ thanks for sharing", dt.JACKIE))

    def test_type1_case_insensitive(self):
        self.assertFalse(dt.is_reply_to_other("@JACKIEFIELDER_ great post", dt.JACKIE))
        self.assertFalse(dt.is_reply_to_other("@jackiefielder_ ok", dt.JACKIE))

    def test_type2_other_person_first(self):
        # Starts with @someoneelse → type-2, should return True
        self.assertTrue(dt.is_reply_to_other("@someoneelse @JackieFielder_ check this out", dt.JACKIE))

    def test_type2_multiple_mentions_before_jackie(self):
        self.assertTrue(dt.is_reply_to_other("@alice @bob @JackieFielder_ agreed", dt.JACKIE))

    def test_type3_no_leading_mention(self):
        # Doesn't start with @ → not a reply at all, should return False
        self.assertFalse(dt.is_reply_to_other("Jackie Fielder just announced something big", dt.JACKIE))
        self.assertFalse(dt.is_reply_to_other("Did you see what @JackieFielder_ said?", dt.JACKIE))

    def test_retweet_style(self):
        # RT @ doesn't start with @ so is not a reply
        self.assertFalse(dt.is_reply_to_other("RT @JackieFielder_ this is a retweet", dt.JACKIE))

    def test_rt_filter(self):
        # The RT check used in download loops
        self.assertTrue("RT @someone great tweet".startswith("RT @"))
        self.assertFalse("@JackieFielder_ nice".startswith("RT @"))
        self.assertFalse("Jackie Fielder said something".startswith("RT @"))

    def test_type1_handle_with_extra_characters(self):
        # @JackieFielder_something is a different handle — should be treated as type-2
        self.assertTrue(dt.is_reply_to_other("@JackieFielder_something hey", dt.JACKIE))

    def test_empty_string(self):
        self.assertFalse(dt.is_reply_to_other("", dt.JACKIE))


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
        self.politician = _make_test_politician(self.tmpdir)

    def tearDown(self):
        for f in os.listdir(self.tmpdir):
            os.unlink(os.path.join(self.tmpdir, f))
        os.rmdir(self.tmpdir)

    def _run_backfill(self, pages):
        with mock.patch.object(dt, "fetch_page", side_effect=_fake_fetch_page(pages)), \
             mock.patch.object(dt, "save_json"):
            return dt.backfill_tweets(
                self.politician,
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
             mock.patch.object(dt, "save_json"):
            dt.backfill_tweets(self.politician,
                               end_time="2025-11-07T00:00:00Z",
                               start_time="2025-10-01T00:00:00Z")
        # Should have stopped after page 2, not fetched a third page
        self.assertEqual(call_count[0], 2)


# ---------------------------------------------------------------------------
# Progress saved on failure (try/finally in __main__)
# ---------------------------------------------------------------------------

class TestProgressOnFailure(unittest.TestCase):
    """Verify that tweets staged before a crash are saved to both JSON and CSV."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.politician = _make_test_politician(self.tmpdir)
        dt._vader.polarity_scores.return_value = {
            "compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 1.0
        }

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir)

    def test_staged_tweets_saved_to_json_and_csv_on_error(self):
        # First fetch returns a page of tweets; second raises to simulate a crash.
        page1 = {"data": [
            _make_api_tweet("1", "2025-11-06T12:00:00Z"),
        ], "includes": {"users": [{"id": "1", "username": "u", "name": "U"}]},
           "meta": {"result_count": 1, "next_token": "tok"}}

        call_count = [0]
        def failing_fetch(*args, **kwargs):
            if call_count[0] == 0:
                call_count[0] += 1
                return page1
            raise RuntimeError("simulated API failure")

        # Replicate the try/finally from __main__
        try:
            with mock.patch.object(dt, "fetch_page", side_effect=failing_fetch):
                dt.download_all_tweets(self.politician)
        except RuntimeError:
            pass
        finally:
            new_count = dt.finalize_json(self.politician)
            if new_count > 0:
                dt.finalize_csv(self.politician)

        # JSON should contain the tweet from the first page
        with open(self.politician.json_path) as f:
            tweets = json.load(f)["tweets"]
        self.assertEqual(len(tweets), 1)
        self.assertEqual(tweets[0]["id"], "1")

        # CSV should also contain it
        with open(self.politician.csv_path, newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "1")


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
        self.politician = _make_test_politician(self.tmpdir)

    def tearDown(self):
        for f in os.listdir(self.tmpdir):
            os.unlink(os.path.join(self.tmpdir, f))
        os.rmdir(self.tmpdir)

    def test_save_json_appends_to_staging(self):
        dt.save_json(SAMPLE_TWEETS, self.politician)
        self.assertTrue(os.path.exists(self.politician.staging_path))
        with open(self.politician.staging_path) as f:
            lines = [l for l in f if l.strip()]
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])["id"], "100")
        self.assertEqual(json.loads(lines[1])["id"], "200")

    def test_finalize_merges_staging_into_json(self):
        dt.save_json(SAMPLE_TWEETS, self.politician)
        dt.finalize_json(self.politician)
        with open(self.politician.json_path) as f:
            data = json.load(f)
        self.assertEqual(len(data["tweets"]), 2)
        ids = {t["id"] for t in data["tweets"]}
        self.assertEqual(ids, {"100", "200"})

    def test_finalize_removes_staging_file(self):
        dt.save_json(SAMPLE_TWEETS, self.politician)
        dt.finalize_json(self.politician)
        self.assertFalse(os.path.exists(self.politician.staging_path))

    def test_finalize_deduplicates(self):
        # Save the same tweets twice (simulates duplicate pages)
        dt.save_json(SAMPLE_TWEETS, self.politician)
        dt.save_json(SAMPLE_TWEETS, self.politician)
        dt.finalize_json(self.politician)
        with open(self.politician.json_path) as f:
            data = json.load(f)
        self.assertEqual(len(data["tweets"]), 2)

    def test_finalize_merges_with_existing_json(self):
        # Pre-populate the JSON with tweet 100
        with open(self.politician.json_path, "w") as f:
            json.dump({"tweets": [{"id": "100", "created_at": "2025-11-01T00:00:00.000Z",
                                   "author_id": "99", "text": "tweet one",
                                   "edit_history_tweet_ids": ["100"],
                                   "public_metrics": {}}]}, f)
        # Stage tweet 200 (new) and tweet 100 (duplicate)
        dt.save_json(SAMPLE_TWEETS, self.politician)
        dt.finalize_json(self.politician)
        with open(self.politician.json_path) as f:
            data = json.load(f)
        self.assertEqual(len(data["tweets"]), 2)

    def test_finalize_returns_new_count(self):
        dt.save_json(SAMPLE_TWEETS, self.politician)
        new_count = dt.finalize_json(self.politician)
        self.assertEqual(new_count, 2)

    def test_finalize_returns_zero_if_no_staging_file(self):
        # Staging file never created (0 tweets found) — should return 0, not raise
        result = dt.finalize_json(self.politician)
        self.assertEqual(result, 0)

    def test_finalize_empty_staging_returns_zero_without_error(self):
        # Staging file exists but is empty — no tweets found this run
        open(self.politician.staging_path, "w").close()
        result = dt.finalize_json(self.politician)
        self.assertEqual(result, 0)
        self.assertFalse(os.path.exists(self.politician.staging_path))

    def test_finalize_returns_zero_for_all_duplicates(self):
        # Put both tweets in main JSON already
        records = [{"id": t["id"], "created_at": t["created_at"], "author_id": t["author_id"],
                    "text": t["text"], "edit_history_tweet_ids": t["edit_history_tweet_ids"],
                    "public_metrics": t["public_metrics"]} for t in SAMPLE_TWEETS]
        with open(self.politician.json_path, "w") as f:
            json.dump({"tweets": records}, f)
        dt.save_json(SAMPLE_TWEETS, self.politician)
        new_count = dt.finalize_json(self.politician)
        self.assertEqual(new_count, 0)

    def test_save_json_persists_username_and_name(self):
        dt.save_json(SAMPLE_TWEETS, self.politician)
        with open(self.politician.staging_path) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        self.assertEqual(lines[0]["username"], "user1")
        self.assertEqual(lines[0]["name"], "User 1")
        self.assertEqual(lines[1]["username"], "user2")
        self.assertEqual(lines[1]["name"], "User 2")

    def test_finalize_json_preserves_username_and_name(self):
        dt.save_json(SAMPLE_TWEETS, self.politician)
        dt.finalize_json(self.politician)
        with open(self.politician.json_path) as f:
            tweets = {t["id"]: t for t in json.load(f)["tweets"]}
        self.assertEqual(tweets["100"]["username"], "user1")
        self.assertEqual(tweets["100"]["name"], "User 1")
        self.assertEqual(tweets["200"]["username"], "user2")
        self.assertEqual(tweets["200"]["name"], "User 2")


# ---------------------------------------------------------------------------
# save_csv
# ---------------------------------------------------------------------------

class TestSaveCsv(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        self.tmp.close()
        os.unlink(self.tmp.name)
        dt._vader.polarity_scores.return_value = {
            "compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 1.0
        }

    def tearDown(self):
        if os.path.exists(self.tmp.name):
            os.unlink(self.tmp.name)

    def test_creates_file_with_header(self):
        dt.save_csv(SAMPLE_TWEETS, dt.JACKIE, path=self.tmp.name)
        with open(self.tmp.name, newline="") as f:
            rows = list(csv.reader(f))
        self.assertEqual(rows[0], ["id", "created_at", "username", "name", "text",
                                   "likes", "retweets", "replies", "quotes", "impressions",
                                   "reply_type", "sentiment", "sentiment_score", "is_excluded"])
        self.assertEqual(len(rows), 3)  # header + 2 tweets

    def test_correct_field_values(self):
        dt.save_csv(SAMPLE_TWEETS[:1], dt.JACKIE, path=self.tmp.name)
        with open(self.tmp.name, newline="") as f:
            reader = csv.DictReader(f)
            row = next(reader)
        self.assertEqual(row["id"], "100")
        self.assertEqual(row["username"], "user1")
        self.assertEqual(row["likes"], "1")
        self.assertEqual(row["impressions"], "10")


# ---------------------------------------------------------------------------
# finalize_csv
# ---------------------------------------------------------------------------

class TestFinalizeCsv(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.politician = _make_test_politician(self.tmp_dir)
        dt._vader.polarity_scores.return_value = {
            "compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 1.0
        }

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir)

    def _write_json(self, tweets):
        with open(self.politician.json_path, "w") as f:
            json.dump({"tweets": tweets}, f)

    def test_csv_row_count_matches_json(self):
        self._write_json(SAMPLE_TWEETS)
        dt.finalize_csv(self.politician)
        with open(self.politician.csv_path, newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), len(SAMPLE_TWEETS))

    def test_csv_contains_correct_ids(self):
        self._write_json(SAMPLE_TWEETS)
        dt.finalize_csv(self.politician)
        with open(self.politician.csv_path, newline="") as f:
            ids = [r["id"] for r in csv.DictReader(f)]
        self.assertEqual(ids, ["100", "200"])

    def test_csv_has_all_columns(self):
        self._write_json(SAMPLE_TWEETS)
        dt.finalize_csv(self.politician)
        with open(self.politician.csv_path, newline="") as f:
            fieldnames = csv.DictReader(f).fieldnames
        expected = ["id", "created_at", "username", "name", "text",
                    "likes", "retweets", "replies", "quotes", "impressions",
                    "reply_type", "sentiment", "sentiment_score", "is_excluded"]
        self.assertEqual(fieldnames, expected)

    def test_overwrites_existing_csv(self):
        # Write a stale CSV with wrong data, then finalize should replace it
        with open(self.politician.csv_path, "w") as f:
            f.write("stale,data\n1,2\n")
        self._write_json(SAMPLE_TWEETS)
        dt.finalize_csv(self.politician)
        with open(self.politician.csv_path, newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), len(SAMPLE_TWEETS))
        self.assertEqual(rows[0]["id"], "100")

    def test_csv_username_and_name_come_from_json(self):
        self._write_json(SAMPLE_TWEETS)
        dt.finalize_csv(self.politician)
        with open(self.politician.csv_path, newline="") as f:
            rows = {r["id"]: r for r in csv.DictReader(f)}
        self.assertEqual(rows["100"]["username"], "user1")
        self.assertEqual(rows["100"]["name"], "User 1")
        self.assertEqual(rows["200"]["username"], "user2")
        self.assertEqual(rows["200"]["name"], "User 2")


# ---------------------------------------------------------------------------
# get_sentiment
# ---------------------------------------------------------------------------

class TestGetSentiment(unittest.TestCase):

    def _mock_vader(self, compound):
        """Return a context manager that mocks polarity_scores to return compound."""
        return mock.patch.object(
            dt._vader, "polarity_scores",
            return_value={"compound": compound, "pos": 0, "neg": 0, "neu": 0}
        )

    def test_positive_label_for_high_compound(self):
        with self._mock_vader(0.5):
            label, score = dt.get_sentiment("great job!")
        self.assertEqual(label, "positive")
        self.assertAlmostEqual(score, 0.5)

    def test_negative_label_for_low_compound(self):
        with self._mock_vader(-0.6):
            label, score = dt.get_sentiment("terrible awful")
        self.assertEqual(label, "negative")
        self.assertAlmostEqual(score, -0.6)

    def test_neutral_label_for_near_zero_compound(self):
        with self._mock_vader(0.0):
            label, score = dt.get_sentiment("today is tuesday")
        self.assertEqual(label, "neutral")
        self.assertAlmostEqual(score, 0.0)

    def test_boundary_positive_at_0_05(self):
        with self._mock_vader(0.05):
            label, _ = dt.get_sentiment("ok")
        self.assertEqual(label, "positive")

    def test_boundary_negative_at_minus_0_05(self):
        with self._mock_vader(-0.05):
            label, _ = dt.get_sentiment("ok")
        self.assertEqual(label, "negative")

    def test_just_below_positive_boundary_is_neutral(self):
        with self._mock_vader(0.04):
            label, _ = dt.get_sentiment("ok")
        self.assertEqual(label, "neutral")

    def test_just_above_negative_boundary_is_neutral(self):
        with self._mock_vader(-0.04):
            label, _ = dt.get_sentiment("ok")
        self.assertEqual(label, "neutral")



# ---------------------------------------------------------------------------
# get_reply_type
# ---------------------------------------------------------------------------

class TestGetReplyType(unittest.TestCase):

    def test_direct_reply_starts_with_jackiefielder(self):
        self.assertEqual(dt.get_reply_type("@JackieFielder_ great point", dt.JACKIE), "Direct reply")

    def test_direct_reply_case_insensitive(self):
        self.assertEqual(dt.get_reply_type("@JACKIEFIELDER_ ok", dt.JACKIE), "Direct reply")
        self.assertEqual(dt.get_reply_type("@jackiefielder_ ok", dt.JACKIE), "Direct reply")

    def test_mention_contains_handle_mid_tweet(self):
        self.assertEqual(
            dt.get_reply_type("Did you see what @JackieFielder_ said?", dt.JACKIE), "Mention")

    def test_mention_name_only_no_handle(self):
        # Plain name with no @ is "Not tagged"
        self.assertEqual(dt.get_reply_type("Jackie Fielder made a statement today", dt.JACKIE), "Not tagged")

    def test_not_tagged_no_reference(self):
        self.assertEqual(dt.get_reply_type("San Francisco politics are wild", dt.JACKIE), "Not tagged")

    def test_mention_handle_after_other_text(self):
        self.assertEqual(
            dt.get_reply_type("Someone should ask @JackieFielder_ about this", dt.JACKIE), "Mention")

    def test_thread_mention_other_person_first(self):
        # Starts with another @handle — Jackie is tagged in the thread but not the addressee
        self.assertEqual(
            dt.get_reply_type("@someoneelse @JackieFielder_ check this out", dt.JACKIE), "Thread mention")

    def test_thread_mention_multiple_handles_before_jackie(self):
        self.assertEqual(
            dt.get_reply_type("@alice @bob @JackieFielder_ thoughts?", dt.JACKIE), "Thread mention")

    def test_thread_mention_case_insensitive(self):
        self.assertEqual(
            dt.get_reply_type("@someoneelse @JACKIEFIELDER_ see above", dt.JACKIE), "Thread mention")



# ---------------------------------------------------------------------------
# get_is_excluded
# ---------------------------------------------------------------------------

class TestGetIsExcluded(unittest.TestCase):

    def test_excluded_account_returns_true(self):
        for username in ["sfpdcallsbot", "sfchronicle", "kqednews",
                         "mlnow", "sfist", "sfstandard", "48hills", "grok"]:
            with self.subTest(username=username):
                self.assertTrue(dt.get_is_excluded(username))

    def test_excluded_account_case_insensitive(self):
        self.assertTrue(dt.get_is_excluded("SFChronicle"))
        self.assertTrue(dt.get_is_excluded("KQEDNEWS"))

    def test_regular_account_returns_false(self):
        self.assertFalse(dt.get_is_excluded("somerandomperson"))
        self.assertFalse(dt.get_is_excluded("JackieFielder_"))

    def test_empty_string_returns_false(self):
        self.assertFalse(dt.get_is_excluded(""))



# ---------------------------------------------------------------------------
# Data integrity — no filtered tweets should exist in the saved files
# ---------------------------------------------------------------------------

def _make_data_integrity_tests(politician):
    """Generate a data validation test class for the given politician's files."""

    class TestDataIntegrity(unittest.TestCase):

        def _tweets(self):
            with open(politician.json_path) as f:
                return json.load(f)["tweets"]

        def _csv_rows(self):
            with open(politician.csv_path, newline="", encoding="utf-8") as f:
                return list(csv.DictReader(f))

        # ------------------------------------------------------------------
        # JSON — structure and field-level checks
        # ------------------------------------------------------------------

        def test_json_tweets_have_required_fields(self):
            required = {"id", "created_at", "author_id", "text",
                        "public_metrics", "edit_history_tweet_ids"}
            tweets = self._tweets()
            missing = [t.get("id", "?") for t in tweets if not required.issubset(t.keys())]
            self.assertEqual(missing, [],
                f"{len(missing)} tweets are missing one or more required fields")

        def test_json_contains_no_retweets(self):
            rts = [t["text"] for t in self._tweets() if t["text"].startswith("RT @")]
            self.assertEqual(rts, [], f"{len(rts)} RT tweets found in JSON")

        def test_json_contains_no_duplicate_ids(self):
            ids = [t["id"] for t in self._tweets()]
            dupes = [i for i in set(ids) if ids.count(i) > 1]
            self.assertEqual(dupes, [], f"{len(dupes)} duplicate IDs found in JSON")

        def test_json_id_is_numeric_string(self):
            invalid = [t.get("id") for t in self._tweets()
                       if not str(t.get("id", "")).isdigit()]
            self.assertEqual(invalid, [],
                f"{len(invalid)} tweets have a non-numeric id")

        def test_json_author_id_is_numeric_string(self):
            invalid = [t.get("id") for t in self._tweets()
                       if not str(t.get("author_id", "")).isdigit()]
            self.assertEqual(invalid, [],
                f"{len(invalid)} tweets have a non-numeric author_id")

        def test_json_created_at_is_valid_datetime(self):
            from datetime import datetime
            invalid = []
            for t in self._tweets():
                try:
                    datetime.fromisoformat(t.get("created_at", "").replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    invalid.append(t.get("id"))
            self.assertEqual(invalid, [],
                f"{len(invalid)} tweets have an unparseable created_at")

        def test_json_edit_history_is_list_of_strings(self):
            invalid = []
            for t in self._tweets():
                eh = t.get("edit_history_tweet_ids")
                if not isinstance(eh, list) or len(eh) == 0:
                    invalid.append(t.get("id"))
                elif not all(isinstance(x, str) and x.isdigit() for x in eh):
                    invalid.append(t.get("id"))
            self.assertEqual(invalid, [],
                f"{len(invalid)} tweets have an invalid edit_history_tweet_ids")

        def test_json_text_is_non_empty_string(self):
            invalid = [t.get("id") for t in self._tweets()
                       if not isinstance(t.get("text"), str) or not t["text"].strip()]
            self.assertEqual(invalid, [],
                f"{len(invalid)} tweets have empty or missing text")

        def test_json_public_metrics_are_valid(self):
            required_metrics = {
                "like_count", "retweet_count", "reply_count",
                "quote_count", "impression_count",
            }
            invalid = []
            for t in self._tweets():
                m = t.get("public_metrics")
                if not isinstance(m, dict):
                    invalid.append(t.get("id"))
                    continue
                if not required_metrics.issubset(m.keys()):
                    invalid.append(t.get("id"))
                    continue
                if any(not isinstance(m[k], int) or m[k] < 0 for k in required_metrics):
                    invalid.append(t.get("id"))
            self.assertEqual(invalid, [],
                f"{len(invalid)} tweets have missing or negative public_metrics")

        # ------------------------------------------------------------------
        # CSV — column presence, types, and value ranges
        # ------------------------------------------------------------------

        def test_json_and_csv_have_same_count(self):
            json_count = len(self._tweets())
            csv_count = len(self._csv_rows())
            self.assertEqual(json_count, csv_count,
                f"JSON has {json_count} tweets but CSV has {csv_count} rows")

        def test_csv_has_all_expected_columns(self):
            expected = ["id", "created_at", "username", "name", "text",
                        "likes", "retweets", "replies", "quotes", "impressions",
                        "reply_type", "sentiment", "sentiment_score", "is_excluded"]
            rows = self._csv_rows()
            self.assertEqual(list(rows[0].keys()), expected,
                f"CSV columns don't match expected schema")

        def test_csv_contains_no_retweets(self):
            rts = [r["text"] for r in self._csv_rows() if r["text"].startswith("RT @")]
            self.assertEqual(rts, [], f"{len(rts)} RT tweets found in CSV")

        def test_csv_contains_no_duplicate_ids(self):
            ids = [r["id"] for r in self._csv_rows()]
            dupes = [i for i in set(ids) if ids.count(i) > 1]
            self.assertEqual(dupes, [], f"{len(dupes)} duplicate IDs found in CSV")

        def test_csv_id_is_numeric(self):
            invalid = [r["id"] for r in self._csv_rows() if not r.get("id", "").isdigit()]
            self.assertEqual(invalid, [], f"{len(invalid)} rows have a non-numeric id")

        def test_csv_created_at_is_valid_datetime(self):
            from datetime import datetime
            invalid = []
            for r in self._csv_rows():
                try:
                    datetime.fromisoformat(r.get("created_at", "").replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    invalid.append(r.get("id"))
            self.assertEqual(invalid, [],
                f"{len(invalid)} rows have an unparseable created_at")

        def test_csv_text_is_non_empty(self):
            invalid = [r["id"] for r in self._csv_rows() if not r.get("text", "").strip()]
            self.assertEqual(invalid, [], f"{len(invalid)} rows have empty text")

        def test_csv_username_and_name_are_strings(self):
            # username and name may be empty for historical data, but must be present
            # and must be strings (not None or a non-string type)
            rows = self._csv_rows()
            self.assertIn("username", rows[0].keys(), "CSV is missing 'username' column")
            self.assertIn("name", rows[0].keys(), "CSV is missing 'name' column")
            invalid = [r["id"] for r in rows
                       if not isinstance(r.get("username"), str)
                       or not isinstance(r.get("name"), str)]
            self.assertEqual(invalid, [],
                f"{len(invalid)} rows have non-string username or name")

        def test_csv_numeric_metrics_are_non_negative(self):
            metric_cols = ["likes", "retweets", "replies", "quotes", "impressions"]
            invalid = []
            for r in self._csv_rows():
                for col in metric_cols:
                    try:
                        if int(r.get(col, 0)) < 0:
                            invalid.append((r["id"], col))
                    except (ValueError, TypeError):
                        invalid.append((r["id"], col))
            self.assertEqual(invalid, [],
                f"{len(invalid)} (row, col) pairs have invalid metric values")

        def test_csv_reply_type_is_valid(self):
            valid = {"Direct reply", "Mention", "Not tagged", "Thread mention"}
            rows = self._csv_rows()
            self.assertIn("reply_type", rows[0].keys(), "CSV is missing 'reply_type' column")
            invalid = [r["id"] for r in rows if r.get("reply_type") not in valid]
            self.assertEqual(invalid, [],
                f"{len(invalid)} rows have missing or invalid reply_type")

        def test_csv_sentiment_label_is_valid(self):
            valid_labels = {"positive", "negative", "neutral"}
            rows = self._csv_rows()
            self.assertIn("sentiment", rows[0].keys(), "CSV is missing 'sentiment' column")
            invalid = [r["id"] for r in rows if r.get("sentiment") not in valid_labels]
            self.assertEqual(invalid, [],
                f"{len(invalid)} rows have an invalid sentiment label")

        def test_csv_sentiment_score_is_in_range(self):
            rows = self._csv_rows()
            self.assertIn("sentiment_score", rows[0].keys(),
                "CSV is missing 'sentiment_score' column")
            invalid = []
            for r in rows:
                try:
                    score = float(r.get("sentiment_score", ""))
                    if not (-1.0 <= score <= 1.0):
                        invalid.append(r["id"])
                except (ValueError, TypeError):
                    invalid.append(r["id"])
            self.assertEqual(invalid, [],
                f"{len(invalid)} rows have an out-of-range or unparseable sentiment_score")

        def test_csv_is_excluded_is_valid(self):
            rows = self._csv_rows()
            self.assertIn("is_excluded", rows[0].keys(), "CSV is missing 'is_excluded' column")
            invalid = [r["id"] for r in rows if r.get("is_excluded") not in {"True", "False"}]
            self.assertEqual(invalid, [],
                f"{len(invalid)} rows have an invalid is_excluded value")

    TestDataIntegrity.__name__ = f"TestDataIntegrity_{politician.name.replace(' ', '')}"
    return TestDataIntegrity


# Run data validation checks for every politician that has data on disk.
for _politician in dt.POLITICIANS.values():
    if os.path.exists(_politician.json_path) and os.path.exists(_politician.csv_path):
        _cls = _make_data_integrity_tests(_politician)
        globals()[_cls.__name__] = _cls
del _cls, _politician


if __name__ == "__main__":
    unittest.main()
