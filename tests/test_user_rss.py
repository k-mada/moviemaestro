"""Unit tests for the RSS parser and refresh flow (app/pipeline/user_rss.py).

The endpoint HTTP surface is covered in tests/test_refresh_user_endpoint.py.
Network is never hit: fetch is stubbed with a canned feed.
"""

from __future__ import annotations

import pytest

from app.pipeline import user_rss
from tests.fakes import FakeSupabase

# Canned feed mirroring the shapes verified against a live pull:
#   - a rated watch item
#   - a rewatch (link ends in /N/ — slug must drop it)
#   - an unrated watch item (no <letterboxd:memberRating> tag)
#   - an unliked watch item (memberLike = No)
#   - a list item (letterboxd-list- guid — must be filtered out)
FEED = """<?xml version='1.0' encoding='utf-8'?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/"
     xmlns:letterboxd="https://letterboxd.com"
     xmlns:tmdb="https://themoviedb.org">
  <channel>
    <title>Letterboxd - Tester</title>
    <item>
      <title>Obsession, 2025 - ★★★½</title>
      <link>https://letterboxd.com/tester/film/obsession-2025/</link>
      <guid isPermaLink="false">letterboxd-watch-1</guid>
      <letterboxd:rewatch>No</letterboxd:rewatch>
      <letterboxd:filmTitle>Obsession</letterboxd:filmTitle>
      <letterboxd:filmYear>2025</letterboxd:filmYear>
      <letterboxd:memberRating>3.5</letterboxd:memberRating>
      <letterboxd:memberLike>Yes</letterboxd:memberLike>
      <tmdb:movieId>1339713</tmdb:movieId>
    </item>
    <item>
      <title>Toy Story 4, 2019 - ★★★½</title>
      <link>https://letterboxd.com/tester/film/toy-story-4/1/</link>
      <guid isPermaLink="false">letterboxd-watch-2</guid>
      <letterboxd:rewatch>Yes</letterboxd:rewatch>
      <letterboxd:filmTitle>Toy Story 4</letterboxd:filmTitle>
      <letterboxd:memberRating>3.5</letterboxd:memberRating>
      <letterboxd:memberLike>Yes</letterboxd:memberLike>
    </item>
    <item>
      <title>The Backrooms, 2022</title>
      <link>https://letterboxd.com/tester/film/the-backrooms/</link>
      <guid isPermaLink="false">letterboxd-watch-3</guid>
      <letterboxd:rewatch>No</letterboxd:rewatch>
      <letterboxd:filmTitle>The Backrooms</letterboxd:filmTitle>
      <letterboxd:memberLike>No</letterboxd:memberLike>
    </item>
    <item>
      <title>Oscars 2027 priority watchlist</title>
      <link>https://letterboxd.com/tester/list/oscars-2027-priority-watchlist/</link>
      <guid isPermaLink="false">letterboxd-list-81731773</guid>
    </item>
  </channel>
</rss>
"""


class TestParse:
    def test_filters_out_non_watch_items(self):
        films = user_rss._parse_feed(FEED, "tester")
        slugs = {f.film_slug for f in films}
        assert "oscars-2027-priority-watchlist" not in slugs
        assert len(films) == 3

    def test_rated_item_fields(self):
        films = {f.film_slug: f for f in user_rss._parse_feed(FEED, "tester")}
        f = films["obsession-2025"]
        assert f.title == "Obsession"
        assert f.rating == 3.5
        assert f.liked is True

    def test_rewatch_link_strips_trailing_number(self):
        films = {f.film_slug: f for f in user_rss._parse_feed(FEED, "tester")}
        assert "toy-story-4" in films  # not "toy-story-4/1" or "1"

    def test_unrated_item_has_none_rating(self):
        films = {f.film_slug: f for f in user_rss._parse_feed(FEED, "tester")}
        f = films["the-backrooms"]
        assert f.rating is None
        assert f.liked is False

    def test_empty_feed_returns_no_films(self):
        empty = "<rss version='2.0'><channel><title>x</title></channel></rss>"
        assert user_rss._parse_feed(empty, "tester") == []

    def test_malformed_rating_tolerated_as_none(self):
        feed = FEED.replace(
            "<letterboxd:memberRating>3.5</letterboxd:memberRating>",
            "<letterboxd:memberRating>garbage</letterboxd:memberRating>",
            1,
        )
        films = {f.film_slug: f for f in user_rss._parse_feed(feed, "tester")}
        assert films["obsession-2025"].rating is None


class TestParseRejectsNonFeed:
    def test_html_interstitial_with_200_raises(self):
        # A well-formed HTML body (parses fine, has no <item>) must NOT be
        # mistaken for an empty feed — it means we were blocked.
        html = (
            "<html><head><title>Just a moment...</title></head>"
            "<body>checking your browser</body></html>"
        )
        with pytest.raises(user_rss.RssFetchError):
            user_rss._parse_feed(html, "tester")

    def test_unparseable_body_raises(self):
        with pytest.raises(user_rss.RssFetchError):
            user_rss._parse_feed("<html><br>not xml", "tester")


class TestSlugFromLink:
    @pytest.mark.parametrize(
        "link,expected",
        [
            ("https://letterboxd.com/u/film/some-slug/", "some-slug"),
            ("https://letterboxd.com/u/film/some-slug/3/", "some-slug"),
            ("/u/film/bare/", "bare"),
        ],
    )
    def test_extracts_slug(self, link, expected):
        assert user_rss._slug_from_link(link) == expected

    def test_no_film_segment_returns_none(self):
        assert user_rss._slug_from_link("https://letterboxd.com/u/list/x/") is None
        assert user_rss._slug_from_link("") is None


class TestRefreshUpsert:
    async def test_upserts_watch_rows_into_userfilms(self, monkeypatch):
        sb = FakeSupabase()
        monkeypatch.setattr(user_rss, "_fetch_rss", lambda user: FEED)

        result = await user_rss.refresh_user_from_rss(sb, "tester")

        assert result.watch_items == 3
        assert result.upserted == 3
        rows = {r["film_slug"]: r for r in sb.tables["UserFilms"]}
        assert set(rows) == {"obsession-2025", "toy-story-4", "the-backrooms"}
        assert rows["the-backrooms"]["rating"] is None
        assert rows["obsession-2025"]["rating"] == 3.5
        # Correct conflict target used.
        upsert_writes = [w for w in sb.writes if w["op"] == "upsert"]
        assert upsert_writes[-1]["on_conflict"] == "lbusername,film_slug"

    async def test_does_not_touch_userratings(self, monkeypatch):
        sb = FakeSupabase()
        monkeypatch.setattr(user_rss, "_fetch_rss", lambda user: FEED)
        await user_rss.refresh_user_from_rss(sb, "tester")
        assert sb.tables.get("UserRatings", []) == []

    async def test_last_write_wins_overwrites_existing_rating_with_null(
        self, monkeypatch
    ):
        sb = FakeSupabase()
        # A prior grid backfill rated this film; RSS now sees it unrated.
        sb.tables["UserFilms"].append(
            {
                "lbusername": "tester",
                "film_slug": "the-backrooms",
                "rating": 4.0,
                "liked": True,
                "title": "The Backrooms",
            }
        )
        monkeypatch.setattr(user_rss, "_fetch_rss", lambda user: FEED)

        await user_rss.refresh_user_from_rss(sb, "tester")

        row = next(
            r for r in sb.tables["UserFilms"] if r["film_slug"] == "the-backrooms"
        )
        assert row["rating"] is None

    async def test_empty_feed_upserts_nothing(self, monkeypatch):
        sb = FakeSupabase()
        empty = "<rss version='2.0'><channel><title>x</title></channel></rss>"
        monkeypatch.setattr(user_rss, "_fetch_rss", lambda user: empty)

        result = await user_rss.refresh_user_from_rss(sb, "tester")

        assert result.upserted == 0
        assert sb.tables["UserFilms"] == []
