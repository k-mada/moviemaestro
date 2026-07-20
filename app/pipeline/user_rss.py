"""Domain B: incremental user refresh from Letterboxd's per-user RSS feed.

The HTML grid (/{user}/films/) is hard-blocked on Railway's datacenter egress;
the RSS feed (/{user}/rss/) is not. This pulls the feed, keeps only diary
'watch' items, and upserts them into UserFilms — the cheap freshness path that
Railway can serve. See LETTERBOXD_DATA_FLOW.md, Domain B.

The feed also carries list/review items and is not capped at 50 diary events
(a live pull returned 50 watch + 50 list items), so filtering <guid> to the
watch prefix is load-bearing, not cosmetic.

Deliberately writes the SAME column set as the grid path (user_films.py):
lbusername, film_slug, rating, liked, title. Both writers are upsert-only with
no deletes, so a ~50-row RSS refresh can never shrink a full grid backfill, and
last-write-wins is safe regardless of run order. If the two paths ever diverge
on columns, migrate UserFilms to a per-column COALESCE-on-conflict upsert (see
Films.upsert_film) so the source lacking a column can't null it out.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from curl_cffi import requests
from supabase import Client

from app.pipeline import letterboxd_throttle

RSS_URL = "https://letterboxd.com/{}/rss/"
WATCH_GUID_PREFIX = "letterboxd-watch-"

# Same TLS impersonation letterboxdpy uses for the HTML routes.
_IMPERSONATE = "chrome"

# RSS namespace prefixes. ElementTree resolves letterboxd:memberRating etc. via
# the {uri}local form, so we pass this map to findtext().
_NS = {
    "letterboxd": "https://letterboxd.com",
    "tmdb": "https://themoviedb.org",
}


class RssFetchError(RuntimeError):
    """RSS feed could not be fetched (private/nonexistent user, block, non-200).

    Carries the HTTP status when there was a response so the endpoint can map
    it to a client-facing status. status is None on a transport-level failure.
    """

    def __init__(self, lbusername: str, status: int | None, detail: str) -> None:
        self.lbusername = lbusername
        self.status = status
        super().__init__(detail)


@dataclass(frozen=True)
class RssFilm:
    film_slug: str
    title: str | None
    rating: float | None
    liked: bool


@dataclass
class RefreshResult:
    lbusername: str
    watch_items: int
    upserted: int


def _fetch_rss(lbusername: str) -> str:
    """Blocking GET of the user's RSS feed. Raises RssFetchError on any
    non-200 or transport failure. Runs under the process-wide letterboxd
    serializer via the async wrapper in refresh_user_from_rss."""
    url = RSS_URL.format(lbusername)
    try:
        resp = requests.get(url, impersonate=_IMPERSONATE, timeout=(10, 30))
    except requests.errors.RequestsError as e:
        raise RssFetchError(lbusername, None, f"transport error: {e}") from e
    if resp.status_code != 200:
        raise RssFetchError(
            lbusername, resp.status_code, f"RSS returned HTTP {resp.status_code}"
        )
    return resp.text


def _slug_from_link(link: str) -> str | None:
    """Slug is the path segment after 'film' in /{user}/film/{slug}/[N/].

    Positional (parts[2]), not index('film'), so a user literally named 'film'
    can't misresolve to the wrong segment. The trailing /N/ on rewatch links
    isn't part of parts[2], so it falls away for free.
    """
    parts = [p for p in urlparse(link).path.split("/") if p]
    if len(parts) >= 3 and parts[1] == "film":
        return parts[2]
    return None


def _parse_rating(item: ET.Element) -> float | None:
    """memberRating is absent for logged-but-unrated films (-> None). A present
    but non-numeric value is tolerated as unrated rather than failing the whole
    refresh over one malformed item."""
    text = item.findtext("letterboxd:memberRating", namespaces=_NS)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_feed(xml_text: str, lbusername: str) -> list[RssFilm]:
    """Parse the feed into RssFilm rows, keeping only 'watch' diary items.

    Raises RssFetchError if the body is not a valid <rss> feed. This is how a
    Cloudflare interstitial served with HTTP 200 — which parses as HTML or not
    at all — surfaces as a gateway failure instead of being silently mistaken
    for an empty (upserted=0) refresh, which would hide a block on the one
    egress this endpoint exists to prove works.

    Unrated items omit <letterboxd:memberRating> — kept with rating=None.
    Items whose <link> yields no slug are skipped (no usable PK).
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise RssFetchError(
            lbusername, 200, f"RSS body is not valid XML: {e}"
        ) from e
    # Namespace-agnostic root check: a real feed's root is <rss>. Anything else
    # (e.g. an HTML challenge page that happened to parse) is not our data.
    if root.tag.rsplit("}", 1)[-1] != "rss":
        raise RssFetchError(
            lbusername, 200, f"expected <rss> root, got <{root.tag}>"
        )

    films: list[RssFilm] = []
    for item in root.iter("item"):
        guid = item.findtext("guid") or ""
        if not guid.startswith(WATCH_GUID_PREFIX):
            continue

        slug = _slug_from_link(item.findtext("link") or "")
        if slug is None:
            continue

        films.append(
            RssFilm(
                film_slug=slug,
                title=item.findtext("letterboxd:filmTitle", namespaces=_NS),
                rating=_parse_rating(item),
                liked=item.findtext("letterboxd:memberLike", namespaces=_NS)
                == "Yes",
            )
        )
    return films


async def refresh_user_from_rss(supabase: Client, lbusername: str) -> RefreshResult:
    """Fetch, parse, and upsert one user's recent watch history from RSS.

    Raises RssFetchError if the feed can't be fetched. A valid feed with no
    watch items returns a result with upserted=0.
    """
    # Routed through the shared serializer for the process-wide gate. Its
    # block-retry is inert here: _fetch_rss raises RssFetchError, not the
    # letterboxdpy block types is_block_error() recognizes, so a transient RSS
    # 429/503 propagates on the first attempt. Intentional — RSS is expected to
    # survive datacenter egress; classify status codes in _fetch_rss if that
    # ever needs to change.
    xml_text = await letterboxd_throttle.call(_fetch_rss, lbusername)
    films = _parse_feed(xml_text, lbusername)
    if not films:
        return RefreshResult(lbusername=lbusername, watch_items=0, upserted=0)

    rows: list[dict[str, Any]] = [
        {
            "lbusername": lbusername,
            "film_slug": f.film_slug,
            "rating": f.rating,
            "liked": f.liked,
            "title": f.title,
        }
        for f in films
    ]

    # Same conflict target and column set as user_films.py phase 1 — keep them
    # identical (see module docstring) so last-write-wins stays safe.
    supabase.table("UserFilms").upsert(
        rows, on_conflict="lbusername,film_slug"
    ).execute()

    return RefreshResult(
        lbusername=lbusername, watch_items=len(films), upserted=len(rows)
    )
