"""Domain B: incremental user refresh from Letterboxd's per-user RSS feed.

The HTML grid is hard-blocked on Railway's datacenter egress; RSS is not. Writes
the same UserFilms column set as the grid path (user_films.py); both are
upsert-only, so last-write-wins is safe regardless of run order.
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
_IMPERSONATE = "chrome"
_NS = {
    "letterboxd": "https://letterboxd.com",
    "tmdb": "https://themoviedb.org",
}


class RssFetchError(RuntimeError):
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
    # /{user}/film/{slug}/[N/] — positional, so a user named 'film' can't
    # misresolve and a rewatch's trailing /N/ drops off.
    parts = [p for p in urlparse(link).path.split("/") if p]
    if len(parts) >= 3 and parts[1] == "film":
        return parts[2]
    return None


def _parse_rating(item: ET.Element) -> float | None:
    text = item.findtext("letterboxd:memberRating", namespaces=_NS)
    if not text:  # absent/empty for logged-but-unrated films
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_feed(xml_text: str, lbusername: str) -> list[RssFilm]:
    # Reject a non-<rss> body (e.g. a Cloudflare interstitial served as 200) so a
    # block surfaces as an error instead of a silent empty refresh.
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise RssFetchError(lbusername, 200, f"RSS body is not valid XML: {e}") from e
    if root.tag.rsplit("}", 1)[-1] != "rss":
        raise RssFetchError(lbusername, 200, f"expected <rss> root, got <{root.tag}>")

    films: list[RssFilm] = []
    for item in root.iter("item"):
        if not (item.findtext("guid") or "").startswith(WATCH_GUID_PREFIX):
            continue
        slug = _slug_from_link(item.findtext("link") or "")
        if slug is None:
            continue
        films.append(
            RssFilm(
                film_slug=slug,
                title=item.findtext("letterboxd:filmTitle", namespaces=_NS),
                rating=_parse_rating(item),
                liked=item.findtext("letterboxd:memberLike", namespaces=_NS) == "Yes",
            )
        )
    return films


async def refresh_user_from_rss(supabase: Client, lbusername: str) -> RefreshResult:
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
    supabase.table("UserFilms").upsert(
        rows, on_conflict="lbusername,film_slug"
    ).execute()

    return RefreshResult(
        lbusername=lbusername, watch_items=len(films), upserted=len(rows)
    )
