"""Phase 3 helpers: scrape one film's Letterboxd average rating + metadata, upsert into Films."""

from __future__ import annotations

from letterboxdpy.movie import Movie
from supabase import Client

# Marker stored in Films.url for slugs that 404'd on Letterboxd. Distinguishable
# from a real Letterboxd URL by inspection or `WHERE url = 'unreachable'`.
TOMBSTONE_URL = "unreachable"


def _upsert_film_rpc(supabase: Client, params: dict) -> None:
    # Wraps the upsert_film() Postgres function. INSERT...ON CONFLICT with
    # per-column COALESCE so a NULL in `params` never clobbers an existing
    # non-NULL value. See migration 20260610200000_add_upsert_film_function.sql.
    supabase.rpc("upsert_film", params).execute()


def scrape_and_upsert_film(supabase: Client, slug: str) -> None:
    """Scrape one Letterboxd film and upsert into Films.

    Raises whatever letterboxdpy raises on failure — caller catches and records.
    """
    movie = Movie(slug)
    _upsert_film_rpc(supabase, {
        "p_film_slug":    movie.slug,
        "p_title":        movie.title,
        "p_lb_rating":    movie.rating,
        "p_url":          movie.url,
        "p_tmdb_link":    getattr(movie, "tmdb_link", None),
        "p_poster":       getattr(movie, "poster", None),
        "p_banner":       getattr(movie, "banner", None),
        "p_release_year": getattr(movie, "year", None),
    })


def tombstone_film(supabase: Client, slug: str) -> None:
    """Insert a stub Films row to suppress future retries of a 404'd slug.

    get_missing_films() returns UserFilms slugs without a matching Films row,
    so a stub row — even with lb_rating NULL — keeps the slug out of future
    runs. Manual retry: DELETE FROM "Films" WHERE film_slug = '...'.
    """
    _upsert_film_rpc(supabase, {
        "p_film_slug":    slug,
        "p_title":        None,
        "p_lb_rating":    None,
        "p_url":          TOMBSTONE_URL,
        "p_tmdb_link":    None,
        "p_poster":       None,
        "p_banner":       None,
        "p_release_year": None,
    })
