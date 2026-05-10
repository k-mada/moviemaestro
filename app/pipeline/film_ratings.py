"""Phase 3 helpers: scrape one film's Letterboxd average rating + metadata, upsert into Films."""

from __future__ import annotations

from letterboxdpy.movie import Movie
from supabase import Client


def scrape_and_upsert_film(supabase: Client, slug: str) -> None:
    """Scrape one Letterboxd film and upsert into Films.

    Raises whatever letterboxdpy raises on failure — caller catches and records.
    """
    movie = Movie(slug)
    row = {
        "film_slug": movie.slug,
        "url": movie.url,
        "title": movie.title,
        "lb_rating": movie.rating,
        "tmdb_link": getattr(movie, "tmdb_link", None),
        "poster": getattr(movie, "poster", None),
        "banner": getattr(movie, "banner", None),
    }
    supabase.table("Films").upsert(row, on_conflict="film_slug").execute()
