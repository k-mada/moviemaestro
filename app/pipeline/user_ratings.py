"""Shared UserRatings histogram refresh for both the grid scrape and RSS paths."""

from __future__ import annotations

from supabase import Client


def refresh_user_ratings(supabase: Client, lbusername: str) -> None:
    """Recompute lbusername's UserRatings histogram from their UserFilms rows.

    Full-replace, atomic server-side. Callers must have upserted UserFilms first.
    """
    supabase.rpc("refresh_user_ratings", {"p_username": lbusername}).execute()
