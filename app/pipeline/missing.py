from supabase import Client


def get_missing_film_slugs(supabase: Client) -> list[str]:
    """Return slugs that exist in UserFilms but not in Films.

    Wraps the Postgres function get_missing_films() defined in the baseline
    migration (20260502215921_remote_schema.sql:101). Single source of truth
    for the SQL — we don't reimplement the join here.
    """
    resp = supabase.rpc("get_missing_films").execute()
    # The RPC returns text[] which supabase-py surfaces as a list at .data.
    return list(resp.data or [])
