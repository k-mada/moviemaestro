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


def get_missing_film_slugs_for_user(supabase: Client, lbusername: str) -> list[str]:
    """Same as get_missing_film_slugs but scoped to one user's UserFilms.

    Wraps the Postgres function get_missing_films_for_user(p_lbusername text),
    which lands with the user_scrape_jobs migration (bpdiscord-aiy step 1c).
    """
    resp = supabase.rpc(
        "get_missing_films_for_user", {"p_lbusername": lbusername}
    ).execute()
    return list(resp.data or [])
