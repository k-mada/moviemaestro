from supabase import Client


def fetch_users(supabase: Client) -> list[str]:
    """Return the list of Letterboxd usernames the refresh pipeline should run for.

    Filters to is_discord=true. The Hater Rankings audience is the discord crew;
    other users in the table (e.g. lurkers added during exploration) are excluded.
    """
    resp = (
        supabase.table("Users")
        .select("lbusername")
        .eq("is_discord", True)
        .execute()
    )
    return [row["lbusername"] for row in resp.data]
