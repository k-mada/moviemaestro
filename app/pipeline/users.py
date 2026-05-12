from supabase import Client


def fetch_users(supabase: Client) -> list[str]:
    """Return the list of Letterboxd usernames the refresh pipeline should run for.

    Filters to is_discord=true. The Hater Rankings audience is the discord crew;
    other users in the table (e.g. lurkers added during exploration) are excluded.

    Whitespace-trims each username and drops empty/None entries. letterboxdpy
    rejects anything that doesn't match ^[A-Za-z0-9_]+$, so a stray leading/
    trailing space (including unicode whitespace like NBSP) would crash phase 1
    with "AssertionError: Invalid username". Trimming here is the central
    chokepoint — every downstream caller gets clean values.
    """
    resp = (
        supabase.table("Users")
        .select("lbusername")
        .eq("is_discord", True)
        .execute()
    )
    cleaned: list[str] = []
    for row in resp.data:
        raw = row.get("lbusername")
        if not raw:
            continue
        trimmed = raw.strip()
        if trimmed:
            cleaned.append(trimmed)
    return cleaned
