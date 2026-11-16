from supabase import Client


def fetch_users(supabase: Client) -> list[str]:
    """Return the list of Letterboxd usernames the refresh pipeline should run for.

    Every Users row, Discord member or not: non-Discord profiles are compared
    against on bpdiscord, so their data must stay as fresh as everyone else's.
    The Stats audience gate (is_discord) is applied by bpdiscord's reads.

    Whitespace-trims each username and drops empty/None entries. letterboxdpy
    rejects anything that doesn't match ^[A-Za-z0-9_]+$, so a stray leading/
    trailing space (including unicode whitespace like NBSP) would crash phase 1
    with "AssertionError: Invalid username". Trimming here is the central
    chokepoint — every downstream caller gets clean values.
    """
    resp = (
        supabase.table("Users")
        .select("lbusername")
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
