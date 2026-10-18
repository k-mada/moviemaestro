"""In-memory fake Supabase client for unit tests.

Implements just enough of supabase-py's chained-builder API for the calls our
pipeline makes. Attempting to mock supabase via MagicMock alone gets fragile
fast; this gives us assertable state and a real query-shape contract.

Supported per-table operations:
  .table(name).select("*").eq(col, val).execute()
  .table(name).select("status").eq("id", uuid).maybe_single().execute()
  .table(name).select("id").eq(...).neq(...).limit(1).execute()
  .table(name).select("col").is_("col", "null").order("col").limit(n).execute()
  .table(name).select("col").gt("col", "x").execute()
  .table(name).update(payload).eq("id", uuid).execute()
  .table(name).update(payload).eq(...).is_("col", "null").execute()
  .table(name).upsert(rows).execute()  # ignores on_conflict; PK-equivalent dedupe
  .rpc(fn).execute()

Built-in RPC stubs:
  upsert_film(params) — emulates the COALESCE-preserve upsert for the Films
  table so callers of scrape_and_upsert_film / tombstone_film exercise the
  same never-clobber-non-NULL semantics the Postgres function provides.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

# Mirrors the RPC's identity guard: only a real, non-zero TMDB movie/tv id.
_TMDB_ID = re.compile(r"/(movie|tv)/[1-9][0-9]*/?$")


@dataclass
class _Resp:
    data: Any


class FakeSupabase:
    def __init__(self) -> None:
        # Per-table row stores. Keys are arbitrary; values are dicts.
        self.tables: dict[str, list[dict[str, Any]]] = {
            "Users": [],
            "UserFilms": [],
            "UserRatings": [],
            "Films": [],
            "refresh_jobs": [],
        }
        # RPC stubs: name -> callable returning data
        self.rpcs: dict[str, Any] = {
            "get_missing_films": lambda: [],
            "upsert_film": self._upsert_film_rpc,
            "refresh_user_ratings": self._refresh_user_ratings_rpc,
            "upsert_film_and_merge_duplicates": self._upsert_film_and_merge_rpc,
        }
        # Capture every write call for assertions (chronological).
        self.writes: list[dict[str, Any]] = []
        # Canonical slugs passed to the merge RPC, in call order.
        self.merge_calls: list[str] = []

    def _upsert_film_rpc(self, params: dict) -> None:
        # Mirrors the upsert_film() Postgres function: per-column COALESCE so
        # a NULL in params never overwrites an existing non-NULL value.
        rows = self.tables.setdefault("Films", [])
        incoming = {
            "film_slug":    params["p_film_slug"],
            "title":        params["p_title"],
            "lb_rating":    params["p_lb_rating"],
            "url":          params["p_url"],
            "tmdb_link":    params["p_tmdb_link"],
            "poster":       params["p_poster"],
            "banner":       params["p_banner"],
            "release_year": params["p_release_year"],
        }
        for r in rows:
            if r.get("film_slug") == incoming["film_slug"]:
                for k, v in incoming.items():
                    if v is not None:
                        r[k] = v
                return
        rows.append(incoming)

    def _refresh_user_ratings_rpc(self, params: dict) -> None:
        # Mirrors the refresh_user_ratings() Postgres function: full-replace the
        # user's histogram by re-aggregating their UserFilms rows by rating.
        username = params["p_username"]
        ur = self.tables.setdefault("UserRatings", [])
        ur[:] = [r for r in ur if r.get("username") != username]
        counts: dict[float, int] = {}
        for r in self.tables.get("UserFilms", []):
            if r.get("lbusername") == username and r.get("rating") is not None:
                counts[r["rating"]] = counts.get(r["rating"], 0) + 1
        for rating, count in counts.items():
            ur.append({"username": username, "rating": rating, "count": count})

    def _upsert_film_and_merge_rpc(self, params: dict) -> int:
        # Mirrors upsert_film_and_merge_duplicates(): upsert the canonical film,
        # then fold every other same-tmdb_link slug into it. Returns the number
        # of merged (deleted) Films rows. Records the call for assertions.
        self._upsert_film_rpc(params)
        canonical = params["p_film_slug"]
        self.merge_calls.append(canonical)

        films = self.tables.setdefault("Films", [])
        row = next((r for r in films if r.get("film_slug") == canonical), None)
        v_tmdb = row.get("tmdb_link") if row else None
        if not v_tmdb or not _TMDB_ID.search(v_tmdb):
            return 0

        loser_slugs = {
            r["film_slug"]
            for r in films
            if r.get("tmdb_link") == v_tmdb and r.get("film_slug") != canonical
        }
        if not loser_slugs:
            return 0

        uf = self.tables.setdefault("UserFilms", [])
        affected = sorted(
            {r["lbusername"] for r in uf if r.get("film_slug") in loser_slugs}
        )

        for owner in affected:
            has_canonical = any(
                r for r in uf
                if r.get("lbusername") == owner and r.get("film_slug") == canonical
            )
            owner_losers = [
                r for r in uf
                if r.get("lbusername") == owner and r.get("film_slug") in loser_slugs
            ]
            if not has_canonical and owner_losers:
                # Promote the best loser row to the canonical slug; the rest fall
                # into the delete below. Ordering mirrors the RPC's DISTINCT ON:
                # prefer a real positive rating, then higher rating, then slug.
                best = min(
                    owner_losers,
                    key=lambda r: (
                        not (r.get("rating") not in (None, 0)),
                        -(r.get("rating") or 0),
                        r.get("film_slug") or "",
                    ),
                )
                best["film_slug"] = canonical
                owner_losers.remove(best)
            for r in owner_losers:
                uf.remove(r)

        fr = self.tables.setdefault("FilmRatings", [])
        fr[:] = [r for r in fr if r.get("film_slug") not in loser_slugs]
        films[:] = [r for r in films if r.get("film_slug") not in loser_slugs]

        for owner in affected:
            self._refresh_user_ratings_rpc({"p_username": owner})

        return len(loser_slugs)

    def table(self, name: str) -> "_Table":
        return _Table(self, name)

    def rpc(self, name: str, params: dict | None = None) -> "_Rpc":
        return _Rpc(self, name, params)

    # --- helpers for tests ----------------------------------------------------

    def insert_users(self, *usernames: str, is_discord: bool = True) -> None:
        for u in usernames:
            self.tables["Users"].append({"lbusername": u, "is_discord": is_discord})

    def insert_refresh_job(
        self, job_id: UUID, status: str = "running", *, table: str = "refresh_jobs", **extra: Any
    ) -> None:
        self.tables.setdefault(table, []).append(
            {
                "id": str(job_id),
                "status": status,
                "phase": None,
                "progress": {},
                "errors": [],
                "log_tail": "",
                **extra,
            }
        )

    def get_refresh_job(self, job_id: UUID, *, table: str = "refresh_jobs") -> dict | None:
        for r in self.tables.get(table, []):
            if r["id"] == str(job_id):
                return r
        return None

    def set_refresh_job_status(
        self, job_id: UUID, status: str, *, table: str = "refresh_jobs"
    ) -> None:
        row = self.get_refresh_job(job_id, table=table)
        if row is None:
            raise KeyError(job_id)
        row["status"] = status


@dataclass
class _Filter:
    col: str
    op: str  # "eq" | "neq" | "gt" | "is_null" | "is_not_null"
    val: Any

    def matches(self, row: dict) -> bool:
        if self.op == "eq":
            return row.get(self.col) == self.val
        if self.op == "neq":
            return row.get(self.col) != self.val
        if self.op == "gt":
            # PostgREST-equivalent strict greater-than. Rows where the column
            # is None never match (matches Postgres' NULL > x → NULL → false).
            v = row.get(self.col)
            return v is not None and v > self.val
        if self.op == "is_null":
            return row.get(self.col) is None
        if self.op == "is_not_null":
            return row.get(self.col) is not None
        raise NotImplementedError(self.op)


@dataclass
class _Table:
    sb: FakeSupabase
    name: str
    _select_cols: str | None = None
    _filters: list[_Filter] = field(default_factory=list)
    _limit: int | None = None
    _order_by: str | None = None
    _maybe_single: bool = False
    _action: str | None = None
    _payload: Any = None
    _on_conflict: str | None = None

    # query builders
    def select(self, cols: str = "*") -> "_Table":
        self._action = "select"
        self._select_cols = cols
        return self

    def update(self, payload: dict) -> "_Table":
        self._action = "update"
        self._payload = payload
        return self

    def upsert(self, payload, on_conflict: str | None = None) -> "_Table":
        self._action = "upsert"
        self._payload = payload
        self._on_conflict = on_conflict
        return self

    def eq(self, col: str, val: Any) -> "_Table":
        self._filters.append(_Filter(col, "eq", val))
        return self

    def neq(self, col: str, val: Any) -> "_Table":
        self._filters.append(_Filter(col, "neq", val))
        return self

    def gt(self, col: str, val: Any) -> "_Table":
        self._filters.append(_Filter(col, "gt", val))
        return self

    def is_(self, col: str, val: str) -> "_Table":
        # PostgREST surface accepts ?col=is.null / ?col=is.not.null. supabase-py
        # exposes that as .is_(col, "null") / .is_(col, "not.null"). Other
        # values (is.true / is.false) aren't used in this codebase yet.
        if val == "null":
            self._filters.append(_Filter(col, "is_null", None))
        elif val in ("not.null", "not_null"):
            self._filters.append(_Filter(col, "is_not_null", None))
        else:
            raise NotImplementedError(f"is_({col!r}, {val!r})")
        return self

    def order(self, col: str, desc: bool = False) -> "_Table":
        # Single-column ascending only — that's all supabase-py callers in this
        # project use. Add desc/multi-col support when something needs it.
        if desc:
            raise NotImplementedError("desc ordering not supported in fake yet")
        self._order_by = col
        return self

    def limit(self, n: int) -> "_Table":
        self._limit = n
        return self

    def maybe_single(self) -> "_Table":
        self._maybe_single = True
        return self

    def execute(self):
        rows = self.sb.tables.setdefault(self.name, [])

        if self._action == "select":
            matched = [r for r in rows if all(f.matches(r) for f in self._filters)]
            if self._order_by is not None:
                # None sorts last so callers don't accidentally page through it.
                matched.sort(key=lambda r: (r.get(self._order_by) is None,
                                            r.get(self._order_by)))
            if self._limit is not None:
                matched = matched[: self._limit]
            if self._maybe_single:
                return _Resp(data=matched[0] if matched else None)
            return _Resp(data=matched)

        if self._action == "update":
            updated = []
            for r in rows:
                if all(f.matches(r) for f in self._filters):
                    r.update(self._payload)
                    updated.append(r)
            self.sb.writes.append({
                "table": self.name, "op": "update",
                "payload": dict(self._payload),
                "filters": [(f.col, f.op, f.val) for f in self._filters],
                "matched": len(updated),
            })
            return _Resp(data=updated)

        if self._action == "upsert":
            payload = self._payload if isinstance(self._payload, list) else [self._payload]
            # Naive PK dedupe: use on_conflict as the key spec, fall back to "id"
            keys = (self._on_conflict or "id").split(",")
            for new_row in payload:
                idx = None
                for i, existing in enumerate(rows):
                    if all(existing.get(k) == new_row.get(k) for k in keys):
                        idx = i
                        break
                if idx is None:
                    rows.append(dict(new_row))
                else:
                    rows[idx].update(new_row)
            self.sb.writes.append({
                "table": self.name, "op": "upsert",
                "rows_count": len(payload), "on_conflict": self._on_conflict,
            })
            return _Resp(data=payload)

        raise NotImplementedError(f"action {self._action} not supported on fake")


@dataclass
class _Rpc:
    sb: FakeSupabase
    name: str
    params: dict | None = None

    def execute(self):
        if self.name not in self.sb.rpcs:
            raise NotImplementedError(f"rpc {self.name} not stubbed in fake")
        fn = self.sb.rpcs[self.name]
        if not callable(fn):
            return _Resp(data=fn)
        # Stubs can be either zero-arg (legacy `lambda: [...]`) or take the
        # params dict. Try the params form first; fall back if the stub
        # doesn't accept args.
        if self.params is not None:
            try:
                return _Resp(data=fn(self.params))
            except TypeError:
                pass
        return _Resp(data=fn())
