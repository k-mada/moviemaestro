"""In-memory fake Supabase client for unit tests.

Implements just enough of supabase-py's chained-builder API for the calls our
pipeline makes. Attempting to mock supabase via MagicMock alone gets fragile
fast; this gives us assertable state and a real query-shape contract.

Supported per-table operations:
  .table(name).select("*").eq(col, val).execute()
  .table(name).select("status").eq("id", uuid).maybe_single().execute()
  .table(name).select("id").eq(...).neq(...).limit(1).execute()
  .table(name).update(payload).eq("id", uuid).execute()
  .table(name).upsert(rows).execute()  # ignores on_conflict; PK-equivalent dedupe
  .rpc(fn).execute()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID


@dataclass
class _Resp:
    data: Any


class FakeSupabase:
    def __init__(self) -> None:
        # Per-table row stores. Keys are arbitrary; values are dicts.
        self.tables: dict[str, list[dict[str, Any]]] = {
            "Users": [],
            "UserFilms": [],
            "Films": [],
            "refresh_jobs": [],
        }
        # RPC stubs: name -> callable returning data
        self.rpcs: dict[str, Any] = {"get_missing_films": lambda: []}
        # Capture every write call for assertions (chronological).
        self.writes: list[dict[str, Any]] = []

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
    op: str  # "eq" | "neq"
    val: Any

    def matches(self, row: dict) -> bool:
        if self.op == "eq":
            return row.get(self.col) == self.val
        if self.op == "neq":
            return row.get(self.col) != self.val
        raise NotImplementedError(self.op)


@dataclass
class _Table:
    sb: FakeSupabase
    name: str
    _select_cols: str | None = None
    _filters: list[_Filter] = field(default_factory=list)
    _limit: int | None = None
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
