def test_healthz_returns_ok(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_healthz_does_not_require_auth(client):
    # explicitly send no Authorization header
    r = client.get("/healthz", headers={})
    assert r.status_code == 200
