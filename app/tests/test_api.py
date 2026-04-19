from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_health():
    res = client.get("/health")
    assert res.status_code in (200, 503)


def test_risk_events():
    res = client.get("/risk-events")
    assert res.status_code == 200
    assert "events" in res.json()


def test_pricing():
    res = client.get("/pricing")
    assert res.status_code == 200
    assert "tiers" in res.json()


def test_user_status():
    res = client.get("/user-status")
    assert res.status_code == 200
    assert "tier" in res.json()
