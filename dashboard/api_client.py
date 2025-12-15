import os
import requests

API_BASE = os.getenv("API_BASE", "http://api:8000")

def get_realtime_funnel(minutes=60):
    r = requests.get(f"{API_BASE}/funnels/realtime", params={"minutes": minutes})
    r.raise_for_status()
    return r.json()["rows"]

def get_campaign_kpis(campaign_id, hours=24):
    r = requests.get(
        f"{API_BASE}/campaigns/{campaign_id}/kpis",
        params={"hours": hours},
    )
    r.raise_for_status()
    return r.json()["rows"]

def get_alerts(limit=50):
    r = requests.get(f"{API_BASE}/alerts/active", params={"limit": limit})
    r.raise_for_status()
    return r.json()["alerts"]
