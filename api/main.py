# Endpoint = a URL path attached to a python function with an HTTP method (GET, POST, etc)
# GET endpoint = retrieves data
# POST endpoint = creates data
# In our case, POST endpoint creates data that were generated from event generator
# Input validation = ensure that the data we receive is in the right format (Pydantic models help with this)
# class EventIn(BaseModel) = defines the schema for the input data for the POST /events endpoint


import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, List

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# database connection function
def db_conn():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
    )

# creates API server 
# app is the thing that receives requests (receptionist that routes requests to the right function)
app = FastAPI(title = "Ad to Acquisition API")

# Pydantic model for input validation
class EventIn(BaseModel):
    event_id: str
    ts: datetime
    user_id: str
    campaign_id: str
    channel: str
    event_type: str = Field(pattern="^(ad_impression|ad_click|signup|activation)$")
    cost: float = 0.0

# Health check endpoint (GET request to /health to check if server is running)
@app.get("/health")
def health():
    return {"status": "ok"}

# POST endpoint to ingest events
@app.post("/events")
def ingest_event(e: EventIn):
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw_events(event_id, ts, user_id, campaign_id, channel, event_type, cost)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (e.event_id, e.ts, e.user_id, e.campaign_id, e.channel, e.event_type, e.cost),
                )
        return {"ingested": True}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

#POST endpoint to ingest multiple events in batch
@app.post("/events/batch")
def ingest_batch(events: List[EventIn]):
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO raw_events(event_id, ts, user_id, campaign_id, channel, event_type, cost)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    [
                        (
                            e.event_id, e.ts, e.user_id,
                            e.campaign_id, e.channel,
                            e.event_type, e.cost
                        )
                        for e in events
                    ],
                )
        return {"ingested": len(events)}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

# GET endpoint to retrieve campaign KPIs
@app.get("/campaigns/{campaign_id}/kpis")
def campaign_kpis(campaign_id: str, hours: int = 24):
    """
    Returns last N hours of hourly KPI rows for a campaign.
    """
    if hours < 1 or hours > 168:
        raise HTTPException(status_code=400, detail="hours must be between 1 and 168")

    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM campaign_metrics_hourly
                WHERE campaign_id = %s AND hour >= %s
                ORDER BY hour ASC
                """,
                (campaign_id, since),
            )
            rows = cur.fetchall()
    return {"campaign_id": campaign_id, "hours": hours, "rows": rows}

# GET endpoint to retrieve active alerts
@app.get("/alerts/active")
def active_alerts(limit: int = 50):
    limit = max(1, min(limit, 200))
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM alerts
                ORDER BY triggered_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    return {"alerts": rows}


@app.get("/funnels/realtime")
def funnel_realtime(minutes: int = 60):
    """
    Realtime-ish funnel counts over the last N minutes computed directly from raw events.
    Good for demos and sanity checks.
    """
    minutes = max(5, min(minutes, 360))
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)

    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT campaign_id, channel,
                  SUM(CASE WHEN event_type='ad_impression' THEN 1 ELSE 0 END) AS impressions,
                  SUM(CASE WHEN event_type='ad_click' THEN 1 ELSE 0 END) AS clicks,
                  SUM(CASE WHEN event_type='signup' THEN 1 ELSE 0 END) AS signups,
                  SUM(CASE WHEN event_type='activation' THEN 1 ELSE 0 END) AS activations
                FROM raw_events
                WHERE ts >= %s
                GROUP BY campaign_id, channel
                ORDER BY clicks DESC
                """,
                (since,),
            )
            rows = cur.fetchall()
    return {"window_minutes": minutes, "rows": rows}
