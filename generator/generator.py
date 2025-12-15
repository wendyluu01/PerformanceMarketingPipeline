import os
import time
import uuid
import random
from datetime import datetime, timezone

import requests

# =========================
# Config
# =========================

API_HOST = os.getenv("API_HOST", "api")
API_PORT = os.getenv("API_PORT", "8000")
EVENTS_URL = f"http://{API_HOST}:{API_PORT}/events/batch"

EVENTS_PER_SECOND = 20        # global traffic rate
BATCH_SIZE = 50               # micro-batch size
FLUSH_INTERVAL = 2.0          # seconds

# =========================
# Campaign definitions
# =========================

CAMPAIGNS = [
    ("camp_google_brand", "google"),
    ("camp_meta_prospecting", "meta"),
    ("camp_tiktok_creative", "tiktok"),
    ("camp_email_reactivation", "email"),
]

RATES = {
    "camp_google_brand": {
        "ctr": 0.045,
        "click_to_signup": 0.14,
        "signup_to_activation": 0.65,
        "cpc": 0.90,
    },
    "camp_meta_prospecting": {
        "ctr": 0.028,
        "click_to_signup": 0.10,
        "signup_to_activation": 0.55,
        "cpc": 0.80,
    },
    "camp_tiktok_creative": {
        "ctr": 0.032,
        "click_to_signup": 0.08,
        "signup_to_activation": 0.50,
        "cpc": 0.60,
    },
    "camp_email_reactivation": {
        "ctr": 0.070,
        "click_to_signup": 0.20,
        "signup_to_activation": 0.75,
        "cpc": 0.02,
    },
}

# =========================
# Helpers
# =========================

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

buffer = []
last_flush = time.time()

def flush():
    """Send buffered events to API"""
    global buffer, last_flush

    if not buffer:
        return

    try:
        r = requests.post(EVENTS_URL, json=buffer, timeout=5)
        if r.status_code >= 300:
            print(f"[WARN] Batch rejected: {r.status_code} {r.text}")
    except Exception as e:
        print(f"[WARN] Failed to flush batch: {e}")

    buffer = []
    last_flush = time.time()

def enqueue_event(event: dict):
    """Add event to buffer and flush if needed"""
    global buffer, last_flush

    buffer.append(event)

    if (
        len(buffer) >= BATCH_SIZE
        or time.time() - last_flush >= FLUSH_INTERVAL
    ):
        flush()

# =========================
# Event generation
# =========================

def emit_user_journey(user_id: str, campaign_id: str, channel: str, rates: dict):
    # Impression
    enqueue_event({
        "event_id": str(uuid.uuid4()),
        "ts": now_utc(),
        "user_id": user_id,
        "campaign_id": campaign_id,
        "channel": channel,
        "event_type": "ad_impression",
        "cost": 0.0,
    })

    # Click
    if random.random() < rates["ctr"]:
        cost = max(0.0, random.gauss(rates["cpc"], rates["cpc"] * 0.15))
        enqueue_event({
            "event_id": str(uuid.uuid4()),
            "ts": now_utc(),
            "user_id": user_id,
            "campaign_id": campaign_id,
            "channel": channel,
            "event_type": "ad_click",
            "cost": float(cost),
        })

        # Signup
        if random.random() < rates["click_to_signup"]:
            enqueue_event({
                "event_id": str(uuid.uuid4()),
                "ts": now_utc(),
                "user_id": user_id,
                "campaign_id": campaign_id,
                "channel": channel,
                "event_type": "signup",
                "cost": 0.0,
            })

            # Activation
            if random.random() < rates["signup_to_activation"]:
                enqueue_event({
                    "event_id": str(uuid.uuid4()),
                    "ts": now_utc(),
                    "user_id": user_id,
                    "campaign_id": campaign_id,
                    "channel": channel,
                    "event_type": "activation",
                    "cost": 0.0,
                })

# =========================
# Main loop (rate-limited)
# =========================

def run():
    user_pool = [f"user_{i:05d}" for i in range(2000)]
    t0 = time.time()

    print(
        f"[STARTED] Generator | {EVENTS_PER_SECOND} events/sec | "
        f"batch={BATCH_SIZE} flush={FLUSH_INTERVAL}s"
    )

    while True:
        tick_start = time.time()

        # Toggle degradation every ~2 minutes
        elapsed = time.time() - t0
        degrade = (elapsed // 120) % 2 == 1

        for _ in range(EVENTS_PER_SECOND):
            campaign_id, channel = random.choice(CAMPAIGNS)
            user_id = random.choice(user_pool)

            rates = RATES[campaign_id].copy()

            if degrade and campaign_id == "camp_meta_prospecting":
                rates["ctr"] *= 0.55
                rates["click_to_signup"] *= 0.7

            emit_user_journey(user_id, campaign_id, channel, rates)

        # Maintain exact rate
        elapsed = time.time() - tick_start
        time.sleep(max(0, 1.0 - elapsed))

if __name__ == "__main__":
    run()
