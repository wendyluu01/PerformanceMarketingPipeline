import os
import requests
import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime

# -----------------------------
# Config
# -----------------------------
API_HOST = os.getenv("API_HOST", "api")
API_PORT = os.getenv("API_PORT", "8000")
BASE_URL = f"http://{API_HOST}:{API_PORT}"

st.set_page_config(
    page_title="Marketing Performance Dashboard",
    page_icon="📊",
    layout="wide",
)

# -----------------------------
# Cached API calls
# -----------------------------
@st.cache_data(ttl=15)
def fetch_funnel(minutes: int):
    r = requests.get(
        f"{BASE_URL}/funnels/realtime",
        params={"minutes": minutes},
        timeout=5,
    )
    r.raise_for_status()
    return pd.DataFrame(r.json()["rows"])


@st.cache_data(ttl=30)
def fetch_alerts(limit: int = 10):
    r = requests.get(
        f"{BASE_URL}/alerts/active",
        params={"limit": limit},
        timeout=5,
    )
    r.raise_for_status()
    return pd.DataFrame(r.json()["alerts"])


# -----------------------------
# Sidebar controls
# -----------------------------
with st.sidebar:
    st.header("Controls")

    window_minutes = st.slider(
        "Realtime window (minutes)",
        min_value=10,
        max_value=360,
        value=60,
        step=10,
    )

    if st.button("🔄 Refresh data"):
        st.cache_data.clear()

    st.divider()
    st.caption(
        f"Last refreshed: {datetime.utcnow().strftime('%H:%M:%S')} UTC"
    )

# -----------------------------
# Header
# -----------------------------
st.title("📈 Marketing Performance Dashboard")
st.caption("Real-time Ad → Acquisition Monitoring (API-driven)")

# -----------------------------
# Load data
# -----------------------------
df = fetch_funnel(window_minutes)

if df.empty:
    st.warning("No events yet. Generator may still be warming up.")
    st.stop()

# -----------------------------
# KPI calculations
# -----------------------------
totals = df[
    ["impressions", "clicks", "signups", "activations"]
].sum()

ctr = totals["clicks"] / totals["impressions"] if totals["impressions"] else 0
click_to_signup = totals["signups"] / totals["clicks"] if totals["clicks"] else 0
signup_to_activation = (
    totals["activations"] / totals["signups"] if totals["signups"] else 0
)

# -----------------------------
# KPI cards
# -----------------------------
k1, k2, k3, k4, k5, k6 = st.columns(6)

k1.metric("Impressions", f"{int(totals['impressions']):,}")
k2.metric("Clicks", f"{int(totals['clicks']):,}")
k3.metric("Signups", f"{int(totals['signups']):,}")
k4.metric("Activations", f"{int(totals['activations']):,}")
k5.metric("CTR", f"{ctr:.2%}")
k6.metric("Signup → Activation", f"{signup_to_activation:.2%}")

st.divider()


# -----------------------------
# Funnel chart
# -----------------------------
funnel_df = pd.DataFrame(
    {
        "Stage": ["Impressions", "Clicks", "Signups", "Activations"],
        "Count": [
            totals["impressions"],
            totals["clicks"],
            totals["signups"],
            totals["activations"],
        ],
    }
)

funnel_fig = px.funnel(
    funnel_df,
    x="Count",
    y="Stage",
    title="Overall Funnel (Realtime)",
)

st.plotly_chart(funnel_fig, use_container_width=True)

# -----------------------------
# Campaign performance table
# -----------------------------
df["CTR"] = df["clicks"] / df["impressions"]
df["Click → Signup"] = df["signups"] / df["clicks"]
df["Signup → Activation"] = df["activations"] / df["signups"]

df = df.fillna(0)

# alert flag (simple business rule)
df["Status"] = df.apply(
    lambda r: "🚨 At Risk"
    if (r["CTR"] < 0.02 or r["Signup → Activation"] < 0.4)
    else "✅ Healthy",
    axis=1,
)

display_df = (
    df[
        [
            "campaign_id",
            "channel",
            "Status",
            "impressions",
            "clicks",
            "signups",
            "activations",
            "CTR",
            "Click → Signup",
            "Signup → Activation",
        ]
    ]
    .sort_values("clicks", ascending=False)
    .copy()
)

# lock numeric precision (prevents twitching)
display_df["CTR"] = display_df["CTR"].round(3)
display_df["Click → Signup"] = display_df["Click → Signup"].round(3)
display_df["Signup → Activation"] = display_df["Signup → Activation"].round(3)

st.subheader("Campaign Performance")

table_container = st.container()
with table_container:
    st.data_editor(
        display_df,
        hide_index=True,
        disabled=True,
        use_container_width=True,
    )

# -----------------------------
# Clicks by campaign
# -----------------------------
bar_fig = px.bar(
    display_df,
    x="campaign_id",
    y="clicks",
    color="channel",
    title="Clicks by Campaign & Channel",
)

st.plotly_chart(bar_fig, use_container_width=True)

# -----------------------------
# 🚨 Anomaly alerts
# -----------------------------
st.subheader("🚨 Active Anomaly Alerts")

alerts_df = fetch_alerts()

if alerts_df.empty:
    st.success("No active anomalies detected ✅")
else:
    for _, row in alerts_df.iterrows():
        st.error(
            f"""
            **Campaign:** {row['campaign_id']}  
            **Metric:** {row['metric']}  
            **Observed:** {row['observed_value']}  
            **Expected:** {row['expected_value']}  
            **Triggered at:** {row['triggered_at']}
            """
        )

st.divider()

# -----------------------------
# Footer
# -----------------------------
st.caption(
    "API-first • Event-sourced • Airflow-aggregated • Streamlit business layer"
)
