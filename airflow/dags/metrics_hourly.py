import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import psycopg2

def get_conn():
    return psycopg2.connect(
    dbname=os.getenv("MARKETING_DB", "marketing"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
)


def aggregate_last_full_hour():
    """Aggregate raw_events into campaign_metrics_hourly for the last completed hour."""
    now = datetime.now(timezone.utc)
    hour_end = now.replace(minute=0, second=0, microsecond=0)
    hour_start = hour_end - timedelta(hours=1)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH base AS (
                    SELECT
                        campaign_id,
                        channel,
                        date_trunc('hour', ts) as hour,
                        SUM(CASE WHEN event_type='ad_impression' THEN 1 ELSE 0 END) AS impressions,
                        SUM(CASE WHEN event_type='ad_click' THEN 1 ELSE 0 END) AS clicks,
                        SUM(CASE WHEN event_type='activation' THEN 1 ELSE 0 END) AS activations,
                        SUM(CASE WHEN event_type ='signup' THEN 1 ELSE 0 END) AS signups,
                        SUM(CASE WHEN event_type = 'ad_click' THEN cost ELSE 0 END) AS total_cost
                    FROM raw_events
                    WHERE ts >= %s and ts < %s
                    GROUP BY campaign_id, channel, date_trunc('hour', ts)
                )
                INSERT INTO campaign_metrics_hourly (
                    campaign_id, channel, hour, impressions, clicks, activations, signups, total_cost,
                    ctr, click_to_signup, signup_to_activation
                )
                SELECT
                  campaign_id, channel, hour, impressions, clicks, signups, activations, total_cost,
                  CASE WHEN impressions=0 THEN 0 ELSE clicks::float/impressions END AS ctr,
                  CASE WHEN clicks=0 THEN 0 ELSE signups::float/clicks END AS click_to_signup,
                  CASE WHEN signups=0 THEN 0 ELSE activations::float/signups END AS signup_to_activation
                  FROM base
                  ON CONFLICT (campaign_id, channel, hour) DO UPDATE SET
                    impressions = EXCLUDED.impressions,
                    clicks = EXCLUDED.clicks,
                    activations = EXCLUDED.activations,
                    signups = EXCLUDED.signups,
                    total_cost = EXCLUDED.total_cost,
                    ctr = EXCLUDED.ctr,
                    click_to_signup = EXCLUDED.click_to_signup,
                    signup_to_activation = EXCLUDED.signup_to_activation;
                """,
                (hour_start, hour_end),
            )

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1, "retry_delay": timedelta(minutes=1)} 

dag = DAG(
    dag_id="metrics_hourly",
    default_args=default_args,
    description="Aggregate raw events into hourly campaign metrics",
    schedule="*/5 * * * *",  # every 5 minutes (recomputes last full hour safely)
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    tags=["marketing", "etl"],
    catchup=False)
t1 = PythonOperator(
    task_id="aggregate_last_full_hour",
    python_callable=aggregate_last_full_hour,
    dag=dag)
    
