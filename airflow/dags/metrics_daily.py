from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import os

def compute_daily_metrics(execution_date, **context):
    conn = psycopg2.connect(
            dbname=os.getenv("MARKETING_DB", "marketing"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
        )

    day = execution_date.date()

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO campaign_metrics_daily (
                    campaign_id,
                    channel,
                    day,
                    impressions,
                    clicks,
                    signups,
                    cost,
                    ctr,
                    click_to_signup,
                    signup_to_activation,
                    total_cost
                )
                SELECT
                    campaign_id,
                    channel,
                    DATE(ts) AS day,
                    COUNT(*) FILTER (WHERE event_type='ad_impression') AS impressions,
                    COUNT(*) FILTER (WHERE event_type='ad_click') AS clicks,
                    COUNT(*) FILTER (WHERE event_type='signup') AS signups,
                    SUM(cost) FILTER (WHERE event_type='ad_click') AS cost,
                    CASE WHEN COUNT(*) FILTER (WHERE event_type='ad_impression') > 0
                         THEN COUNT(*) FILTER (WHERE event_type='ad_click')::DECIMAL
                              / COUNT(*) FILTER (WHERE event_type='ad_impression')
                         ELSE 0 END AS ctr,
                    CASE WHEN COUNT(*) FILTER (WHERE event_type='ad_click') > 0
                         THEN COUNT(*) FILTER (WHERE event_type='signup')::DECIMAL
                              / COUNT(*) FILTER (WHERE event_type='ad_click')
                         ELSE 0 END AS click_to_signup,
                    CASE WHEN COUNT(*) FILTER (WHERE event_type='signup') > 0
                         THEN COUNT(*) FILTER (WHERE event_type='activation')::DECIMAL
                              / COUNT(*) FILTER (WHERE event_type='signup')
                         ELSE 0 END AS signup_to_activation,
                    SUM(cost) AS total_cost
                FROM raw_events
                WHERE DATE(ts) = %s
                GROUP BY campaign_id, channel
                ON CONFLICT (campaign_id, channel, day)
                DO UPDATE SET
                    impressions = EXCLUDED.impressions,
                    clicks = EXCLUDED.clicks,
                    signups = EXCLUDED.signups,
                    cost = EXCLUDED.cost,
                    ctr = EXCLUDED.ctr,
                    click_to_signup = EXCLUDED.click_to_signup,
                    signup_to_activation = EXCLUDED.signup_to_activation,
                    total_cost = EXCLUDED.total_cost
                """,
                (day,)
            )

    conn.close()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="campaign_metrics_daily",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["marketing", "daily"],
) as dag:

    compute_daily = PythonOperator(
        task_id="compute_campaign_metrics_daily",
        python_callable=compute_daily_metrics,
    )
