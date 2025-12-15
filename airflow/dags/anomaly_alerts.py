import os
import uuid
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

def detect_ctr_drop(threshold_drop=0.30, baseline_hours = 72):
    """
    Alert if the CTR in the last completed hour drops more than the threshold_drop vs trailing baseline average
    """

    now = datetime.now(timezone.utc)
    hour_end = now.replace(minute=0, second=0, microsecond=0)
    hour_start = hour_end - timedelta(hours=1)
    baseline_start = hour_end - timedelta(hours=baseline_hours)

    with get_conn() as conn:
        with conn.cursor() as cur:
            # last hour ctr per campaign per channel
            cur.execute(
                """
                SELECT campaign_id, channel, ctr
                FROM campaign_metrics_hourly
                WHERE hour >= %s AND hour < %s
                """,
                (hour_start,hour_end),
            )
            last_rows = cur.fetchall()

            for campaign_id, channel, last_ctr in last_rows:
                cur.execute(
                    """
                    SELECT AVG(ctr)
                    FROM campaign_metrics_hourly
                    WHERE campaign_id =%s AND channel =%s AND hour >= %s AND hour < %s"""
                    ,
                    (campaign_id, channel, baseline_start, hour_start),
                )
                baseline_avg_ctr = cur.fetchone()[0] 
                if baseline_avg_ctr is None or baseline_avg_ctr:
                    continue  # no baseline data

                drop = (baseline_avg_ctr - last_ctr) / baseline_avg_ctr
                if drop >= threshold_drop:
                    alert_id = str(uuid.uuid4())
                    msg = (
                        f"CTR drop detected for campaign {campaign_id} on channel {channel}: "
                        f"baseline avg CTR={baseline_avg_ctr:.4f}, last hour CTR={last_ctr:.4f}, drop={drop:.2%}"
                    )
                    cur.execute(
                        """
                       INSERT INTO alerts(alert_id, triggered_at, campaign_id, channel, metric,
                                           current_value, baseline_value, threshold, message)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (alert_id) DO NOTHING
                        """,
                        (
                            alert_id,
                            datetime.now(timezone.utc),
                            campaign_id,
                            channel,
                            "ctr_drop",
                            float(last_ctr),
                            float(baseline_avg_ctr),
                            float(threshold_drop),
                            msg,
                        ),
                    )

default_args = {"owner": "airflow", "retries": 0} 

dag = DAG(
    dag_id="anomaly_alerts",
    default_args=default_args,
    description="Detect anomalies in campaign metrics and create alerts",
    schedule="*/15 * * * *",  # every 15 minutes
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    tags=["marketing", "alerts"],
    catchup=False)

detect_ctr_drop_task = PythonOperator(
    task_id="detect_ctr_drop",
    python_callable=detect_ctr_drop,
    op_kwargs={"threshold_drop": 0.30, "baseline_hours": 72}, 
    dag=dag)
