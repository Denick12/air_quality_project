from __future__ import annotations
import pendulum
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime
import pytz

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import (
    create_engine, Table, Column, Float, Integer, String, MetaData, insert
)
from sqlalchemy.dialects.postgresql import TIMESTAMP


# ----------------------------
# EXTRACT
# ----------------------------
def extract_air_quality_data():
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": [-1.286389, -4.043477],
        "longitude": [36.817223, 39.668206],
        "current": ["pm10", "pm2_5", "ozone", "carbon_monoxide",
                    "nitrogen_dioxide", "sulphur_dioxide", "uv_index"],
    }
    responses = openmeteo.weather_api(url, params=params)
    locations = {(-1.286389, 36.817223): "Nairobi", (-4.043477, 39.668206): "Mombasa"}

    extracted_data = []
    for response in responses:
        current = response.Current()
        utc_offset_seconds = response.UtcOffsetSeconds()
        tz = pytz.FixedOffset(utc_offset_seconds // 60)
        current_time = datetime.fromtimestamp(current.Time(), tz=tz)

        loc = locations.get(
            (round(response.Latitude(), 6), round(response.Longitude(), 6)),
            "Unknown"
        )

        data = {
            "location": loc,
            "latitude": response.Latitude(),
            "longitude": response.Longitude(),
            "elevation": response.Elevation(),
            "utc_offset_seconds": utc_offset_seconds,
            "current_time": current_time.isoformat(),
            "pm10": current.Variables(0).Value(),
            "pm2_5": current.Variables(1).Value(),
            "ozone": current.Variables(2).Value(),
            "carbon_monoxide": current.Variables(3).Value(),
            "nitrogen_dioxide": current.Variables(4).Value(),
            "sulphur_dioxide": current.Variables(5).Value(),
            "uv_index": current.Variables(6).Value(),
        }
        extracted_data.append(data)

    return extracted_data


# ----------------------------
# TRANSFORM
# ----------------------------
def transform_air_quality_data(ti=None):
    data = ti.xcom_pull(task_ids="extract_data")
    if not data:
        return []

    transformed = []
    for row in data:
        # Fill missing with 0 and round floats
        for k, v in row.items():
            if isinstance(v, float):
                row[k] = round(v if v is not None else 0, 2)

        row["location"] = row["location"].title()
        transformed.append(row)

    return transformed


# ----------------------------
# LOAD
# ----------------------------
def load_air_quality_data(ti=None):
    data = ti.xcom_pull(task_ids="transform_data")
    if not data:
        return

    engine = create_engine("postgresql://airflow:airflow@postgres_air:5432/air_quality")
    metadata = MetaData()

    air_quality = Table(
        "air_quality", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("location", String(100)),
        Column("latitude", Float),
        Column("longitude", Float),
        Column("elevation", Float),
        Column("utc_offset_seconds", Integer),
        Column("current_time", TIMESTAMP),
        Column("pm10", Float),
        Column("pm2_5", Float),
        Column("ozone", Float),
        Column("carbon_monoxide", Float),
        Column("nitrogen_dioxide", Float),
        Column("sulphur_dioxide", Float),
        Column("uv_index", Float),
    )

    metadata.create_all(engine)

    with engine.begin() as conn:
        for row in data:
            row["current_time"] = datetime.fromisoformat(row["current_time"])
            stmt = insert(air_quality).values(row)
            conn.execute(stmt)


# ----------------------------
# DAG
# ----------------------------
with DAG(
    dag_id="air_quality_etl",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule="0 */2 * * *",  # every 2 hours
    tags=["etl", "air_quality"],
) as dag:
    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_air_quality_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_air_quality_data,
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_air_quality_data,
    )

    extract >> transform >> load

