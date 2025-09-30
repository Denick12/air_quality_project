
import time
import json
import psycopg2
from kafka import KafkaProducer

conn = psycopg2.connect("dbname=air_quality user=airflow password=airflow host=postgres_air port=5432")
cursor = conn.cursor()

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

last_id = 0

while True:
    cursor.execute("SELECT * FROM air_quality WHERE id > %s ORDER BY id ASC", (last_id,))
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    for row in rows:
        record = dict(zip(colnames, row))
        producer.send("air_quality_topic", value=record)
        last_id = record["id"]

    producer.flush()
    time.sleep(60)  # pulls every minute
