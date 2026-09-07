# Databricks notebook source
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.secrets.create_scope(scope="github")
print("created")
print("scopes:", [s.name for s in w.secrets.list_scopes()])

# COMMAND ----------

import requests
token = dbutils.secrets.get(scope="github", key="pat")
r = requests.get(
    "https://api.github.com/repos/apache/airflow",
    headers={"Authorization": f"Bearer {token}"},
    timeout=30,
)
print(r.status_code, r.json()["stargazers_count"])
print("rate limit remaining:", r.headers["X-RateLimit-Remaining"])

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists gh;
# MAGIC create schema if not exists gh.bronze;
# MAGIC create table if not exists gh.bronze.raw_airflow_issues (
# MAGIC     ingested_at timestamp,
# MAGIC     payload     string
# MAGIC );

# COMMAND ----------

import json, requests
from datetime import datetime, timezone
from pyspark.sql import Row

token = dbutils.secrets.get(scope="github", key="pat")
run_ts = datetime.now(timezone.utc)

since = spark.sql("""
    select coalesce(
        max(payload:updated_at::timestamp),
        timestamp'2026-06-01'
    ) from gh.bronze.raw_airflow_issues
""").collect()[0][0]


def fetch_issues(since, max_pages=50):
    url = "https://api.github.com/repos/apache/airflow/issues"
    params = {
        "state": "all",
        "since": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sort": "updated",
        "direction": "asc",
        "per_page": 100,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    for _ in range(max_pages):
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            return
        yield from batch
        nxt = r.links.get("next")
        if not nxt:
            return
        url, params = nxt["url"], None

records = list(fetch_issues(since))

if records:
    rows = [Row(ingested_at=run_ts, payload=json.dumps(x)) for x in records]
    (spark.createDataFrame(rows)
        .write.format("delta").mode("append")
        .saveAsTable("gh.bronze.raw_airflow_issues"))

print(f"appended {len(records)} records")

# COMMAND ----------

display(spark.sql("""
    select
        count(*)                                as rows,
        count(distinct payload:id::string)      as distinct_ids,
        min(payload:updated_at::timestamp)      as oldest,
        max(payload:updated_at::timestamp)      as newest
    from gh.bronze.raw_airflow_issues
"""))

# COMMAND ----------

display(spark.sql("""
    select
        count(*)                            as rows,
        count(distinct payload:id::string)  as distinct_ids,
        min(payload:updated_at::timestamp)  as oldest,
        max(payload:updated_at::timestamp)  as newest
    from gh.bronze.raw_airflow_issues
"""))