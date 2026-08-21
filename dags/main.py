from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from api.animals_data import (
    fetch_raw_animal_data,
    extract_animal_data,
    save_to_json,
)

from datawarehouse.dwh import staging_table, core_table
# from dataquality.soda import yt_elt_data_quality

local_tz = pendulum.timezone("Africa/Johannesburg")

default_args = {
    "owner": "Rachi Huli",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "rachy@gmail.com",
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
}
staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *",
    catchup=False,
) as dag_produce:

    raw_data = fetch_raw_animal_data()
    cleaned_data = extract_animal_data(raw_data)
    save_to_json_task = save_to_json(cleaned_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    raw_data >> cleaned_data >> save_to_json_task

with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    catchup=False,
    schedule=None,
)as dag_update:

    update_staging = staging_table()
    update_core = core_table()

    # trigger_data_quality = TriggerDagRunOperator(
    #     task_id="trigger_data_quality",
    #     trigger_dag_id="data_quality",
    # )

    update_staging >> update_core # >> trigger_data_quality