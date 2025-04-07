from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import json
import requests

DAG_FOLDER = "/opt/airflow/dags"

def _get_AQIdata():
    API_KEY = "2eaaa008-13eb-45d5-ae38-488fe66ecfeb"
    url = "https://api.airvisual.com/v2/city"
    payload = {
        "city": "Bangkok",
        "state": "Bangkok",
        "country": "Thailand",
        "key": "2eaaa008-13eb-45d5-ae38-488fe66ecfeb"
    }

    response = requests.get(url, params=payload)
    data = response.json()

    with open(f"{DAG_FOLDER}/data.json", "w") as f:
        json.dump(data, f)


with DAG(
    "Cap_Get_AQI_DAG",
    schedule=None,
    start_date=timezone.datetime(2025, 3, 1),
    catchup=False,
    tags=["CapS"],
):
    start = EmptyOperator(task_id="start")

    get_AQIdata = PythonOperator(
        task_id="get_AQIdata",
        python_callable=_get_AQIdata,
    )

    end = EmptyOperator(task_id="end")

    #flow
    start >> get_AQIdata >> end