from airflow import DAG
from datetime import timedelta
from airflow.utils import timezone
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import requests

DAG_FOLDER = "/opt/airflow/dags"

def _get_IQAdata():
    #API_KEY = "2eaaa008-13eb-45d5-ae38-488fe66ecfeb"
    API_KEY = Variable.get("IQA_key")
    url = "https://api.airvisual.com/v2/city"
    payload = {
        "city": "Bangkok",
        "state": "Bangkok",
        "country": "Thailand",
        "key": API_KEY
    }

    response = requests.get(url, params=payload)
    data = response.json()

    with open(f"{DAG_FOLDER}/data.json", "w") as f:
        json.dump(data, f)

def _validate_data():
    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)
    assert data.get("data") is not None

def _create_IQA_table():
    pg_hook = PostgresHook(
        postgres_conn_id="IQA_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    #cursor.execute("DROP TABLE IF EXISTS XXX;");

    sql = """
        CREATE TABLE IF NOT EXISTS IQA (
            ts TIMESTAMP NOT NULL,
            temp FLOAT,
            pres FLOAT,
            humid INT,
            wind_s FLOAT,
            aqi_cn INT,
            m_pollu_cn VARCHAR(5)
        );
    """
    
    cursor.execute(sql)
    connection.commit()

def _load_data_to_postgres():
    pg_hook = PostgresHook(
        postgres_conn_id="IQA_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)

        ts = data["data"]["current"]["weather"]["ts"]
        temp = data["data"]["current"]["weather"]["tp"]
        pres = data["data"]["current"]["weather"]["pr"]
        humid = data["data"]["current"]["weather"]["hu"]
        wind_s = data["data"]["current"]["weather"]["ws"]
        aqi_cn = data["data"]["current"]["pollution"]["aqicn"]
        m_pollu_cn = data["data"]["current"]["pollution"]["maincn"]

    sql = """
        INSERT INTO IQA (ts, temp, pres, humid, wind_s, aqi_cn, m_pollu_cn)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(sql, (ts, temp, pres, humid, wind_s, aqi_cn, m_pollu_cn))
    connection.commit()

default_args = {
    "email": ["chon@odds.team"],
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "Cap_Get_AQI_DAG",
    default_args=default_args,
    schedule="@hourly",
    start_date=timezone.datetime(2025, 2, 1),
    catchup=False,
    tags=["CapS"],
):
    start = EmptyOperator(task_id="start")

    get_IQAdata = PythonOperator(
        task_id="get_IQAdata",
        python_callable=_get_IQAdata,
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    create_IQA_table = PythonOperator(
        task_id= "create_IQA_table",
        python_callable=_create_IQA_table,
    )

    load_data_to_postgres = PythonOperator(
        task_id= "load_data_to_postgres",
        python_callable=_load_data_to_postgres,
    )
    
    send_email = EmailOperator(
        task_id="send_email",
        to=["chon@odds.team"],
        subject="Process Pipline Complete",
        html_content="Done",
    )

    end = EmptyOperator(task_id="end")

    #Flow
    start >> get_IQAdata >> validate_data >> load_data_to_postgres >> send_email 
    start >> create_IQA_table >> load_data_to_postgres 
    send_email >> end