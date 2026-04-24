from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from extract_dag import df 
import pandas as pd
from sqlalchemy import create_engine , text

default_args ={
    "owner" : "Ahmed Nab",
    "depends_on_past" : "False",
    "retries" : 1,
    "start_date": datetime(2026,4,24)
}

dag= DAG(
      dag_id='Loading_dag',
      default_args=default_args,
      description="Extract data from local device",
      schedule='@daily',
      catchup=False,
      tags=['Loading','local']
)


def load_to_sql_server(df):

    server = 'localhost'
    database = 'master'
    username = 'sa'
    password = 'StrongPass123'

    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )

    engine = create_engine(connection_string)

    # Test connection
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM sys.databases"))
        for row in result:
            print(row.name)

    # Load DataFrame into SQL Server
    df.to_sql(
        name="Transactions",
        con=engine,
        schema="trolling",
        if_exists="replace",   # change to 'append' in production
        index=False
    )

    print("Data loaded successfully.")




Load_data=PythonOperator(
    task_id='loading_data',
    python_callable= load_to_sql_server(df)
)
