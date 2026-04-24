import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from sqlalchemy import create_engine, text


default_args ={
    "owner" : "Ahmed Nab",
    "depends_on_past" : False,
    "retries" : 1,
    "start_date": datetime(2026,4,24)
}

dag= DAG(
      dag_id='etl_pipeline_dag',
      default_args=default_args,
      description="Extract, transform, and load transactions",
      schedule='@daily',
      catchup=False,
      tags=['etl','local']
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CSV_PATH = PROJECT_ROOT / "include" / "Transactions.csv"
DEFAULT_CLEANED_CSV_PATH = PROJECT_ROOT / "include" / "cleaned_transactions.csv"


def resolve_transactions_path() -> str:
    configured = os.getenv("TRANSACTIONS_CSV_PATH")
    if configured:
        candidate = Path(configured)
    else:
        candidate = DEFAULT_CSV_PATH

    if not candidate.exists():
        raise FileNotFoundError(
            f"Transactions file not found at '{candidate}'. "
            "Set TRANSACTIONS_CSV_PATH to the correct path."
        )
    return str(candidate)


def resolve_cleaned_transactions_path() -> str:
    configured = os.getenv("CLEANED_TRANSACTIONS_CSV_PATH")
    candidate = Path(configured) if configured else DEFAULT_CLEANED_CSV_PATH
    return str(candidate)


def clean_transactions() -> pd.DataFrame:
    filepath = resolve_transactions_path()
    df = pd.read_csv(filepath)
    original_len = len(df)
    report = {}

    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())

    full_dupes = df.duplicated().sum()
    df = df.drop_duplicates()
    report["duplicate_rows_dropped"] = int(full_dupes)

    id_dupes_mask = df.duplicated(subset="TransactionID", keep="first")
    id_dupes = id_dupes_mask.sum()
    report["duplicate_transaction_ids"] = int(id_dupes)
    if id_dupes:
        print(f"{id_dupes} duplicate TransactionID(s) found — keeping first occurrence:")
        print(df[id_dupes_mask][["TransactionID", "AccountID", "Amount"]].to_string(index=False))
    df = df[~id_dupes_mask].copy()

    df["TransactionID"] = df["TransactionID"].astype(int)
    df["AccountID"]     = df["AccountID"].astype(int)
    df["Amount"]        = pd.to_numeric(df["Amount"], errors="coerce")
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"], errors="coerce")

    nulls = df.isnull().sum()
    report["nulls_per_column"] = nulls[nulls > 0].to_dict()
    if report["nulls_per_column"]:
        print(f"Null values detected: {report['nulls_per_column']}")

    invalid_amount_mask = df["Amount"] <= 0
    report["invalid_amounts"] = int(invalid_amount_mask.sum())
    if report["invalid_amounts"]:
        print(f" {report['invalid_amounts']} row(s) with negative or zero Amount:")
        print(df[invalid_amount_mask][["TransactionID", "AccountID", "Amount"]].to_string(index=False))
    df["amount_flag"] = invalid_amount_mask.map({True: "INVALID_AMOUNT", False: ""})

    allowed_types = {"Transfer", "Deposit", "Withdrawal", "Payment"}
    unknown_types = ~df["TransactionType"].isin(allowed_types)
    report["unknown_transaction_types"] = int(unknown_types.sum())
    if report["unknown_transaction_types"]:
        print(f"Unknown TransactionType values: {df.loc[unknown_types, 'TransactionType'].unique()}")

    print("\n── QA Report ─────────────────────────────────────────────────────────")
    print(f"  Rows loaded                   : {original_len:,}")
    print(f"  Duplicate rows dropped        : {report['duplicate_rows_dropped']}")
    print(f"  Duplicate TransactionIDs      : {report['duplicate_transaction_ids']}")
    print(f"  Invalid amounts (≤0)          : {report['invalid_amounts']}")
    print(f"  Null values                   : {report['nulls_per_column'] or 'None'}")
    print(f"  Unknown TransactionTypes      : {report['unknown_transaction_types']}")
    print(f"  Rows after cleaning           : {len(df):,}")
    print("──────────────────────────────────────────────────────────────────────\n")

    output_path = Path(resolve_cleaned_transactions_path())
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Cleaned file written to: {output_path}")

    return str(output_path)


def load_to_sql_server(ti):
    cleaned_path = ti.xcom_pull(task_ids="extract_transform")
    if not cleaned_path:
        raise ValueError("No cleaned file path received from extract_transform task.")

    df = pd.read_csv(cleaned_path)

    server = 'localhost'
    database = 'master'
    username = 'sa'
    password = 'StrongPass123'

    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )

    engine = create_engine(connection_string)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM sys.databases"))
        for row in result:
            print(row.name)

    df.to_sql(
        name="Transactions",
        con=engine,
        schema="trolling",
        if_exists="replace",
        index=False
    )

    print("Data loaded successfully.")


extract_transform=PythonOperator(
    task_id='extract_transform',
    python_callable=clean_transactions,
    dag=dag,
)

load_data=PythonOperator(
    task_id='load_data',
    python_callable=load_to_sql_server,
    dag=dag,
)

extract_transform >> load_data

