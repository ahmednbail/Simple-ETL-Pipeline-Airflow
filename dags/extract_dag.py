import pandas as pd 
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


default_args ={
    "owner" : "Ahmed Nab",
    "depends_on_past" : "False",
    "retries" : 1,
    "start_date": datetime(2026,4,24)
}

dag= DAG(

      dag_id='Extract_transfer_dag',
      default_args=default_args,
      description="Extract data from local device",
      schedule='@daily',
      catchup=False,
      tags=['Extract','transfer','local']
)


Path=r"D:\Ahmed\Data engineering - MSC\Simple-ETL-Pipeline-Airflow\Transactions.csv"

def clean_transactions(filepath: str) -> pd.DataFrame:
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

    return df
clean_transactions(Path)

extract_transfer=PythonOperator(
    task_id='extract_transfer',
    python_callable=clean_transactions(Path),
    dag=dag,
)


