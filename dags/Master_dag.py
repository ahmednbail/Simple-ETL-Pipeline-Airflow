from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow import DAG


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 24),
    "max_active_runs": 1,
    "retries": 1,
}

dag = DAG(
    dag_id="master_transfer_dag",
    description="All transfer DAGs",
    schedule="05 00 * * *",
    concurrency=6,
    max_active_runs=1,
    default_args=default_args,
    tags=["master_transfer"],
    catchup=False,  
)

ext_date = "{{ execution_date }}"

def create_trigger_task(trigger_dag_id):
    return TriggerDagRunOperator(
        task_id=trigger_dag_id,
        dag=dag,
        trigger_dag_id=trigger_dag_id,
        conf={"execution_date": ext_date},
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True,
        trigger_rule="all_done",
    )

with TaskGroup(group_id="pipelines", dag=dag) as pipelines:
    dags = [
        "Extract_transfer_dag",
        "Loading_dag",
    ]
    for task_id in dags:
        create_trigger_task(task_id)
