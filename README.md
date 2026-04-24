# Week 6 Task: Intro to Airflow - Simple ETL Pipeline

Build a simple ETL pipeline using Apache Airflow to understand how workflows are scheduled and managed.

---

## Objective

Create and orchestrate a simple ETL pipeline with Apache Airflow instead of running a standalone Python script manually.

## Scenario

You have a small dataset (Banking or E-commerce) stored as a CSV file.  
You need to automate a pipeline that:

1. Reads the data
2. Cleans it
3. Loads it into a database or output file

---

## Tasks

### Task 1: Airflow Setup

Students should:

- Install Airflow (local install or Docker preferred)
- Run the Airflow web UI
- Verify the scheduler is working

**Deliverable:**
- Screenshot of Airflow UI with the **DAGs** tab visible

### Task 2: Create Your First DAG

Create a DAG named:

`simple_etl_pipeline`

DAG must include:

- `schedule_interval` (daily or manual)
- `start_date`
- `retries`

### Task 3: Define ETL Tasks

Create **3 tasks** using `PythonOperator`:

1. **Extract Task**
   - Read CSV file using `pandas`
   - Print number of records

2. **Transform Task**
   - Clean data:
     - Remove null values
     - Fix data types
   - Add one new column (example: `total_amount` or a `status_flag`)

3. **Load Task**
   - Save cleaned data:
     - either to a new CSV
     - OR insert into SQLite / SQL Server

### Task 4: Set Task Dependencies

Pipeline flow must be:

`extract -> transform -> load`

### Task 5: Run and Monitor

- Trigger DAG manually
- Check task status in Airflow UI
- Ensure all tasks complete successfully

**Deliverable:**
- Screenshot of a successful DAG run

---

## Final Submission Checklist

Students must submit:

1. Airflow DAG file (`.py`)
2. Dataset used (`.csv`)
3. Output file or database result
4. Screenshots:
   - Airflow UI
   - Successful DAG execution

---

## Suggested Project Structure (Optional)

```text
Simple-ETL-Pipeline-Airflow/
|-- dags/
|   |-- simple_etl_pipeline.py
|-- data/
|   |-- input.csv
|-- output/
|   |-- cleaned_data.csv
|-- README.md
```

## Notes

- Keep task functions modular and easy to test.
- Use logging/print statements to validate each ETL stage.
- If using Docker, ensure both webserver and scheduler containers are running.