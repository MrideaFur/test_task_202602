import os
import shutil
import pandas as pd
import pendulum as pm
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


temp_folder = '/tmp/airflow_tmp'

def get_run_path(context):
  run_id = context['run_id']
  return os.path.join(temp_folder, context['dag'].dag_id, run_id)

def cleanup_temp(**context):
  path = get_run_path(context)
  shutil.rmtree(path, ignore_errors=True)
  print(f"Cleaned up: {path}")
  
def extract_sales_data(**context):
  path = get_run_path(context)
  os.makedirs(path, exist_ok=True)
  file_path = os.path.join(path, 'extract.csv')
  
  execution_date = context['execution_date']
  prev_day = execution_date - timedelta(days=1)
  
  pg_hook = PostgresHook(postgres_conn_id='source_db')
  conn = pg_hook.get_conn()
  schema = context['params'].get('schema', 'public')
  query = f"""
    select distinct "order_id", "customer_id", "product_id", "amount", "status", "created_at"
    from "{schema}"."sales"
    where 1=1
      and "created_at" >= %s
      and "created_at" <  %s
      and "status"     =  %s
    ;
  """
  params = [
    prev_day.strftime('%Y-%m-%d'), 
    execution_date.strftime('%Y-%m-%d'), 
    'completed'
  ]
  df = pd.read_sql(query, conn, params=params)
  conn.close()
  
  if df.empty:
    cleanup_temp(**context)
    raise AirflowSkipException(f"No data found for {prev_day} date")

  df.to_csv(file_path, index=False)
  print(f"Extracted {len(df)} rows to {file_path}") 
  
def transform_data(**context):
  path = get_run_path(context)
  input_path = os.path.join(path, 'extract.csv')
  output_path = os.path.join(path, 'transform.csv')
  
  try:
    df = pd.read_csv(input_path)
    if df.empty:
      raise ValueError("Critical Error: Extracted data is empty unexpectedly.")

    daily_stats = df.groupby('customer_id').agg(
      daily_total=('amount', 'sum'),
      daily_orders=('order_id', 'count')
    ).reset_index()
    
    daily_stats['date'] = context['execution_date'].strftime('%Y-%m-%d')
    daily_stats.to_csv(output_path, index=False)
    print(f"Transformed {len(daily_stats)} rows.")
  finally:
    if os.path.exists(input_path):
      os.remove(input_path)

def load_to_dwh(**context):
  path = get_run_path(context)
  input_path = os.path.join(path, 'transform.csv')
  
  try:
    df = pd.read_csv(input_path)
    if df.empty:
      raise ValueError("Critical Error: Transform data is empty unexpectedly.")
    pg_hook = PostgresHook(postgres_conn_id='dwh_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    schema = context['params'].get('schema', 'public')
    query = f"""
      insert into "{schema}"."customer_daily_stats" ("customer_id", "daily_total", "daily_orders", "date")
      values %s
      on conflict ("customer_id", "date") 
      do update set
        "daily_total" = excluded."daily_total",
        "daily_orders" = excluded."daily_orders"
      ;
    """
    execute_values(cursor, query, list(df.itertuples(index=False, name=None)))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(df)} rows.")
  finally:
    if os.path.exists(input_path):
      os.remove(input_path)
    cleanup_temp(**context)


default_args = {
  'owner': 'data_team',
  'start_date': pm.datetime(2023, 10, 1, 0, tz="Europe/Moscow"),
  'on_failure_callback': cleanup_temp,
  'max_active_runs': 5
}

with DAG(
  dag_id = 'sales_pipeline',
  default_args=default_args,
  schedule_interval='@daily',
  # schedule_interval=None,
  catchup=True,
  tags=['sales']
):
  extract_task = PythonOperator(
    task_id='extract_sales_data', 
    python_callable=extract_sales_data, 
    params={
      "schema":'dev'
    }
  )
  transform_task = PythonOperator(
    task_id='transform_data', 
    python_callable=transform_data
  )
  load_task = PythonOperator(
    task_id='load_to_dwh', 
    python_callable=load_to_dwh,
    params={
      "schema":'dev'
    }
  )

extract_task >> transform_task >> load_task #type: ignore
