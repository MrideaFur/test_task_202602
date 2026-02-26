from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 10, 1),
}

dag = DAG(
    'sales_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=True
)

def extract_sales_data(**context):
    pg_hook = PostgresHook(postgres_conn_id='source_db')
    conn = pg_hook.get_conn()
    execution_date = context['execution_date']
    prev_day = execution_date - timedelta(days=1)

    query = f"""
        SELECT order_id, customer_id, product_id, amount, status, created_at
        FROM sales
        WHERE DATE(created_at) = '{prev_day.strftime('%Y-%m-%d')}'
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    context['ti'].xcom_push(key='sales_data', value=df.to_dict())
    print(f"Extracted {len(df)} rows.")

def transform_data(**context):
    data = context['ti'].xcom_pull(key='sales_data', task_ids='extract_sales_data')
    df = pd.DataFrame.from_dict(data)
    
    if df.empty:
        return
    
    df = df[df['status'] == 'completed']
    
    daily_stats = df.groupby('customer_id').agg({
        'amount': 'sum',
        'order_id': 'count'
    }).reset_index()
    daily_stats.columns = ['customer_id', 'daily_total', 'daily_orders']
    daily_stats['date'] = context['execution_date'].strftime('%Y-%m-%d')
    
    context['ti'].xcom_push(key='transformed_data', value=daily_stats.to_dict())
    print(f"Transformed data for {len(daily_stats)} customers.")

def load_to_dwh(**context):
    pg_hook = PostgresHook(postgres_conn_id='dwh_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    df = pd.DataFrame.from_dict(data)
    
    if df.empty:
        cursor.close()
        conn.close()
        return
    
    for _, row in df.iterrows():
        insert_query = f"""
            INSERT INTO customer_daily_stats (customer_id, date, daily_total, daily_orders)
            VALUES ({row['customer_id']}, '{row['date']}', {row['daily_total']}, {row['daily_orders']})
        """
        cursor.execute(insert_query)
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(df)} rows to DWH.")

extract_task = PythonOperator(
    task_id='extract_sales_data',
    python_callable=extract_sales_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_dwh',
    python_callable=load_to_dwh,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task