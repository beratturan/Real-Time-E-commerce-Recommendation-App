import json
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
import mysql.connector
from airflow.utils.dates import days_ago
import psycopg2

def start_dag():
    print('DAG started! at ', datetime.now())


def extract_and_transform(**context):

    # create connection to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="data-db",
        user="postgres",
        password="123456"
    )

    # create cursor
    cur = conn.cursor()

    # execute SQL query
    cur.execute("SELECT p.product_id, COUNT(DISTINCT o.user_id) AS distinct_users_count, CAST(SUM(oi.quantity) AS INTEGER) AS total_quantity \
                    FROM products p \
                    JOIN order_items oi ON oi.product_id = p.product_id \
                    JOIN orders o ON oi.order_id = o.order_id \
                    GROUP BY p.product_id \
                    ORDER BY distinct_users_count DESC \
                    LIMIT 10;")

    # fetch all rows
    rows = cur.fetchall()

    for i in rows:
        print(i)
    # close cursor and connection
    cur.close()
    conn.close()

    task_instance = context['task_instance']
    task_instance.xcom_push(key = 'bestsellers', value=rows)

  
def load_to_db(**kwargs):


    conn = mysql.connector.connect(
        host="mysql",
        database="airflow_db",
        user="admin",
        password="admin"
    )
    cursor = conn.cursor()

    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids='extract_and_transform', key='bestsellers')



    for row in data:

        sql = "INSERT INTO non_pers_bestseller (productid, user_count,product_count)  \
                VALUE (%s, %s, %s) \
                ON DUPLICATE KEY UPDATE user_count = '%s' , product_count = '%s'"
               
        values = (row[0], int(row[1]),int(row[2]),int(row[1]),int(row[2]))
        cursor.execute(sql, values)
    
    conn.commit()
    cursor.close()
    conn.close()

def end_dag():
    print('DAG ended! at ', datetime.now())

dag_bestseller = DAG(
    dag_id='Bestsellers',
    schedule_interval=None,
    start_date=datetime(2023, 3, 22),
    dagrun_timeout=timedelta(minutes=60),
    description='Calculate the Best Seller products and write to the table in mysql db',
    catchup=False,
    
)


start_task = PythonOperator(
    task_id='start_dag',
    python_callable=start_dag,
    dag=dag_bestseller
)

extract_and_transform_task = PythonOperator(
    task_id='extract_and_transform',
    python_callable=extract_and_transform,
    dag=dag_bestseller
)

load_to_db_task = PythonOperator(
    task_id='load_to_db',
    python_callable=load_to_db,
    dag=dag_bestseller
)


end_task = PythonOperator(
    task_id='end_dag',
    python_callable=end_dag,
    dag=dag_bestseller
)



start_task >> extract_and_transform_task >> load_to_db_task >> end_task



