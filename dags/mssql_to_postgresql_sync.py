from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pyodbc

# MSSQL ve PostgreSQL bağlantı bilgilerini girin
mssql_conn = {
    'DRIVER': '{ODBC Driver 17 for SQL Server}',
    'DATABASE': 'Db_test',
    'UID': 'sa',
    'PWD': '12345'
}

postgres_conn = {
    'host': '172.23.24.153',
    'database': 'uyumtest',
    'user': 'uyum',
    'password': '12345'
}

def transfer_data():
    try:
        # MSSQL'e bağlan
        mssql_connection = pyodbc.connect(';'.join([f"{key}={value}" for key, value in mssql_conn.items()]))
        mssql_cursor = mssql_connection.cursor()
        
        # PostgreSQL'e bağlan
        postgres_connection = psycopg2.connect(**postgres_conn)
        postgres_cursor = postgres_connection.cursor()
        
        # MSSQL'den veri al
        mssql_cursor.execute("SELECT * FROM zz_uyumsoft")
        data = mssql_cursor.fetchall()
        
        # Veriyi PostgreSQL'e aktar
        for row in data:
            postgres_cursor.execute("INSERT INTO zz_uyumsoft (deneme, testm) VALUES (%s, %s)", (row[0], row[1]))
        
        # Değişiklikleri kaydet
        postgres_connection.commit()
        
        # Bağlantıları kapat
        mssql_cursor.close()
        mssql_connection.close()
        postgres_cursor.close()
        postgres_connection.close()
        
        print("Veri aktarımı başarıyla tamamlandı.")
        
    except Exception as e:
        print("Veri aktarımı sırasında bir hata oluştu:", str(e))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 19),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'mssql_to_postgres_transfer',
    default_args=default_args,
    description='MSSQL\'den PostgreSQL\'e veri aktarımı',
    schedule_interval=timedelta(seconds=3),
)

transfer_data_task = PythonOperator(
    task_id='transfer_data',
    python_callable=transfer_data,
    dag=dag,
)

transfer_data_task
