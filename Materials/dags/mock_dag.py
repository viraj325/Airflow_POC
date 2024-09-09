from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

batch_names = ['test', 'demo', 'random']

def batch_job(name) -> str:
    print(f"Performing Batch Job called {name}")
    return f"Batch Job called {name} is successful"

# for name in batch_names:
#     with DAG(dag_id=str(name) + '_dag',
#             start_date=datetime (2023, 10, 11), 
#             schedule_interval=timedelta(hours=24),
#             catchup=False) as dag:
    
#         tasks = {}
#         release = PythonOperator (
#             task_id=f"batch_{name}",
#             python_callable=batch_job, 
#             op_args= [name]
#         )

#         tasks[f"batch_{name}"] = release
#         release.set_upstream(tasks[f"batch_{name}"])