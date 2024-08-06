from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

k = KubernetesPodOperator(
    name="hello-dry-run",
    image="debian",
    cmds=["bash", "-cx"],
    arguments=["echo", "10"],
    labels={"foo": "bar"},
    task_id="dry_run_demo",
    do_xcom_push=True,
)

quay_k8s = KubernetesPodOperator(
    namespace="default",
    image="quay.io/apache/bash",
    image_pull_secrets=[k8s.V1LocalObjectReference("testquay")],
    cmds=["bash", "-cx"],
    arguments=["echo", "10", "echo pwd"],
    labels={"foo": "bar"},
    name="airflow-private-image-pod",
    on_finish_action="delete_pod",
    in_cluster=True,
    task_id="task-two",
    get_logs=True,
)

def testK8() -> bool:
    k.dry_run()
    return True

with DAG(dag_id="test_dag",
         start_date=datetime(2023,10,11),
         schedule_interval=timedelta(hours=24),
         catchup=False) as dag:
    
    testK8()