from airflow.decorators import dag, task
from pendulum import datetime
# from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# from kubernetes.client import models as k8s

# example_k8_operator = KubernetesPodOperator(
#     namespace="default",
#     image="quay.io/apache/bash",
#     image_pull_secrets=[k8s.V1LocalObjectReference("testquay")],
#     cmds=["bash", "-cx"],
#     arguments=["echo", "10", "echo pwd"],
#     labels={"foo": "bar"},
#     name="airflow-private-image-pod",
#     on_finish_action="delete_pod",
#     in_cluster=True,
#     task_id="task-two",
#     get_logs=True,
# )

#is_retry = "{{dag_run.conf.get('retry', 'false')}}"

def create_dag(dag_id, schedule, dag_number, default_args):
    @dag(dag_id=dag_id, start_date=datetime (2023, 10, 11), schedule=schedule, default_args=default_args, catchup=False)
    def batch_dag():
        @task()
        def batch_job(*args) -> str:
            # example_k8_operator.dry_run()
            print(f"Performing Batch Job called {dag_id}")
            print(f"Batch Job called {dag_number} is successful")

        batch_job()

    generated_dag = batch_dag()

    return generated_dag


# build a dag for each number in range(1, 4)
for n in range(1, 4):
    dag_id = "{}_dag".format(str(n))

    default_args = {"test": "demo", "random": datetime(2023, 7, 1)}

    schedule = "@daily"

    dag_number = n

    globals()[dag_id] = create_dag(dag_id, schedule, dag_number, default_args)