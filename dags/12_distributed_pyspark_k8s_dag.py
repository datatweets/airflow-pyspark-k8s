"""Airflow DAG launching a distributed PySpark job on Kubernetes."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="12_distributed_pyspark_k8s_dag",
    default_args=default_args,
    description="Multi-node PySpark execution on Kubernetes",
    schedule_interval=None,
    catchup=False,
    tags=["pyspark", "kubernetes", "distributed"],
) as dag:

    spark_submit_task = BashOperator(
        task_id="spark_submit_distributed",
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master k8s://https://kubernetes.default.svc:443 \
        --deploy-mode cluster \
        --name pyspark-k8s-distributed \
        --conf spark.kubernetes.namespace=default \
        --conf spark.kubernetes.container.image=lotfinejad/airflow-pyspark:latest \
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=airflow-worker \
        --conf spark.executor.instances=2 \
        --conf spark.executor.memory=512m \
        --conf spark.executor.cores=1 \
        --conf spark.driver.memory=512m \
        --conf spark.driver.cores=1 \
        --conf spark.kubernetes.executor.deleteOnTermination=true \
        --conf spark.kubernetes.submission.waitAppCompletion=true \
        --conf spark.sql.shuffle.partitions=4 \
        local:///opt/airflow/scripts/distributed_sales_job.py
        """,
        executor_config={
            "pod_override": {
                "spec": {
                    "serviceAccountName": "airflow-worker",
                    "containers": [
                        {
                            "name": "base",
                            "resources": {
                                "requests": {"memory": "1Gi", "cpu": "500m"},
                                "limits": {"memory": "1Gi", "cpu": "500m"},
                            },
                        }
                    ],
                }
            }
        },
    )
