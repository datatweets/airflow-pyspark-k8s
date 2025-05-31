# Creating and Running Your First DAG on Kubernetes with Airflow

Given your infrastructure setup with Airflow running on Kubernetes using the `KubernetesExecutor`, let's walk through creating and understanding your first DAG. This tutorial will focus on the practical aspects of writing DAG code and seeing it execute in your containerized environment.

## Understanding Your Environment

Before we dive into the code, let's understand how your setup differs from a traditional Airflow installation. In your Kubernetes-based setup, Airflow doesn't use a traditional `airflow.cfg` file. Instead, configuration is managed through ConfigMaps and environment variables defined in your Helm templates. This approach aligns with cloud-native practices where configuration is externalized and managed declaratively.

Your DAGs are stored in the `dags/` directory, which is mounted as a volume in your Kubernetes pods. This means when you create or modify a DAG file locally, it becomes available to the Airflow scheduler running inside Kubernetes.

## Creating Your First DAG

Let's create a simple "Hello World" DAG that demonstrates the fundamental concepts. Create a new file called `simple_hello_world.py` in your `dags/` directory:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments define the basic parameters that all tasks in this DAG will inherit
# Think of these as the "house rules" that apply to everyone unless specifically overridden
default_args = {
    'owner': 'data-team',  # Who is responsible for this DAG
    'retries': 1,          # How many times to retry a failed task
    'retry_delay': timedelta(minutes=5),  # Wait time between retries
}

# The DAG object represents your entire workflow
# It's like the blueprint that tells Airflow what to do and when
dag = DAG(
    dag_id='hello_world',  # Unique identifier for your DAG
    description='Our first "Hello World" DAG!',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),  # When the DAG becomes active
    schedule_interval=None,  # No automatic scheduling (manual trigger only)
    catchup=False,  # Don't run for past dates
)

# A task is a single unit of work within your DAG
# BashOperator executes bash commands - perfect for our hello world example
hello_task = BashOperator(
    task_id='hello_world_task',  # Unique identifier within the DAG
    bash_command='echo "Hello from Kubernetes pod!"',
    dag=dag  # Associate this task with our DAG
)

# By referencing the task at the end, we ensure Airflow registers it
hello_task
```

## Understanding the Code Components

Let's break down each part of this code to understand what's happening:

### The Import Statements

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
```

These imports bring in the essential building blocks. The `datetime` and `timedelta` help us work with time-based scheduling. The `DAG` class is the container for our workflow, and `BashOperator` is one of many operators that execute specific types of tasks.

### Default Arguments

The `default_args` dictionary is like setting default preferences that apply to all tasks unless overridden. In your Kubernetes environment, the `retries` parameter is particularly important because pods can occasionally fail to start due to resource constraints or scheduling issues.

### The DAG Definition

When we create the DAG object, we're defining the workflow's metadata and behavior:

- `dag_id`: This must be unique across all your DAGs. It's how Airflow identifies your workflow.
- `start_date`: This tells Airflow when the DAG becomes active. Even with `schedule_interval=None`, this date matters for manual triggers.
- `schedule_interval`: Setting this to `None` means the DAG only runs when manually triggered, which is perfect for testing.
- `catchup`: By setting this to `False`, we prevent Airflow from trying to run the DAG for all dates between `start_date` and now.

### The Task Definition

The `BashOperator` creates a task that runs a bash command. In your Kubernetes setup, this is particularly interesting because each task runs in its own pod. When this task executes, Kubernetes will:

1. Spin up a new worker pod
2. Mount your DAGs directory
3. Execute the bash command
4. Report the results back to the scheduler
5. Terminate the pod

## Running Your DAG

After saving the file, the Airflow scheduler running in your Kubernetes cluster will automatically detect it within about 30 seconds. Here's how to execute it:

1. Access your Airflow UI at `http://localhost:30080`
2. You should see your new `hello_world` DAG in the list
3. Click the toggle switch to unpause the DAG
4. Click the play button and select "Trigger DAG"

## What Happens Behind the Scenes

When you trigger the DAG in your Kubernetes environment, a fascinating sequence of events occurs:

1. The scheduler (running in its own pod) detects the trigger event
2. It creates a job specification for the task
3. The Kubernetes API receives a request to create a new pod
4. Kubernetes schedules the pod on an available node
5. The pod starts with the Airflow image and your mounted DAGs
6. The task executes within the pod
7. Logs are written to the persistent volume
8. The pod reports success/failure back to the scheduler
9. The pod terminates, freeing up resources

## Enhancing Your DAG

Let's modify the DAG to make it more interesting and showcase additional features:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Using the context manager syntax (with statement) is more Pythonic
# It ensures proper cleanup and makes the code cleaner
with DAG(
    dag_id='hello_world_enhanced',
    description='Enhanced Hello World with multiple tasks',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  # Run once per day
    catchup=False,
    tags=['beginner', 'tutorial', 'kubernetes'],  # Tags for organization
) as dag:

    # First task: Print a message
    hello_task = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello from pod: $HOSTNAME"'
    )

    # Second task: Print the current date
    date_task = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    # Third task: A Python function
    def print_context(**context):
        """This function receives the Airflow context and prints execution details"""
        print(f"Running in Kubernetes pod: {context['task_instance'].hostname}")
        print(f"Execution date: {context['execution_date']}")
        print(f"Task: {context['task'].task_id}")
        return "Python task completed successfully!"

    python_task = PythonOperator(
        task_id='python_info',
        python_callable=print_context,
        provide_context=True
    )

    # Define task dependencies using the bit shift operator
    # This creates a pipeline: hello -> date -> python
    hello_task >> date_task >> python_task
```

## Understanding Task Dependencies

The line `hello_task >> date_task >> python_task` creates a dependency chain. This means:
- `date_task` won't start until `hello_task` completes successfully
- `python_task` won't start until `date_task` completes successfully

In Kubernetes terms, this means three separate pods will be created sequentially, each waiting for the previous one to complete.

## Viewing Logs and Debugging

In your Kubernetes setup, logs are particularly important because pods are ephemeral. To view logs:

1. Click on a task square in the Grid view
2. Click "Log" to see the task output
3. Notice the pod name in the logs - each execution uses a different pod

You can also check logs from the command line:

```bash
# View scheduler logs
kubectl logs -f deployment/airflow-scheduler -n airflow

# See all pods including completed ones
kubectl get pods -n airflow --show-all
```

## Best Practices for Your Environment

When working with Airflow on Kubernetes, keep these points in mind:

1. **Resource Awareness**: Each task spawns a new pod, so be mindful of resource requests and limits
2. **Persistent Data**: Use mounted volumes or external storage for data that needs to persist between tasks
3. **Idempotency**: Design tasks to be re-runnable without side effects
4. **Error Handling**: With distributed execution, network issues can occur - always include proper error handling

## Next Steps

Now that you understand the basics, you can:
1. Create more complex DAGs with multiple parallel tasks
2. Use the PythonOperator to run data processing tasks
3. Integrate with your PySpark setup for big data processing
4. Experiment with different operators like KubernetesPodOperator for custom containers

Remember, in your Kubernetes environment, each task is isolated in its own pod, providing excellent scalability and resource management. This architecture allows you to process large amounts of data efficiently while maintaining clean separation between tasks.

The beauty of your setup is that it combines the orchestration power of Airflow with the scalability and resource management of Kubernetes, giving you a robust platform for building complex data pipelines.