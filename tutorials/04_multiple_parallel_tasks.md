# Creating Complex DAGs with Multiple Parallel Tasks in Airflow

When you're orchestrating data pipelines, you often need tasks that can run simultaneously rather than waiting for each other. Think of it like cooking a meal - you don't wait for the vegetables to finish roasting before you start boiling the pasta. You run both tasks in parallel to save time. This tutorial will teach you how to create DAGs that leverage parallel execution in your Kubernetes-based Airflow setup.

## Understanding Parallel Execution in Airflow

Before we dive into the code, let's understand what parallel execution means in the context of Airflow and Kubernetes. When tasks run in parallel, multiple Kubernetes pods are created simultaneously, each handling a different piece of work. This is particularly powerful in your setup because Kubernetes can scale these pods across multiple nodes, truly distributing the workload.

Imagine you're processing data from multiple sources - sales data, customer data, and inventory data. Instead of processing them one after another (which could take hours), you can process all three simultaneously and then combine the results. This is the power of parallel task execution.

## Building Our First Parallel DAG

Let's start with a practical example. We'll create a DAG that simulates a data processing pipeline where we extract data from three different sources in parallel, then combine the results. Create a new file called `parallel_processing_dag.py` in your `dags/` directory:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Define our default arguments
default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Create our DAG using the context manager
with DAG(
    dag_id='parallel_data_processing',
    description='Demonstrates parallel task execution',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger for testing
    catchup=False,
    tags=['parallel', 'tutorial'],
) as dag:
    
    # Start task - this represents the beginning of our pipeline
    # DummyOperator is perfect for creating structure without actual processing
    start = DummyOperator(
        task_id='start_pipeline',
    )
    
    # Three parallel extraction tasks
    # Each simulates extracting data from a different source
    extract_sales = BashOperator(
        task_id='extract_sales_data',
        bash_command='echo "Extracting sales data..."; sleep 5; echo "Sales data extracted!"',
    )
    
    extract_customers = BashOperator(
        task_id='extract_customer_data',
        bash_command='echo "Extracting customer data..."; sleep 7; echo "Customer data extracted!"',
    )
    
    extract_inventory = BashOperator(
        task_id='extract_inventory_data',
        bash_command='echo "Extracting inventory data..."; sleep 3; echo "Inventory data extracted!"',
    )
    
    # Process the extracted data - also in parallel
    process_sales = BashOperator(
        task_id='process_sales_data',
        bash_command='echo "Processing sales data..."; sleep 2; echo "Sales processing complete!"',
    )
    
    process_customers = BashOperator(
        task_id='process_customer_data',
        bash_command='echo "Processing customer data..."; sleep 3; echo "Customer processing complete!"',
    )
    
    process_inventory = BashOperator(
        task_id='process_inventory_data',
        bash_command='echo "Processing inventory data..."; sleep 2; echo "Inventory processing complete!"',
    )
    
    # Combine all processed data
    combine_data = BashOperator(
        task_id='combine_all_data',
        bash_command='echo "Combining all processed data..."; sleep 2; echo "Data combination complete!"',
    )
    
    # End task
    end = DummyOperator(
        task_id='end_pipeline',
    )
    
    # Define the task dependencies
    # This is where we create the parallel execution pattern
    start >> [extract_sales, extract_customers, extract_inventory]
    
    # Each extract task leads to its corresponding process task
    extract_sales >> process_sales
    extract_customers >> process_customers
    extract_inventory >> process_inventory
    
    # All process tasks must complete before combining
    [process_sales, process_customers, process_inventory] >> combine_data
    
    # Finally, end the pipeline
    combine_data >> end
```

## Understanding the Code Structure

Let me walk you through what's happening in this DAG, as there are several important concepts at play here.

### The DummyOperator

We're using `DummyOperator` for our start and end tasks. Think of these as markers or milestones in your pipeline. They don't perform any actual work but help organize the flow visually and logically. In complex pipelines, these markers make it easier to understand where parallel branches begin and end.

### Creating Parallel Branches

The magic happens in this line:
```python
start >> [extract_sales, extract_customers, extract_inventory]
```

By using a list on the right side of the `>>` operator, we're telling Airflow that all three extraction tasks can run simultaneously once the start task completes. In your Kubernetes environment, this means three pods will be created at the same time, each running independently.

### The Sleep Commands

I've added `sleep` commands with different durations to simulate real-world scenarios where tasks take varying amounts of time. This helps you visualize how parallel execution works - the faster tasks will complete while others are still running. For example, the inventory extraction takes 3 seconds while customer extraction takes 7 seconds, but they both start at the same time.

### Convergence Points

Look at this line:
```python
[process_sales, process_customers, process_inventory] >> combine_data
```

This creates what we call a convergence point. The `combine_data` task will wait for ALL three processing tasks to complete before it starts. This is crucial when you need to gather results from parallel operations before proceeding.

## Another Pattern: Simple Fan-Out

Let's look at another common pattern where one task triggers multiple parallel tasks that don't need to converge. Create a file called `notification_dag.py`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='parallel_notifications',
    description='Send notifications to multiple channels in parallel',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['parallel', 'notifications'],
) as dag:
    
    # Generate a report
    generate_report = BashOperator(
        task_id='generate_report',
        bash_command='echo "Generating daily report..."; sleep 3; echo "Report complete!"',
    )
    
    # Send notifications to different channels - all in parallel
    email_notification = BashOperator(
        task_id='send_email',
        bash_command='echo "Sending email notification..."; sleep 2; echo "Email sent!"',
    )
    
    slack_notification = BashOperator(
        task_id='send_slack_message',
        bash_command='echo "Posting to Slack..."; sleep 1; echo "Slack message posted!"',
    )
    
    teams_notification = BashOperator(
        task_id='send_teams_message',
        bash_command='echo "Posting to Teams..."; sleep 1; echo "Teams message posted!"',
    )
    
    update_dashboard = BashOperator(
        task_id='update_dashboard',
        bash_command='echo "Updating dashboard..."; sleep 3; echo "Dashboard updated!"',
    )
    
    # After report is generated, send all notifications in parallel
    generate_report >> [email_notification, slack_notification, 
                       teams_notification, update_dashboard]
```

In this pattern, once the report is generated, all four notification tasks run simultaneously. They don't need to wait for each other, and there's no convergence point. Each notification is independent.

## Visualizing Parallel Execution

When you trigger these DAGs and view them in the Graph view, you'll see different patterns:

For the first DAG (parallel_data_processing), you'll see a pattern where tasks spread out and then converge. The visualization clearly shows which tasks can run simultaneously and where they must wait for others.

For the second DAG (parallel_notifications), you'll see a fan-out pattern where one task branches into multiple parallel tasks that never reconverge.

## What Happens in Your Kubernetes Cluster

Understanding what happens behind the scenes helps you appreciate the power of parallel execution. When you trigger the `parallel_data_processing` DAG:

1. The scheduler creates a pod for the `start_pipeline` task
2. Once that completes, Kubernetes receives requests to create THREE pods simultaneously for the extraction tasks
3. Each extraction pod runs independently - if one fails, the others continue
4. As each extraction completes, a new pod is created for the corresponding processing task
5. The `combine_data` task waits until all three processing pods complete successfully
6. Finally, the `end_pipeline` pod is created

This means at peak execution, you might have multiple pods running across your Kubernetes cluster, truly distributing the workload.

## Running and Monitoring Your Parallel DAG

To see parallel execution in action:

1. Save the DAG file in your `dags/` directory
2. Access your Airflow UI at `http://localhost:30080`
3. Find your DAG and unpause it
4. Trigger the DAG manually
5. Quickly switch to the Graph view
6. Watch as tasks change colors - you'll see multiple tasks turn green simultaneously

You can also monitor from the command line:
```bash
# Watch pods being created and terminated in real-time
kubectl get pods -n airflow -w
```

## Best Practices for Parallel Tasks

When designing parallel tasks, keep these principles in mind:

**Task Independence**: Parallel tasks should be truly independent. If Task A needs data from Task B, they cannot run in parallel. Design your tasks so each can complete without waiting for others in the same parallel group.

**Resource Awareness**: Each parallel task creates a new pod. If you have 10 parallel tasks, you'll have 10 pods running simultaneously. Make sure your Kubernetes cluster has enough resources.

**Clear Naming**: Use descriptive task IDs that indicate what each parallel branch does. This makes debugging much easier when viewing logs or the graph.

**Appropriate Timeouts**: Set reasonable timeouts for tasks, especially when running many in parallel. One hung task shouldn't block your entire pipeline.

## Conclusion

Parallel task execution transforms how you build data pipelines. Instead of processing data sequentially, which can take hours, you can leverage Kubernetes to run multiple tasks simultaneously, dramatically reducing overall execution time.

The key concepts to remember are:
- Use lists with the `>>` operator to create parallel dependencies
- Tasks in the same list will run simultaneously
- Use lists on the left side of `>>` to create convergence points
- Each parallel task runs in its own Kubernetes pod
- Design tasks to be independent when running in parallel

As you build your data pipelines, look for opportunities to parallelize work. Any time you have multiple independent operations - like reading from different data sources, sending multiple notifications, or processing separate datasets - you can use parallel execution to make your pipelines faster and more efficient.

The combination of Airflow's orchestration capabilities and Kubernetes' scalability gives you a powerful platform for building sophisticated data workflows that can handle large-scale data processing efficiently.