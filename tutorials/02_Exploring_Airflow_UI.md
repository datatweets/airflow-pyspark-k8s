# Navigating and Exploring the Airflow UI
##### Mohammad Mehdi Lotfinejad
## Introduction

This tutorial will guide you through the Apache Airflow UI, showing you how to navigate, monitor, and debug your data pipelines. We'll explore the main features using example DAGs provided by Airflow.

------

## 1. Accessing the Airflow UI

### Steps:

1. Ensure your Airflow scheduler and webserver are running
2. Open your web browser and navigate to `http://localhost:8080` or `http://localhost:30080`
3. You'll see the Airflow login page. 
4. Enter your administrator username and password
5. Click "Sign In"

<img width="1512" alt="Screenshot 2025-05-31 at 4 11 18 PM" src="https://github.com/user-attachments/assets/484ec429-e2c1-4762-95fb-9cb8e471aa32" />

------

## 2. Understanding the DAGs Dashboard

### What You'll See:

After logging in, you'll be taken to the main DAGs list page showing all available DAGs.

### Key Elements:

- **DAG List**: All available DAGs with their current status
- **Warning Messages**: Pay attention to messages at the top, if any. 
- **DAG Import Errors**: Shows if any DAGs failed to import
- **System Warnings**: SQLite and SequentialExecutor warnings (normal for development)

### Common Warnings You Might See:

```
⚠️ DAG Import Error - Missing dependencies (e.g., Pandas not installed)
⚠️ Do not use SQLite as metadata DB in production
⚠️ Do not use SequentialExecutor in production
```
<img width="1512" alt="Screenshot 2025-05-31 at 4 13 20 PM" src="https://github.com/user-attachments/assets/ec529f0e-8292-40c1-bb14-8dd73b552c3e" />


------

## 3. Understanding DAG Status Indicators

### Run Status Circles:

Each DAG shows circular indicators representing:

- **Dark Green circle**: Successful runs
- **Red circle**: Failed runs
- **Light Green circle**: Running
- **Gray circle**: Queued
- **Numbers inside**: Count of runs in that status

### Task Status Indicators:

Small circles on the right show individual task statuses:

- Scheduled
- Failed
- Queued
- Running
- Success





------

## 4. Executing Your First DAG

### Steps to Run a DAG:

1. Find `simple_hello_world` in the DAG list
2. Notice it's paused by default (toggle switch is off)
3. Click the toggle switch to unpause the DAG
4. Observe the schedule: `none`
5. See the green success indicator appear

<img width="1512" alt="Screenshot 2025-05-31 at 4 15 41 PM" src="https://github.com/user-attachments/assets/3f3faf68-4602-4d80-8f3e-84e12c6f7890" />



------

## 5. Exploring DAG Details

### Accessing Detailed View:

1. Click on the DAG name (`simple_hello_world`)
2. You'll see the detailed DAG view

### Key Components:

- Left Panel

  Lists all tasks in execution order; this DAG only has one task.

  - `simple_hello_world_task`

- **Top Bar**: Shows execution duration

- Task Squares: Visual representation of task status

  - Green = Success
  - Pink = Skipped (due to branching)

- **Legend**: Color codes for all possible task states


<img width="1512" alt="Screenshot 2025-05-31 at 4 17 52 PM" src="https://github.com/user-attachments/assets/523b14b9-bf6c-416d-a949-20b459b5b534" />



------

## 6. Manually Triggering DAG Runs

### How to Trigger:

1. Click the Play button (▶️) in the DAG view
2. Observe the new run appearing



------

## 7. Debugging and Monitoring

### Viewing Run Details:

1. Hover over any run for quick info
2. Click on a run to see detailed information:
   - Scheduled time
   - Actual execution time
   - Run type (manual/scheduled)
   - Duration

### Viewing Task Details:

1. Hover over task boxes for quick status
2. Click on a task for detailed view:
   - Task instance details
   - Task actions
   - Access to logs



------

## 8. Accessing Task Logs

### Steps:

1. Click on any task box
2. Click "Log" in the task details
3. View complete execution logs

### Why Logs Matter:

- See exact execution details
- Debug failures and exceptions
- Understand task behavior
- Track data processing steps

<img width="1512" alt="Screenshot 2025-05-31 at 4 20 34 PM" src="https://github.com/user-attachments/assets/c1c17593-245c-4215-b4d5-d74b2a7b834b" />


------

## 9. Graph View Visualization

### Accessing Graph View:

1. Click "Graph" tab in the DAG view
2. See visual representation of your pipeline

### What It Shows:

- Task dependencies as connected nodes
- Execution flow with arrows
- Task status by outline color:
  - Green outline = Executed successfully
  - Pink outline = Skipped
- Task type by fill color (see legend)

### Benefits:

- Quickly understand pipeline structure
- Debug dependency issues
- Visualize execution paths


<img width="1512" alt="Screenshot 2025-05-31 at 4 20 51 PM" src="https://github.com/user-attachments/assets/d7a8a3ff-f4df-4cbe-8203-82f94c3786d6" />


------

## 10. Calendar View

### Accessing Calendar:

1. Click "Calendar" tab
2. See monthly/yearly execution history

### What It Shows:

- Green boxes: Successful execution days
- Red boxes: Failed execution days
- Easy way to track execution patterns


<img width="1512" alt="Screenshot 2025-05-31 at 4 21 08 PM" src="https://github.com/user-attachments/assets/6271a0c9-41f2-4755-a28a-6f28b583206e" />


------

## Summary

The Airflow UI provides powerful tools for:

- **Monitoring**: Real-time status of DAGs and tasks
- **Debugging**: Detailed logs and execution history
- **Control**: Manual triggering and scheduling
- **Visualization**: Graph and calendar views

### Pro Tips:

1. Always check for import errors at the top
2. Use the Graph view to verify your DAG structure
3. Check logs immediately when tasks fail
4. Use manual triggers for testing
5. Monitor the calendar view for execution patterns

### Next Steps:

- Create your own DAGs
- Experiment with different operators
- Set up email alerts for failures
- Configure production-ready executors
