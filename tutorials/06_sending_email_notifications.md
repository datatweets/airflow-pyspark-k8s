# Sending Email Notifications from Airflow DAGs Using Gmail

Email notifications are a crucial part of any production data pipeline. When your DAG completes successfully at 3 AM, you want to know about it. When it fails, you definitely want to know about it. In this tutorial, we'll learn how to set up Gmail to work with Airflow and create a DAG that sends email notifications. We'll build a practical example that collects information about your Python environment and emails it to you.

## Why Email Notifications Matter in Airflow

Think of email notifications as your pipeline's way of talking to you. Without them, you'd have to constantly check the Airflow UI to see if your DAGs are running correctly. That's not practical, especially for pipelines that run overnight or on weekends.

Email notifications serve three main purposes in Airflow:
 
- First, they provide immediate alerts when something goes wrong, allowing you to respond quickly to failures. 
- Second, they can deliver summaries of successful runs, giving you confidence that your data is being processed correctly. 
- Third, they enable team collaboration by keeping everyone informed about pipeline status without requiring constant manual monitoring.

## Understanding Gmail's Security: Why We Need App Passwords

Here's something that might surprise you: Gmail won't let you use your regular password to send emails from scripts or applications like Airflow. This is actually a good thing – it's a security feature that protects your account. Instead, Gmail requires you to create something called an "App Password" specifically for each application that needs to send email.

An App Password is a 16-character code that works only for one specific purpose. If someone somehow gets hold of this password, they can only use it to send emails – they can't change your account settings, read your emails, or access your Google Drive. It's like giving someone a key that only opens one specific door in your house, not the master key.

To create an App Password, you first need to enable Two-Factor Authentication (2FA) on your Google account. This adds an extra layer of security by requiring both your password and a verification code from your phone when you log in.

## Setting Up Your Gmail Account for Airflow

Let's walk through the process of preparing your Gmail account step by step. I'll explain what's happening at each stage so you understand not just what to do, but why you're doing it.

### Step 1: Enable Two-Factor Authentication

First, log into your Gmail account in a web browser. Click on your profile picture in the top-right corner and select "Manage your Google Account." This takes you to your account settings.

In the left sidebar, click on "Security." This is where Google keeps all the security-related settings for your account. Scroll down until you find a section called "Signing in to Google." Look for "2-Step Verification" and click on it.

Google will guide you through the setup process. You'll need to provide your phone number and choose how you want to receive verification codes – either by text message or through the Google Authenticator app. Follow the prompts until you see "2-Step Verification: On" in your security settings.

### Step 2: Create an App Password

Now that 2FA is enabled, you can create App Passwords. Still in the Security section of your Google Account, look for "App passwords" under "Signing in to Google." Click on it.

Google might ask you to confirm your password again – this is normal and adds another layer of security. Once you're in the App passwords page, you'll see a field that says "App name." This is where you'll give your App Password a descriptive name.

Type something meaningful like "Airflow Email DAG" or "Kubernetes Airflow SMTP." This name is just for your reference – it helps you remember what this password is for if you need to manage it later. Click "Create."
<img width="1512" alt="Screenshot 2025-05-31 at 9 42 09 PM" src="https://github.com/user-attachments/assets/adbdc700-cd62-466b-bd08-fa5d4085072a" />

### Step 3: Save Your App Password

This is the most important part: Google will now show you a 16-character password. It will look something like this:
```
btxt jksl rzve wabl
```

Copy this password exactly as shown. You can include the spaces or remove them – Gmail will accept either format. This is the ONLY time Google will show you this password. Once you click "Done," you cannot see it again. If you lose it, you'll need to delete this App Password and create a new one.

I recommend copying this password to a secure password manager immediately. You'll need it in just a moment when we set up our Airflow DAG.

## Storing Your App Password in Airflow

Now we need to tell Airflow about your Gmail App Password. In our Kubernetes-based setup, we'll use Airflow Variables to store this password. This keeps it out of your code and makes it easy to update if needed.

Open your Airflow UI at `http://localhost:30080`. Navigate to Admin → Variables in the top menu. Click the "+" button to create a new variable.

Set the Key to `gmail_app_password` and paste your 16-character App Password as the Value. Click Save. Airflow will now store this password securely and make it available to your DAGs.

## Building the Email Notification DAG

Now let's create a DAG that demonstrates email notifications. We'll build something practical: a DAG that collects information about your Python environment and emails it to you. This is useful for documenting what packages are installed in your Airflow workers.

Create a new file called `email_notification_dag.py` in your `dags/` directory:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import sys
import pkg_resources
import platform
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Email configuration - replace these with your actual email addresses
SENDER_EMAIL = "your-gmail-address@gmail.com"  # Replace with your Gmail address
RECEIVER_EMAIL = "recipient@example.com"  # Replace with where you want to receive emails

# Get the App Password from Airflow Variables
# This is more secure than hardcoding it in your DAG
SENDER_PASSWORD = Variable.get("gmail_app_password")

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def collect_environment_info(**context):
    """
    This function collects information about the Python environment
    running in your Airflow worker pods. It's a practical example
    of gathering data that you might want to email to yourself.
    """
    # Collect basic Python information
    python_info = {
        'version': sys.version,
        'implementation': platform.python_implementation(),
        'platform': platform.platform(),
        'executable': sys.executable
    }
    
    # Check for important data processing packages
    important_packages = [
        'pandas', 'numpy', 'apache-airflow', 'pyspark',
        'sqlalchemy', 'psycopg2', 'boto3', 'kubernetes'
    ]
    
    installed_packages = {}
    missing_packages = []
    
    # Check which packages are installed and their versions
    for package in important_packages:
        try:
            dist = pkg_resources.get_distribution(package)
            installed_packages[package] = dist.version
        except pkg_resources.DistributionNotFound:
            missing_packages.append(package)
    
    # Build a nicely formatted summary
    summary_lines = [
        "=== Python Environment Report ===",
        f"\nGenerated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"\nPython Version: {python_info['version']}",
        f"Platform: {python_info['platform']}",
        f"Implementation: {python_info['implementation']}",
        "\n=== Installed Packages ===",
    ]
    
    for package, version in installed_packages.items():
        summary_lines.append(f"✓ {package}: {version}")
    
    if missing_packages:
        summary_lines.append("\n=== Missing Packages ===")
        for package in missing_packages:
            summary_lines.append(f"✗ {package}: Not installed")
    
    summary = "\n".join(summary_lines)
    
    # Print to Airflow logs so you can see it there too
    print(summary)
    
    # Push to XCom so the email task can retrieve it
    context['task_instance'].xcom_push(key='environment_summary', value=summary)
    
    return "Environment info collected successfully"

def send_email_notification(**context):
    """
    This function retrieves the environment summary and sends it via email.
    It demonstrates how to use Gmail's SMTP server with an App Password.
    """
    # Retrieve the summary from the previous task
    task_instance = context['task_instance']
    summary = task_instance.xcom_pull(key='environment_summary', task_ids='collect_environment')
    
    if not summary:
        raise ValueError("No environment summary found! Did the previous task fail?")
    
    # Create the email message
    message = MIMEMultipart()
    message['From'] = SENDER_EMAIL
    message['To'] = RECEIVER_EMAIL
    message['Subject'] = f"Airflow Environment Report - {datetime.now().strftime('%Y-%m-%d')}"
    
    # Add the summary as the email body
    body = f"""
Hello,

This is an automated report from your Airflow DAG running on Kubernetes.
Below is the current Python environment configuration:

{summary}

This email was sent from DAG: email_notification_dag
Task: send_email_notification

Best regards,
Your Airflow Pipeline
"""
    
    message.attach(MIMEText(body, 'plain'))
    
    # Connect to Gmail and send the email
    try:
        # Create SMTP session
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()  # Enable TLS encryption
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        
        # Send the email
        text = message.as_string()
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, text)
        server.quit()
        
        print(f"Email sent successfully to {RECEIVER_EMAIL}")
        return "Email sent successfully"
        
    except Exception as e:
        print(f"Failed to send email: {str(e)}")
        raise

# Create the DAG
with DAG(
    dag_id='email_notification_dag',
    description='Collect Python environment info and send via email',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['email', 'notification', 'tutorial'],
) as dag:
    
    # Task 1: Collect environment information
    collect_task = PythonOperator(
        task_id='collect_environment',
        python_callable=collect_environment_info,
        provide_context=True,
    )
    
    # Task 2: Send email with the collected information
    email_task = PythonOperator(
        task_id='send_email',
        python_callable=send_email_notification,
        provide_context=True,
    )
    
    # Set up the dependency: collect first, then send
    collect_task >> email_task
```

## Understanding the Code

Let me walk you through what this DAG does and why each part is important.

The DAG consists of two tasks that work together. The first task, `collect_environment`, gathers information about the Python environment running in your Airflow worker pod. This includes the Python version, platform details, and which important packages are installed. This information is genuinely useful – it helps you understand exactly what's available in your Airflow environment.

The second task, `send_email`, takes this information and sends it to you via email. It uses Gmail's SMTP server with the App Password you created earlier. The email includes a timestamp and is formatted to be easy to read.

Notice how we're using `Variable.get("gmail_app_password")` to retrieve your App Password. This is much more secure than putting the password directly in your code. If you share this DAG file with others, they won't see your password.

The email sending process uses Python's built-in `smtplib` library. We connect to Gmail's SMTP server at `smtp.gmail.com` on port 587, enable TLS encryption for security, and then authenticate with your email address and App Password.

## Testing Your Email DAG

Before running the DAG, make sure you've updated these two lines with your actual email addresses:
```python
SENDER_EMAIL = "your-gmail-address@gmail.com"  # Your Gmail address
RECEIVER_EMAIL = "recipient@example.com"  # Where to send the email
```

Save the file in your `dags/` directory. The Airflow scheduler will pick it up within a minute or two. 

In the Airflow UI, find `email_notification_dag` in your list of DAGs. If it's paused (which it probably is by default), click the toggle to unpause it. Then click the play button to trigger a manual run.

Click on the DAG to see its progress. You should see the `collect_environment` task turn green first, followed by `send_email`. If both tasks complete successfully, check the inbox of your RECEIVER_EMAIL address. You should have received an email with your Python environment details!

## Troubleshooting Common Issues

If your email task fails, here are the most common causes and solutions.

**Authentication Failed Error**: This usually means your App Password isn't correct. Double-check that you copied it exactly from Google, and that you saved it correctly in Airflow Variables. Remember, the password might have spaces in it.

**Connection Timeout**: Your Kubernetes cluster might be blocking outbound connections on port 587. This is common in corporate environments. You'll need to ask your infrastructure team to allow outbound SMTP connections.

**Email Not Received**: Check your spam folder first. Gmail sometimes marks automated emails as spam, especially when you're first setting things up. Also verify that your RECEIVER_EMAIL is spelled correctly.

## Adding Email Notifications to Your Existing DAGs

Now that you understand how email notifications work, you can add them to any of your existing DAGs. Here's a simple pattern you can follow:

```python
def task_success_email(**context):
    """Send an email when important tasks complete"""
    task_instance = context['task_instance']
    dag_run = context['dag_run']
    
    subject = f"DAG {dag_run.dag_id} completed successfully"
    body = f"""
    Good news! Your DAG has completed successfully.
    
    DAG: {dag_run.dag_id}
    Execution Date: {dag_run.execution_date}
    Task: {task_instance.task_id}
    
    All systems are running smoothly.
    """
    
    # Use the same email sending code from our example
    # ... (SMTP connection and sending logic)
```

You can create different email functions for different scenarios – one for successes, one for failures, one for sending data quality reports, and so on.

## Best Practices for Email Notifications

As you implement email notifications in your production DAGs, keep these principles in mind.

First, be selective about what triggers an email. If every single task sends an email, you'll quickly overwhelm your inbox and start ignoring them. Reserve emails for truly important events – failures, end-of-pipeline summaries, or critical data quality alerts.

Second, make your email subjects descriptive. Include the DAG name, the date, and whether it's a success or failure. This makes it easy to filter and search your emails later.

Third, consider using email templates for consistent formatting. You can create a Python function that generates nicely formatted HTML emails with your company's branding, making your notifications look professional.

Finally, remember that your App Password is a credential that needs to be protected. In a production environment, consider using Airflow Connections instead of Variables, as they provide better security features. You might also want to create a dedicated Gmail account just for sending automated notifications, rather than using your personal email.

## Conclusion

Email notifications transform Airflow from a silent background worker into a communicative partner in your data pipeline operations. With the Gmail App Password approach we've learned, you can easily add email notifications to any DAG, keeping yourself and your team informed about pipeline status without constant manual monitoring.

The example we built – collecting and emailing Python environment information – is just the beginning. You can adapt this pattern to send data quality reports, alert on specific business conditions, or provide daily summaries of processed data. The combination of Airflow's orchestration power and email's universal accessibility creates a robust notification system for your data pipelines.

*Remember, the goal of email notifications is to give you peace of mind. When your pipelines run successfully, you know about it. When they fail, you know about it immediately. This visibility is crucial for maintaining reliable data operations, especially in a Kubernetes environment where pods come and go dynamically.*

---
## Appendix: Adding Multiple Recipients to Your Email Notifications

Now that you have email notifications working, you might be wondering: "What if I need to send the same notification to multiple people?" This is a common requirement in team environments where several people need to stay informed about pipeline status. Let me show you how to extend our email DAG to handle multiple recipients.

The good news is that Python's email handling makes this straightforward. Instead of having a single `RECEIVER_EMAIL`, you can create a list of email addresses and send to all of them at once. Here's how to modify the code we wrote earlier:

```python
# Instead of a single receiver:
# RECEIVER_EMAIL = "recipient@example.com"

# Use a list of receivers:
RECEIVER_EMAILS = [
    "data-engineer@example.com",
    "team-lead@example.com", 
    "qa-analyst@example.com",
    "stakeholder@example.com"
]

# In your send_email_notification function, modify the message creation:
def send_email_notification(**context):
    # ... previous code for retrieving summary ...
    
    # Create the email message
    message = MIMEMultipart()
    message['From'] = SENDER_EMAIL
    
    # For multiple recipients, join them with commas
    message['To'] = ", ".join(RECEIVER_EMAILS)
    
    message['Subject'] = f"Airflow Environment Report - {datetime.now().strftime('%Y-%m-%d')}"
    
    # ... rest of the email body creation ...
    
    # When sending, use the list directly
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        
        # sendmail accepts a list of recipients
        text = message.as_string()
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAILS, text)
        server.quit()
        
        print(f"Email sent successfully to {len(RECEIVER_EMAILS)} recipients")
        return f"Email sent to {', '.join(RECEIVER_EMAILS)}"
        
    except Exception as e:
        print(f"Failed to send email: {str(e)}")
        raise
```

There's an important distinction to understand here. The `message['To']` field is what recipients see in their email client - it shows who else received the email. We join the email addresses with commas because that's the format email clients expect to display. However, the `server.sendmail()` method needs an actual Python list to know where to send the message.

You can also implement more sophisticated recipient management. For instance, you might want different recipients for different types of notifications:

```python
# Define recipient groups for different scenarios
NOTIFICATION_GROUPS = {
    'success': ['team-lead@example.com'],
    'failure': ['data-engineer@example.com', 'team-lead@example.com', 'oncall@example.com'],
    'daily_report': ['team-lead@example.com', 'stakeholder@example.com', 'analyst@example.com'],
    'data_quality_alert': ['data-engineer@example.com', 'qa-analyst@example.com']
}

# Then in your email function, choose the appropriate group:
def send_notification(notification_type='success', **context):
    recipients = NOTIFICATION_GROUPS.get(notification_type, NOTIFICATION_GROUPS['success'])
    
    # ... rest of email sending logic using recipients list ...
```

If you want to implement CC (carbon copy) and BCC (blind carbon copy) functionality, that's also possible. CC recipients are visible to everyone, while BCC recipients are hidden:

```python
# Define your recipient lists
TO_EMAILS = ["primary-recipient@example.com"]
CC_EMAILS = ["manager@example.com", "team-lead@example.com"]
BCC_EMAILS = ["audit@example.com", "archive@example.com"]

# In your email function:
message = MIMEMultipart()
message['From'] = SENDER_EMAIL
message['To'] = ", ".join(TO_EMAILS)
message['Cc'] = ", ".join(CC_EMAILS)
# Note: BCC is not added to headers - it's handled in sendmail

# When sending, combine all recipients
all_recipients = TO_EMAILS + CC_EMAILS + BCC_EMAILS
server.sendmail(SENDER_EMAIL, all_recipients, text)
```

One thing to be mindful of when sending to multiple recipients is email etiquette and privacy. If you're sending notifications about failures or sensitive data, consider whether all recipients should see each other's email addresses. Sometimes it's better to use BCC to protect privacy, or even to send individual emails to each recipient.

For production environments where you might have many recipients or complex routing logic, consider storing your recipient lists in Airflow Variables too. This makes it easy to update who receives notifications without modifying your DAG code:

```python
# Store in Airflow Variables as a JSON string:
# Key: notification_recipients
# Value: {"success": ["email1@example.com"], "failure": ["email1@example.com", "email2@example.com"]}

import json
from airflow.models import Variable

# In your DAG:
recipients_config = json.loads(Variable.get("notification_recipients", "{}"))
failure_recipients = recipients_config.get('failure', ['default@example.com'])
```

This approach gives you the flexibility to manage recipients through the Airflow UI, making it easy for non-technical team members to update notification settings without touching the code.
