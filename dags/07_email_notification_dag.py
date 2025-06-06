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
SENDER_EMAIL = "learnwithdatatweets@gmail.com"  # Replace with your Gmail address
RECEIVER_EMAIL = "lotfinejad@gmail.com"  # Replace with where you want to receive emails

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
    dag_id='07_email_notification_dag',
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
