# Complete Kubernetes Tutorial: From Containers to Production Data Pipelines with Apache Airflow

## Table of Contents

1. [Introduction: Why This Matters](#introduction)
2. [Chapter 1: Understanding Containers and Docker](#chapter-1-containers)
3. [Chapter 2: The Problem That Kubernetes Solves](#chapter-2-kubernetes-problem)
4. [Chapter 3: Kubernetes Fundamentals](#chapter-3-kubernetes-fundamentals)
5. [Chapter 4: Kind - Kubernetes in Docker](#chapter-4-kind)
6. [Chapter 5: Helm - The Kubernetes Package Manager](#chapter-5-helm)
7. [Chapter 6: Understanding Your Airflow Architecture](#chapter-6-airflow-architecture)
8. [Chapter 7: Deep Dive into Your Helm Chart](#chapter-7-helm-chart-analysis)
9. [Chapter 8: Step-by-Step Deployment Guide](#chapter-8-deployment)
10. [Chapter 9: Production Considerations](#chapter-9-production)
11. [Chapter 10: Troubleshooting and Best Practices](#chapter-10-troubleshooting)

------

## Introduction: Why This Matters {#introduction}

Imagine you're running a restaurant. At first, you might cook everything yourself in one kitchen. But as your restaurant grows, you need multiple chefs, different stations for different dishes, and a way to coordinate everything seamlessly. That's exactly what we're doing with data pipelines - moving from a single-machine setup to a distributed, scalable system.

In this tutorial, you'll learn how to transform your Apache Airflow data pipelines from running on a single Docker container to a production-ready, scalable system running on Kubernetes. By the end, you'll understand not just the "how" but the "why" behind every decision.

### What You'll Build

You'll deploy an Apache Airflow system that:

- Automatically scales based on workload
- Recovers from failures without human intervention
- Runs PySpark jobs for big data processing
- Can be deployed consistently across development, staging, and production
- Monitors and manages resources efficiently

### Prerequisites

Before we begin, ensure you have:

- Docker installed and running
- Basic understanding of Python and Apache Airflow
- A terminal/command line interface
- About 8GB of free RAM for local testing

------

## Chapter 1: Understanding Containers and Docker {#chapter-1-containers}

Before we dive into Kubernetes, let's ensure we understand containers. Think of a container as a shipping container for software. Just as shipping containers revolutionized global trade by standardizing how goods are packaged and transported, Docker containers standardize how software is packaged and run.

### Why Containers Matter

Let me tell you a story. Sarah is a data engineer who built a perfect data pipeline on her laptop. It processes customer data, runs machine learning models, and generates reports. Everything works beautifully... until she tries to deploy it to the production server. Suddenly, nothing works. Different Python versions, missing libraries, conflicting dependencies - it's a nightmare.

This is where containers come in. A container packages your application with everything it needs to run:

- The application code
- Runtime environment (like Python)
- System libraries
- System settings

### Understanding Your Dockerfile

Let's examine your Dockerfile to understand how containers work:

```dockerfile
FROM apache/airflow:2.8.1
```

This line is like saying "start with a pre-built foundation." Apache has already created a container with Airflow installed. You're building on top of it, like adding extra floors to an existing building.

```dockerfile
USER root

# Install Java (OpenJDK 17) and clean up in same layer to reduce image size
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        procps \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /tmp/*
```

Here's what's happening:

1. `USER root` - Temporarily become the administrator (like using "sudo")
2. Install Java - Because you need it for PySpark
3. Clean up immediately - This is crucial! Docker creates layers, and cleaning in the same command keeps the image smaller

```dockerfile
# Set JAVA_HOME environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
```

These environment variables tell the system where to find Java. It's like putting up signs in a building saying "Java is on the 3rd floor."

```dockerfile
# Switch to airflow user early
USER airflow

# Install Python packages
RUN pip install --no-cache-dir --user \
    Flask-Session==0.5.0 \
    pandas==2.0.3 \
    pyspark==3.5.0 \
    boto3==1.34.0 \
    requests==2.31.0 \
    psycopg2-binary==2.9.9
```

Security best practice: Run as a non-root user when possible. Here, you're installing Python packages that your data pipelines need.

### The Container Advantage

Your container is now a self-contained unit that will run identically whether it's on:

- Your local laptop
- A colleague's machine
- A cloud server
- A Kubernetes cluster

This consistency is invaluable for data pipelines where reproducibility is crucial.

------

## Chapter 2: The Problem That Kubernetes Solves {#chapter-2-kubernetes-problem}

Now that we understand containers, let's explore why we need Kubernetes.

### The Growing Pains

Imagine your data pipeline system is growing. You started with one Airflow container, but now you need:

- Multiple scheduler instances for high availability
- Separate containers for different types of jobs
- Auto-scaling based on workload
- Automatic recovery when containers crash
- Load balancing between containers
- Secure communication between services
- Resource management and limits

Trying to manage this manually with Docker alone would be like trying to conduct an orchestra where you have to tell each musician individually when to play each note. You need a conductor - that's Kubernetes.

### What Kubernetes Provides

Kubernetes (often abbreviated as K8s) is a container orchestration platform. Think of it as an intelligent system that:

1. **Manages Container Lifecycle**: Starts, stops, and restarts containers as needed
2. **Handles Scaling**: Automatically adds or removes containers based on demand
3. **Ensures High Availability**: If a container dies, Kubernetes starts a new one
4. **Manages Networking**: Containers can find and talk to each other easily
5. **Handles Storage**: Persistent data is managed across container restarts
6. **Manages Resources**: CPU and memory are allocated efficiently

### Real-World Scenario

Let's say you're running an e-commerce data pipeline that processes orders. On Black Friday, order volume increases 10x. With Kubernetes:

- It automatically spins up more worker containers to handle the load
- If a worker crashes processing a large batch, Kubernetes restarts it
- Load is distributed evenly across all workers
- After Black Friday, it scales back down to save resources

Without Kubernetes, you'd need engineers manually monitoring and adjusting the system 24/7.

------

## Chapter 3: Kubernetes Fundamentals {#chapter-3-kubernetes-fundamentals}

Now let's understand the core concepts of Kubernetes. I'll use analogies to make these concepts clear.

### The Kubernetes Architecture

Think of Kubernetes as a city:

**The Control Plane (City Hall)**

- **API Server**: The reception desk where all requests come in
- **Scheduler**: The city planner who decides where new buildings (pods) should go
- **Controller Manager**: The inspector ensuring everything is built according to plan
- **etcd**: The city records office storing all information

**Worker Nodes (City Districts)**

- **Kubelet**: The district manager ensuring everything runs properly
- **Container Runtime**: The construction crew actually building and running containers
- **Kube-proxy**: The traffic controller managing network routes

### Core Kubernetes Objects

Let me explain each Kubernetes object using real-world analogies:

#### 1. Pods

A Pod is the smallest unit in Kubernetes. Think of it as an apartment. Just as an apartment might have multiple rooms (containers) that share resources like electricity and water (network and storage), a Pod can have multiple containers that work together.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: airflow-worker
spec:
  containers:
  - name: worker
    image: lotfinejad/airflow-pyspark:latest
    ports:
    - containerPort: 8793
```

#### 2. Deployments

A Deployment is like a housing development blueprint. It says "I want 3 identical apartments (pods) built and maintained." If one gets damaged, the Deployment ensures a new one is built to replace it.

Looking at your repository's deployment example:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver
spec:
  replicas: 2  # We want 2 copies for high availability
  selector:
    matchLabels:
      app: airflow-webserver
  template:
    metadata:
      labels:
        app: airflow-webserver
    spec:
      containers:
      - name: webserver
        image: lotfinejad/airflow-pyspark:latest
        command: ["airflow", "webserver"]
```

#### 3. Services

A Service is like a phone directory. Instead of remembering specific apartment addresses (Pod IPs that can change), you look up "Pizza Delivery" in the directory. Services provide stable endpoints for accessing Pods.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: airflow-webserver
spec:
  selector:
    app: airflow-webserver  # Find pods with this label
  ports:
  - port: 8080
    targetPort: 8080
  type: ClusterIP  # Internal access only
```

#### 4. ConfigMaps and Secrets

- **ConfigMaps**: Like a bulletin board with public information (configuration files)
- **Secrets**: Like a safe with sensitive information (passwords, API keys)

From your repository:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: airflow-config
data:
  airflow.cfg: |
    [core]
    executor = KubernetesExecutor
    load_examples = False
```

#### 5. PersistentVolumes and PersistentVolumeClaims

Think of these as storage units:

- **PersistentVolume (PV)**: The actual storage unit
- **PersistentVolumeClaim (PVC)**: Your rental agreement for that storage

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: airflow-logs
spec:
  accessModes:
    - ReadWriteMany  # Multiple pods can read/write
  resources:
    requests:
      storage: 10Gi
```

#### 6. ServiceAccounts and RBAC

Security in Kubernetes is like a company's access control:

- **ServiceAccount**: An employee ID badge
- **Role**: What you're allowed to do (e.g., "can read files")
- **RoleBinding**: Giving a specific employee ID certain permissions

From your RBAC configuration:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: airflow-scheduler
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: airflow-scheduler
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: airflow-scheduler
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: airflow-scheduler
subjects:
- kind: ServiceAccount
  name: airflow-scheduler
```

This gives the Airflow scheduler permission to create and manage pods - essential for the KubernetesExecutor!

------

## Chapter 4: Kind - Kubernetes in Docker {#chapter-4-kind}

Now that we understand Kubernetes concepts, let's talk about Kind (Kubernetes in Docker).

### What is Kind?

Kind is a tool for running local Kubernetes clusters using Docker containers as "nodes". Think of it as a flight simulator for pilots - it gives you a realistic Kubernetes environment on your laptop without needing actual servers.

### Why Use Kind?

1. **Learning**: Perfect for understanding Kubernetes without cloud costs
2. **Development**: Test your applications locally before deploying
3. **CI/CD**: Run integration tests in a real Kubernetes environment
4. **Experimentation**: Try configurations without breaking production

### Understanding Your kind-config.yaml

Let's analyze your Kind configuration:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: airflow-cluster
nodes:
  - role: control-plane
    kubeadmConfigPatches:
      - |
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "ingress-ready=true"
    extraPortMappings:
      - containerPort: 80
        hostPort: 80
        protocol: TCP
      - containerPort: 443
        hostPort: 443
        protocol: TCP
      - containerPort: 30007
        hostPort: 30007
        protocol: TCP
  - role: worker
    extraMounts:
      - hostPath: ./dags
        containerPath: /opt/airflow/dags
      - hostPath: ./logs
        containerPath: /opt/airflow/logs
      - hostPath: ./scripts
        containerPath: /opt/airflow/scripts
  - role: worker
  - role: worker
```

Let me explain each part:

**Control Plane Node**: This is like the city hall of your Kubernetes cluster. The configuration:

- Labels it as "ingress-ready" (ready to receive external traffic)
- Maps ports so you can access services from your laptop
- Port 30007 is specifically for accessing Airflow's web interface

**Worker Nodes**: These are where your actual workloads run. Notice:

- You have 3 worker nodes for distributing workload
- The first worker mounts local directories (./dags, ./logs, ./scripts)
- This allows you to develop DAGs locally and have them immediately available in the cluster

### The Beauty of This Setup

With this configuration, you get:

1. A real Kubernetes cluster running on your laptop
2. Local file access for development
3. Multiple nodes to test distributed scenarios
4. Port access to your services

It's like having a miniature data center on your laptop!

------

## Chapter 5: Helm - The Kubernetes Package Manager {#chapter-5-helm}

If Kubernetes is like a city, Helm is like a city planner with pre-made blueprints for common buildings.

### What is Helm?

Helm is a package manager for Kubernetes. Just as `apt` or `brew` help you install software on Linux or Mac, Helm helps you install applications on Kubernetes.

### Why Helm Matters

Without Helm, deploying Airflow on Kubernetes would require:

- Creating 15-20 different YAML files
- Ensuring they all work together
- Updating version numbers in multiple places
- Managing configuration across environments

With Helm, you just need:

- A Chart (the blueprint)
- A values file (your customizations)

### Helm Concepts

**Chart**: A collection of files that describe a related set of Kubernetes resources. Think of it as a recipe book.

**Values**: Your specific ingredients and preferences. The same recipe (chart) can make different dishes based on the values.

**Release**: When you install a chart with specific values, you create a release. It's like the actual meal you cooked from the recipe.

**Repository**: Where charts are stored and shared, like a cookbook library.

### Understanding Helm Templates

Helm uses Go templating to make Kubernetes manifests dynamic. Let's look at an example from your repository:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "airflow.fullname" . }}-webserver
  labels:
    {{- include "airflow.labels" . | nindent 4 }}
    component: webserver
spec:
  replicas: {{ .Values.webserver.replicas }}
  selector:
    matchLabels:
      {{- include "airflow.selectorLabels" . | nindent 6 }}
      component: webserver
  template:
    metadata:
      labels:
        {{- include "airflow.selectorLabels" . | nindent 8 }}
        component: webserver
    spec:
      containers:
      - name: webserver
        image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
```

The `{{ }}` brackets contain template variables. When you install the chart, Helm replaces these with actual values:

- `{{ .Values.webserver.replicas }}` becomes `2` (from your values.yaml)
- `{{ .Values.image.repository }}` becomes `lotfinejad/airflow-pyspark`

This templating makes your deployment flexible and reusable across environments.

------

## Chapter 6: Understanding Your Airflow Architecture {#chapter-6-airflow-architecture}

Now let's understand how Airflow works in a Kubernetes environment, specifically with the KubernetesExecutor.

### Traditional Airflow vs. Kubernetes-Native Airflow

**Traditional Airflow** (LocalExecutor or CeleryExecutor):

- Runs on fixed servers or containers
- Workers are long-running processes
- Scaling requires manual intervention
- Resource usage is often inefficient

**Kubernetes-Native Airflow** (KubernetesExecutor):

- Each task runs in its own pod
- Pods are created on-demand and destroyed after task completion
- Automatic scaling based on workload
- Efficient resource usage

### Your Airflow Components on Kubernetes

Looking at your architecture, here's what each component does:

#### 1. Scheduler

The scheduler is the brain of Airflow:

- Monitors DAGs and tasks
- Decides when tasks should run
- With KubernetesExecutor, creates pods for each task

From your configuration:

```yaml
scheduler:
  enabled: true
  replicas: 1
  serviceAccount:
    create: true
    name: "airflow-scheduler"
```

The scheduler needs special permissions (via ServiceAccount) to create and manage pods.

#### 2. Webserver

The webserver provides the Airflow UI:

- View DAGs and task status
- Trigger runs manually
- Check logs and debug issues

Your configuration runs 2 replicas for high availability:

```yaml
webserver:
  enabled: true
  replicas: 2
  service:
    type: NodePort
    port: 8080
    nodePort: 30007
```

The NodePort service makes it accessible from outside the cluster on port 30007.

#### 3. PostgreSQL Database

The database stores:

- DAG definitions and metadata
- Task states and history
- User information and permissions

```yaml
postgresql:
  enabled: true
  auth:
    postgresPassword: "postgres"
    username: "airflow"
    password: "airflow"
    database: "airflow"
  primary:
    persistence:
      enabled: true
      size: 8Gi
```

Persistence is crucial - you don't want to lose your DAG history if the database restarts!

#### 4. Redis (Optional)

While not in your current configuration, Redis is often used for:

- Caching
- As a message broker for CeleryExecutor
- Storing temporary data

### The KubernetesExecutor Flow

Here's how a task executes in your setup:

1. **Scheduler finds a ready task** → "Customer order data needs processing"
2. **Scheduler creates a pod specification** → "I need a worker with 2GB RAM and 1 CPU"
3. **Kubernetes creates the pod** → "Creating worker pod on node-2"
4. **Pod pulls the Docker image** → "Loading lotfinejad/airflow-pyspark:latest"
5. **Task executes** → "Processing 10,000 customer orders"
6. **Task completes, pod terminates** → "Job done, cleaning up resources"

This on-demand execution means:

- No idle workers consuming resources
- Each task gets a clean environment
- Failed tasks don't affect others
- Perfect for bursty workloads

------

## Chapter 7: Deep Dive into Your Helm Chart {#chapter-7-helm-chart-analysis}

Let's examine your Helm chart structure in detail to understand how everything fits together.

### Chart Structure

Your Helm chart follows the standard structure:

```
helm/
├── charts/
│   └── airflow/
│       ├── Chart.yaml           # Chart metadata
│       ├── values.yaml          # Default values
│       ├── templates/           # Kubernetes manifests
│       │   ├── configmap.yaml
│       │   ├── deployment-scheduler.yaml
│       │   ├── deployment-webserver.yaml
│       │   ├── rbac.yaml
│       │   ├── secret.yaml
│       │   ├── service-webserver.yaml
│       │   └── statefulset-postgresql.yaml
│       └── files/
│           └── airflow.cfg      # Airflow configuration
```

### Analyzing Key Templates

#### 1. The ConfigMap (configmap.yaml)

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "airflow.fullname" . }}-config
  labels:
    {{- include "airflow.labels" . | nindent 4 }}
data:
  airflow.cfg: |
    {{- .Files.Get "files/airflow.cfg" | nindent 4 }}
```

This template:

- Creates a ConfigMap with your Airflow configuration
- Uses `.Files.Get` to read the actual config file
- Makes configuration changes easy without rebuilding images

#### 2. The Scheduler Deployment with RBAC

Your scheduler deployment is special because it needs permissions to create pods:

```yaml
# From rbac.yaml
{{- if .Values.scheduler.serviceAccount.create -}}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "airflow.serviceAccountName" . }}-scheduler
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ include "airflow.fullname" . }}-scheduler
rules:
- apiGroups: [""]
  resources: ["pods", "pods/log"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ include "airflow.fullname" . }}-scheduler
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ include "airflow.fullname" . }}-scheduler
subjects:
- kind: ServiceAccount
  name: {{ include "airflow.serviceAccountName" . }}-scheduler
{{- end }}
```

This RBAC configuration is crucial! It:

- Creates a ServiceAccount (identity for the scheduler)
- Defines a Role (what the scheduler can do)
- Binds them together with a RoleBinding

Without this, the KubernetesExecutor would fail with permission errors.

#### 3. The Values File Deep Dive

Your values.yaml file controls everything:

```yaml
# Image configuration
image:
  repository: lotfinejad/airflow-pyspark
  pullPolicy: IfNotPresent
  tag: "latest"

# Resource management
resources:
  scheduler:
    limits:
      cpu: 1000m
      memory: 2Gi
    requests:
      cpu: 500m
      memory: 1Gi
  webserver:
    limits:
      cpu: 1000m
      memory: 2Gi
    requests:
      cpu: 500m
      memory: 1Gi
```

Understanding resources:

- **Requests**: Minimum resources guaranteed
- **Limits**: Maximum resources allowed
- **1000m**: 1 CPU core (m = millicores)
- **2Gi**: 2 Gibibytes of memory

This ensures your Airflow components have enough resources without hogging the cluster.

### Environment-Specific Values

One of Helm's strengths is using different values for different environments:

**Development** (values-dev.yaml):

```yaml
webserver:
  replicas: 1  # Save resources in dev
postgresql:
  primary:
    persistence:
      size: 1Gi  # Smaller database
```

**Production** (values-prod.yaml):

```yaml
webserver:
  replicas: 3  # High availability
postgresql:
  primary:
    persistence:
      size: 50Gi  # Larger database
    resources:
      limits:
        memory: 4Gi  # More memory for performance
```

------

## Chapter 8: Step-by-Step Deployment Guide {#chapter-8-deployment}

Now let's deploy your Airflow system step by step. I'll explain what happens at each stage.

### Step 1: Install Prerequisites

First, install the necessary tools:

```bash
# Install Docker (if not already installed)
# For Mac: Download Docker Desktop
# For Linux:
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Install Kind
# For Mac:
brew install kind

# For Linux:
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind

# Install kubectl
# For Mac:
brew install kubectl

# For Linux:
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/

# Install Helm
# For Mac:
brew install helm

# For Linux:
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

### Step 2: Create Your Kind Cluster

```bash
# Create the cluster using your configuration
kind create cluster --config kind-config.yaml

# Verify the cluster is running
kubectl cluster-info

# You should see something like:
# Kubernetes control plane is running at https://127.0.0.1:xxxxx
# CoreDNS is running at https://127.0.0.1:xxxxx/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy
```

What's happening here:

1. Kind creates Docker containers that act as Kubernetes nodes
2. It sets up networking between them
3. Installs Kubernetes components
4. Configures kubectl to talk to your new cluster

### Step 3: Build and Load Your Docker Image

Since Kind runs in Docker, we need to load your custom Airflow image into the Kind cluster:

```bash
# If you haven't built the image locally:
docker pull lotfinejad/airflow-pyspark:latest

# Load the image into Kind
kind load docker-image lotfinejad/airflow-pyspark:latest --name airflow-cluster

# Verify the image is loaded
docker exec -it airflow-cluster-control-plane crictl images | grep airflow
```

This makes your image available to all nodes in the Kind cluster.

### Step 4: Create Kubernetes Namespace

Namespaces are like folders - they organize resources:

```bash
# Create a namespace for Airflow
kubectl create namespace airflow

# Set it as the default for subsequent commands
kubectl config set-context --current --namespace=airflow
```

### Step 5: Install Airflow Using Helm

Now for the main event - installing Airflow:

```bash
# First, add the Airflow Helm repository (if using official chart)
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Install using your custom values
helm install airflow ./helm/charts/airflow \
  --namespace airflow \
  --create-namespace \
  --values ./helm/charts/airflow/values.yaml \
  --wait
```

The `--wait` flag makes Helm wait until all resources are ready.

### Step 6: Verify the Installation

Check that everything is running:

```bash
# Check all pods
kubectl get pods

# You should see something like:
# NAME                                     READY   STATUS    RESTARTS   AGE
# airflow-postgresql-0                     1/1     Running   0          2m
# airflow-scheduler-7f9c9f9f5-abc123       1/1     Running   0          2m
# airflow-webserver-6d8c9f9f5-def456       1/1     Running   0          2m
# airflow-webserver-6d8c9f9f5-ghi789       1/1     Running   0          2m

# Check services
kubectl get services

# Check persistent volumes
kubectl get pvc
```

### Step 7: Access the Airflow UI

Your configuration uses NodePort to expose Airflow:

```bash
# Get the exact NodePort (should be 30007 based on your config)
kubectl get service airflow-webserver -o jsonpath='{.spec.ports[0].nodePort}'

# Access Airflow at:
# http://localhost:30007

# Default credentials (from your configuration):
# Username: admin
# Password: admin
```

### Step 8: Deploy Your First DAG

Create a test DAG to verify everything works:

```python
# Save this as dags/test_kubernetes_executor.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def print_environment():
    """Print pod information"""
    print(f"Running in pod: {os.environ.get('HOSTNAME', 'unknown')}")
    print(f"Node name: {os.environ.get('NODE_NAME', 'unknown')}")
    print(f"Namespace: {os.environ.get('NAMESPACE', 'unknown')}")
    
    # Test PySpark availability
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("TestKubernetesExecutor") \
            .getOrCreate()
        print(f"Spark version: {spark.version}")
        spark.stop()
    except Exception as e:
        print(f"Spark test failed: {e}")

with DAG(
    'test_kubernetes_executor',
    default_args=default_args,
    description='Test DAG for Kubernetes Executor',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'kubernetes'],
) as dag:
    
    # Task 1: Check environment
    check_env = BashOperator(
        task_id='check_environment',
        bash_command='echo "Running on $(hostname)" && env | grep -E "(HOSTNAME|NODE_NAME|AIRFLOW)" | sort'
    )
    
    # Task 2: Python task
    python_task = PythonOperator(
        task_id='print_pod_info',
        python_callable=print_environment,
        pool='default_pool',
        pool_slots=1
    )
    
    # Task 3: Simulate data processing
    process_data = BashOperator(
        task_id='process_data',
        bash_command="""
            echo "Starting data processing..."
            for i in {1..5}; do
                echo "Processing batch $i"
                sleep 2
            done
            echo "Processing complete!"
        """
    )
    
    check_env >> python_task >> process_data
```

Copy this DAG to your dags folder and watch the magic:

```bash
# The DAG should appear in the UI within 30 seconds
# Trigger it manually and watch the pods

# In another terminal, watch pods being created:
kubectl get pods -w

# You'll see pods like:
# airflowtestkubernetesexecutorcheckenvironment-abc123   0/1     Init:0/1   0          1s
# airflowtestkubernetesexecutorcheckenvironment-abc123   0/1     PodInitializing   0   3s
# airflowtestkubernetesexecutorcheckenvironment-abc123   1/1     Running           0   5s
# airflowtestkubernetesexecutorcheckenvironment-abc123   0/1     Completed         0   8s
```

Each task creates its own pod, runs, and then terminates. This is the KubernetesExecutor in action!

### Step 9: Monitor Resource Usage

Check how your cluster is performing:

```bash
# Overall cluster resources
kubectl top nodes

# Pod resource usage
kubectl top pods

# Check logs for any pod
kubectl logs <pod-name>

# For scheduler logs (continuous stream)
kubectl logs -f deployment/airflow-scheduler
```

### Troubleshooting Common Issues

**Issue 1: Pods stuck in Pending**

```bash
# Check why
kubectl describe pod <pod-name>

# Common causes:
# - Insufficient resources: Scale down resource requests
# - Image pull errors: Ensure image is loaded in Kind
```

**Issue 2: Database connection errors**

```bash
# Check PostgreSQL pod
kubectl logs airflow-postgresql-0

# Verify service DNS
kubectl run -it --rm debug --image=busybox --restart=Never -- nslookup airflow-postgresql
```

**Issue 3: Scheduler not creating pods**

```bash
# Check scheduler logs
kubectl logs deployment/airflow-scheduler | grep ERROR

# Verify RBAC permissions
kubectl auth can-i create pods --as=system:serviceaccount:airflow:airflow-scheduler
```

------

## Chapter 9: Production Considerations {#chapter-9-production}

Moving from development to production requires additional considerations. Let's explore what changes when you deploy to a real Kubernetes cluster.

### 1. Security Hardening

#### Use Secrets Properly

Never store sensitive data in ConfigMaps. Create proper secrets:

```yaml
# Create secret for database connection
apiVersion: v1
kind: Secret
metadata:
  name: airflow-database
type: Opaque
stringData:
  connection: postgresql://airflow:strongpassword@postgresql:5432/airflow

# Create secret for Fernet key (for encrypting connections in Airflow)
apiVersion: v1
kind: Secret
metadata:
  name: airflow-fernet-key
type: Opaque
stringData:
  fernet-key: "your-fernet-key-here"  # Generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Update your deployment to use these secrets:

```yaml
env:
  - name: AIRFLOW__CORE__SQL_ALCHEMY_CONN
    valueFrom:
      secretKeyRef:
        name: airflow-database
        key: connection
  - name: AIRFLOW__CORE__FERNET_KEY
    valueFrom:
      secretKeyRef:
        name: airflow-fernet-key
        key: fernet-key
```

#### Network Policies

Restrict network traffic between pods:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: airflow-network-policy
spec:
  podSelector:
    matchLabels:
      app: airflow
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: airflow
    - namespaceSelector:
        matchLabels:
          name: airflow
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: airflow
  - to:
    - namespaceSelector:
        matchLabels:
          name: kube-system
    ports:
    - protocol: TCP
      port: 53  # DNS
```

### 2. High Availability

#### Multi-Region Deployment

For true high availability, deploy across multiple regions:

```yaml
# values-prod-multi-region.yaml
webserver:
  replicas: 6  # 2 per region
  affinity:
    podAntiAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchExpressions:
            - key: component
              operator: In
              values:
              - webserver
          topologyKey: topology.kubernetes.io/zone

scheduler:
  replicas: 2  # Active-passive setup
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchExpressions:
          - key: component
            operator: In
            values:
            - scheduler
        topologyKey: kubernetes.io/hostname
```

#### Database High Availability

Use a managed database service or set up PostgreSQL with replication:

```yaml
postgresql:
  architecture: replication
  auth:
    replicationUsername: replicator
    replicationPassword: replicatorpassword
  primary:
    persistence:
      size: 100Gi
    resources:
      limits:
        memory: 8Gi
        cpu: 4
  readReplicas:
    replicaCount: 2
    persistence:
      size: 100Gi
    resources:
      limits:
        memory: 4Gi
        cpu: 2
```

### 3. Monitoring and Observability

#### Prometheus Integration

Add Prometheus annotations to your pods:

```yaml
metadata:
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"
    prometheus.io/path: "/metrics"
```

Deploy Prometheus and Grafana:

```bash
# Add Prometheus Helm repo
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Install Prometheus stack
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace
```

#### Custom Airflow Metrics

Create a custom metrics exporter:

```python
# dags/metrics_exporter.py
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from prometheus_client import Gauge, generate_latest
from flask import Response
import flask

# Define metrics
dag_success_rate = Gauge('airflow_dag_success_rate', 'DAG success rate', ['dag_id'])
task_duration = Gauge('airflow_task_duration_seconds', 'Task duration in seconds', ['dag_id', 'task_id'])
pending_tasks = Gauge('airflow_pending_tasks', 'Number of pending tasks')

def export_metrics():
    # Calculate metrics
    for dag in DagModel.get_all_dags():
        recent_runs = DagRun.find(dag_id=dag.dag_id, limit=100)
        success_count = sum(1 for run in recent_runs if run.state == State.SUCCESS)
        dag_success_rate.labels(dag_id=dag.dag_id).set(success_count / len(recent_runs) if recent_runs else 0)
    
    # Export pending tasks
    pending_count = TaskInstance.query.filter_by(state=State.QUEUED).count()
    pending_tasks.set(pending_count)
    
    return Response(generate_latest(), mimetype='text/plain')
```

### 4. Resource Optimization

#### Pod Autoscaling

Configure Horizontal Pod Autoscaler for the webserver:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: airflow-webserver-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: airflow-webserver
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

#### Resource Quotas

Set namespace resource limits:

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: airflow-quota
  namespace: airflow
spec:
  hard:
    requests.cpu: "100"
    requests.memory: 200Gi
    limits.cpu: "200"
    limits.memory: 400Gi
    persistentvolumeclaims: "10"
    services.loadbalancers: "2"
```

### 5. Backup and Disaster Recovery

#### Database Backups

Implement automated PostgreSQL backups:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: airflow-db-backup
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: postgres-backup
            image: postgres:13-alpine
            env:
            - name: PGPASSWORD
              valueFrom:
                secretKeyRef:
                  name: airflow-postgresql
                  key: password
            command:
            - /bin/sh
            - -c
            - |
              DATE=$(date +%Y%m%d_%H%M%S)
              pg_dump -h airflow-postgresql -U airflow -d airflow > /backup/airflow_$DATE.sql
              # Upload to S3 or other storage
              aws s3 cp /backup/airflow_$DATE.sql s3://my-backup-bucket/airflow/
              # Keep only last 30 days
              find /backup -name "airflow_*.sql" -mtime +30 -delete
            volumeMounts:
            - name: backup
              mountPath: /backup
          volumes:
          - name: backup
            persistentVolumeClaim:
              claimName: airflow-backup-pvc
          restartPolicy: OnFailure
```

#### DAG Backup

Since DAGs are code, use Git:

```yaml
# Sync DAGs from Git repository
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-git-sync
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: git-sync
        image: k8s.gcr.io/git-sync/git-sync:v3.6.0
        env:
        - name: GIT_SYNC_REPO
          value: "https://github.com/yourorg/airflow-dags.git"
        - name: GIT_SYNC_BRANCH
          value: "main"
        - name: GIT_SYNC_DEST
          value: "dags"
        - name: GIT_SYNC_WAIT
          value: "60"  # Sync every 60 seconds
        volumeMounts:
        - name: dags
          mountPath: /git
      volumes:
      - name: dags
        persistentVolumeClaim:
          claimName: airflow-dags-pvc
```

### 6. Performance Tuning

#### Connection Pooling

Configure database connection pooling:

```python
# In airflow.cfg or environment variables
AIRFLOW__CORE__SQL_ALCHEMY_POOL_SIZE=20
AIRFLOW__CORE__SQL_ALCHEMY_MAX_OVERFLOW=40
AIRFLOW__CORE__SQL_ALCHEMY_POOL_RECYCLE=1800
AIRFLOW__CORE__SQL_ALCHEMY_POOL_PRE_PING=True
```

#### Task Concurrency

Optimize parallelism settings:

```python
# Global settings
AIRFLOW__CORE__PARALLELISM=128
AIRFLOW__CORE__DAG_CONCURRENCY=64
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=16

# Per-DAG settings
dag = DAG(
    'high_priority_pipeline',
    max_active_runs=5,
    concurrency=20,  # Max tasks running simultaneously
    ...
)
```

------

## Chapter 10: Troubleshooting and Best Practices {#chapter-10-troubleshooting}

### Common Issues and Solutions

#### 1. Pod Scheduling Issues

**Problem**: Tasks stay in queue, pods not created

```bash
# Check scheduler logs
kubectl logs deployment/airflow-scheduler | grep ERROR

# Common solutions:
# 1. Verify RBAC permissions
kubectl auth can-i create pods --as=system:serviceaccount:airflow:airflow-scheduler

# 2. Check resource availability
kubectl describe nodes
kubectl top nodes

# 3. Check for pod disruption budgets
kubectl get pdb
```

#### 2. Image Pull Errors

**Problem**: Pods fail with ImagePullBackOff

```bash
# Check events
kubectl describe pod <pod-name>

# Solutions:
# 1. For private registries, create image pull secret
kubectl create secret docker-registry regcred \
  --docker-server=docker.io \
  --docker-username=<username> \
  --docker-password=<password>

# 2. Update service account
kubectl patch serviceaccount airflow-scheduler \
  -p '{"imagePullSecrets": [{"name": "regcred"}]}'
```

#### 3. Database Connection Issues

**Problem**: Scheduler can't connect to database

```bash
# Test connection from a debug pod
kubectl run -it --rm debug --image=postgres:13-alpine --restart=Never -- \
  psql -h airflow-postgresql -U airflow -d airflow -c "SELECT 1"

# Check DNS resolution
kubectl run -it --rm debug --image=busybox --restart=Never -- \
  nslookup airflow-postgresql
```

#### 4. Performance Issues

**Problem**: Slow task execution

```python
# Add performance monitoring to your DAGs
from datetime import datetime
import time

def performance_wrapped_task(task_func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        
        # Log start
        print(f"Task started at: {datetime.now()}")
        print(f"Pod: {os.environ.get('HOSTNAME')}")
        print(f"Node: {os.environ.get('NODE_NAME')}")
        
        # Execute task
        result = task_func(*args, **kwargs)
        
        # Log completion
        duration = time.time() - start_time
        print(f"Task completed in {duration:.2f} seconds")
        
        return result
    return wrapper

# Use in your tasks
@performance_wrapped_task
def process_data():
    # Your task logic here
    pass
```

### Best Practices

#### 1. DAG Design for Kubernetes

```python
# Good: Lightweight tasks that scale well
def process_batch(batch_id):
    # Process single batch
    data = fetch_batch(batch_id)
    result = transform_data(data)
    save_result(result)
    return batch_id

# Create dynamic tasks for parallel processing
for i in range(10):
    task = PythonOperator(
        task_id=f'process_batch_{i}',
        python_callable=process_batch,
        op_args=[i],
        pool='data_processing',  # Use pools to limit concurrency
        resources={
            'request_memory': '2Gi',
            'request_cpu': '1000m',
            'limit_memory': '4Gi',
            'limit_cpu': '2000m'
        }
    )
```

#### 2. Resource Management

```python
# Define resource profiles for different task types
RESOURCE_PROFILES = {
    'small': {
        'request_memory': '512Mi',
        'request_cpu': '250m',
        'limit_memory': '1Gi',
        'limit_cpu': '500m'
    },
    'medium': {
        'request_memory': '2Gi',
        'request_cpu': '1000m',
        'limit_memory': '4Gi',
        'limit_cpu': '2000m'
    },
    'large': {
        'request_memory': '8Gi',
        'request_cpu': '4000m',
        'limit_memory': '16Gi',
        'limit_cpu': '8000m'
    }
}

# Use profiles in tasks
heavy_task = PythonOperator(
    task_id='process_large_dataset',
    python_callable=process_large_data,
    resources=RESOURCE_PROFILES['large']
)
```

#### 3. Logging Best Practices

```python
import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def centralized_logging_task(**context):
    # Set up structured logging
    logger = logging.getLogger(__name__)
    
    # Add context to logs
    extra = {
        'dag_id': context['dag'].dag_id,
        'task_id': context['task'].task_id,
        'execution_date': context['execution_date'].isoformat(),
        'try_number': context['task_instance'].try_number,
        'pod_name': os.environ.get('HOSTNAME'),
        'node_name': os.environ.get('NODE_NAME')
    }
    
    logger.info("Starting task execution", extra=extra)
    
    try:
        # Your task logic
        result = perform_work()
        logger.info(f"Task completed successfully: {result}", extra=extra)
        
        # Optionally save logs to S3
        if context['task_instance'].try_number > 1:
            # Save retry logs for debugging
            log_content = context['task_instance'].log.read()
            s3 = S3Hook()
            s3.load_string(
                string_data=log_content,
                key=f"logs/{context['dag'].dag_id}/{context['task'].task_id}/{context['execution_date']}.log",
                bucket_name='airflow-logs'
            )
    except Exception as e:
        logger.error(f"Task failed: {str(e)}", extra=extra, exc_info=True)
        raise
```

#### 4. Security Best Practices

```python
# Use Airflow Connections instead of hardcoding credentials
from airflow.hooks.base import BaseHook

def secure_data_pipeline():
    # Get connection from Airflow's encrypted connection store
    conn = BaseHook.get_connection('my_database')
    
    # Use the connection
    connection_string = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    
    # Never log sensitive information
    logger.info(f"Connecting to database at {conn.host}:{conn.port}")  # Good
    # logger.info(f"Connection string: {connection_string}")  # Bad!
```

#### 5. Testing Strategies

```python
# Unit test your tasks
import pytest
from unittest.mock import MagicMock, patch

def test_process_data_task():
    # Mock Airflow context
    mock_context = {
        'dag': MagicMock(dag_id='test_dag'),
        'task': MagicMock(task_id='test_task'),
        'execution_date': datetime(2024, 1, 1),
        'task_instance': MagicMock(try_number=1)
    }
    
    # Mock external dependencies
    with patch('my_module.fetch_data') as mock_fetch:
        mock_fetch.return_value = test_data
        
        # Run task
        result = process_data_task(**mock_context)
        
        # Verify results
        assert result == expected_result
        mock_fetch.assert_called_once()

# Integration test with Kind
def test_dag_in_kind():
    # Deploy to Kind cluster
    subprocess.run(['kind', 'create', 'cluster', '--config', 'kind-test.yaml'])
    
    try:
        # Install Airflow
        subprocess.run(['helm', 'install', 'airflow-test', './helm/charts/airflow'])
        
        # Wait for pods
        kubectl_wait('pod', 'airflow-webserver', 'Running')
        
        # Trigger DAG
        trigger_dag('test_dag')
        
        # Verify execution
        assert check_dag_success('test_dag')
    finally:
        # Cleanup
        subprocess.run(['kind', 'delete', 'cluster'])
```

### Maintenance Procedures

#### Regular Health Checks

Create a maintenance DAG:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import psycopg2
import requests

def check_database_health(**context):
    """Verify database is healthy"""
    conn = psycopg2.connect(
        host="airflow-postgresql",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    
    cur = conn.cursor()
    
    # Check table sizes
    cur.execute("""
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        LIMIT 10
    """)
    
    large_tables = cur.fetchall()
    print("Largest tables:", large_tables)
    
    # Check for long-running queries
    cur.execute("""
        SELECT pid, age(clock_timestamp(), query_start), usename, query 
        FROM pg_stat_activity
        WHERE state != 'idle' 
        AND query NOT ILIKE '%pg_stat_activity%'
        ORDER BY query_start DESC
    """)
    
    long_queries = cur.fetchall()
    if long_queries:
        print("Warning: Long-running queries detected:", long_queries)
    
    conn.close()

def cleanup_old_logs(**context):
    """Remove old log entries"""
    conn = psycopg2.connect(
        host="airflow-postgresql",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    
    cur = conn.cursor()
    
    # Delete old log entries (older than 30 days)
    cur.execute("""
        DELETE FROM log 
        WHERE dttm < NOW() - INTERVAL '30 days'
    """)
    
    deleted_count = cur.rowcount
    conn.commit()
    conn.close()
    
    print(f"Deleted {deleted_count} old log entries")

def check_scheduler_health(**context):
    """Verify scheduler is processing tasks"""
    # Query for recently scheduled tasks
    recent_tasks = TaskInstance.query.filter(
        TaskInstance.start_date > datetime.now() - timedelta(minutes=10)
    ).count()
    
    if recent_tasks == 0:
        raise AlertException("No tasks scheduled in last 10 minutes!")
    
    print(f"Scheduler is healthy: {recent_tasks} tasks scheduled recently")

# Create maintenance DAG
maintenance_dag = DAG(
    'system_maintenance',
    default_args={
        'owner': 'platform-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': True,
        'email': ['platform-team@company.com']
    },
    description='System maintenance and health checks',
    schedule_interval='0 * * * *',  # Hourly
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['maintenance', 'platform']
)

# Define tasks
db_health = PythonOperator(
    task_id='check_database_health',
    python_callable=check_database_health,
    dag=maintenance_dag
)

cleanup_logs = PythonOperator(
    task_id='cleanup_old_logs',
    python_callable=cleanup_old_logs,
    dag=maintenance_dag
)

scheduler_health = PythonOperator(
    task_id='check_scheduler_health',
    python_callable=check_scheduler_health,
    dag=maintenance_dag
)

# Set dependencies
db_health >> cleanup_logs
scheduler_health
```

## Conclusion

Congratulations! You've journeyed from understanding basic containers to deploying a production-ready Apache Airflow system on Kubernetes. Let's recap what you've learned:

1. **Containers** package applications with all dependencies for consistent deployment
2. **Kubernetes** orchestrates containers at scale with self-healing and resource management
3. **Kind** provides a local Kubernetes environment perfect for development and testing
4. **Helm** simplifies complex deployments with reusable, parameterized charts
5. **KubernetesExecutor** transforms Airflow into a cloud-native, scalable data platform

Your Airflow deployment now has:

- **Scalability**: Automatically adjusts to workload
- **Resilience**: Self-healing with automatic pod recreation
- **Efficiency**: Resources used only when needed
- **Flexibility**: Easy to deploy across environments
- **Observability**: Built-in monitoring and logging

### Next Steps

1. **Experiment**: Modify the Helm values and see how it affects deployment
2. **Monitor**: Set up Prometheus and Grafana for detailed metrics
3. **Optimize**: Tune resource requests/limits based on actual usage
4. **Extend**: Add custom operators for your specific use cases
5. **Contribute**: Share your improvements with the community

Remember, Kubernetes is a journey, not a destination. Keep learning, experimenting, and building. The cloud-native data platform you've built today is the foundation for handling tomorrow's data challenges.

Happy orchestrating! 🚀