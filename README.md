# Airflow + PySpark on Kubernetes

A complete infrastructure setup for running Apache Airflow with PySpark on Kubernetes using Helm charts and Kind for local development.

## Overview

This project provides a production-ready deployment of Apache Airflow with PySpark integration on Kubernetes. It includes:

* **Apache Airflow** with KubernetesExecutor
* **PySpark** integration for big data processing
* **PostgreSQL** as the metadata database
* **Kubernetes** deployment using Helm charts
* **Kind** (Kubernetes in Docker) for local development
* Sample DAGs demonstrating Airflow and PySpark capabilities

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Airflow Web    │    │  Airflow        │    │  PostgreSQL     │
│  Server (8080)  │    │  Scheduler      │    │  Database       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  Shared Volumes │
                    │  - DAGs         │
                    │  - Scripts      │
                    │  - Logs         │
                    │  - Plugins      │
                    └─────────────────┘
```

## Prerequisites

* Docker (v20.10+)
* Kind (v0.11+)
* kubectl (v1.21+)
* Helm (v3.0+)
* Git

| Tool         | Purpose                          | Link                                                                                               |
| ------------ | -------------------------------- | -------------------------------------------------------------------------------------------------- |
| Docker       | Runs the Kind Kubernetes cluster | [https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)   |
| kubectl      | CLI for Kubernetes               | [https://kubernetes.io/docs/tasks/tools/](https://kubernetes.io/docs/tasks/tools/)                 |
| Kind         | Local Kubernetes in Docker       | [https://kind.sigs.k8s.io/docs/user/quick-start/](https://kind.sigs.k8s.io/docs/user/quick-start/) |
| Helm         | Kubernetes package manager       | [https://helm.sh/docs/intro/install/](https://helm.sh/docs/intro/install/)                         |
| GitHub Token | For private repo Git-Sync        | [https://github.com/settings/tokens](https://github.com/settings/tokens)                           |

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/datatweets/airflow-pyspark-k8s.git
cd airflow-pyspark-k8s
```

### 2. Configure your environment

* Update volume paths in `values.yaml` (replace `YOUR_USERNAME`):

  ```yaml
  volumes:
    hostPaths:
      dags:    /Users/YOUR_USERNAME/airflow-pyspark-k8s/dags
      scripts:/Users/YOUR_USERNAME/airflow-pyspark-k8s/scripts
      logs:   /Users/YOUR_USERNAME/airflow-pyspark-k8s/logs
      plugins:/Users/YOUR_USERNAME/airflow-pyspark-k8s/plugins
  ```
* Update hostPath in `kind-config.yaml`:

  ```yaml
  extraMounts:
  - hostPath: /Users/YOUR_USERNAME/airflow-pyspark-k8s
    containerPath: /workspace
  ```
* Set the correct `JAVA_HOME` in `templates/airflow-configmap.yaml` for your CPU architecture:

  * x86\_64: `/usr/lib/jvm/java-17-openjdk-amd64`
  * arm64:  `/usr/lib/jvm/java-17-openjdk-arm64`

### 3. Create the Kind cluster

```bash
kind create cluster --config kind-config.yaml --name airflow-cluster
```

### 4. Apply RBAC configuration

```bash
kubectl apply -f k8s/rbac.yaml
```

### 5. Deploy with Helm

```bash
helm upgrade --install airflow-pyspark . \
  --namespace default \
  --create-namespace \
  --values values.yaml \
  --wait
```

### 6. Verify deployment

```bash
kubectl get pods -w
kubectl get services
```

### 7. Access the Airflow Web UI

Open [http://localhost:30080](http://localhost:30080)

**Default credentials:** admin / admin

## DAGs and Examples

* **Hello World DAG** (`dags/hello_world_dag.py`)
* **Spark WordCount DAG** (`dags/spark_wordcount.py`)

## Updating on-the-fly

Edit DAG files under `dags/` and they will auto-refresh in Airflow.

```bash
# After changes:
helm upgrade airflow-pyspark . --namespace default --values values.yaml --wait
```

## Cleanup

```bash
helm uninstall airflow-pyspark --namespace default
kind delete cluster --name airflow-cluster
```
