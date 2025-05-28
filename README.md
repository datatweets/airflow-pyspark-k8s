# Airflow + PySpark on Kubernetes

A complete infrastructure setup for running Apache Airflow with PySpark on Kubernetes using Helm charts and Kind for local development.

## Overview

This project provides a production-ready deployment of Apache Airflow with PySpark integration on Kubernetes. It includes:

- **Apache Airflow** with LocalExecutor
- **PySpark** integration for big data processing
- **PostgreSQL** as the metadata database
- **Kubernetes** deployment using Helm charts
- **Kind** (Kubernetes in Docker) for local development
- Sample DAGs demonstrating Airflow and PySpark capabilities

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Airflow Web    │    │  Airflow        │    │  PostgreSQL     │
│  Server         │    │  Scheduler      │    │  Database       │
│  (Port 8080)    │    │                 │    │                 │
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

### Required Software
- **Docker** (v20.10+)
- **Kind** (v0.11+)
- **Kubectl** (v1.21+)
- **Helm** (v3.0+)

### Installation Commands

```bash
# Install Kind
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.11.1/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind

# Install Kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/

# Install Helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/datatweets/airflow-pyspark-k8s.git
cd airflow-pyspark-k8s
```

### 2. Configure Paths and Architecture

**⚠️ IMPORTANT: Before deployment, you must update the configuration files for your environment:**

#### Update Volume Paths in values.yaml

Replace `lotfinejad` with your actual username in [`values.yaml`](values.yaml):

```yaml
# Change this section in values.yaml
volumes:
  hostPaths:
    dags: /Users/YOUR_USERNAME/airflow-pyspark-k8s/dags      # Replace YOUR_USERNAME
    scripts: /Users/YOUR_USERNAME/airflow-pyspark-k8s/scripts
    logs: /Users/YOUR_USERNAME/airflow-pyspark-k8s/logs
    plugins: /Users/YOUR_USERNAME/airflow-pyspark-k8s/plugins
```

**For different operating systems:**
- **macOS**: `/Users/YOUR_USERNAME/airflow-pyspark-k8s/`
- **Linux**: `/home/YOUR_USERNAME/airflow-pyspark-k8s/`
- **Windows**: `/c/Users/YOUR_USERNAME/airflow-pyspark-k8s/`

#### Update Kind Configuration

Also update the hostPath in [`kind-config.yaml`](kind-config.yaml):

```yaml
extraMounts:
- hostPath: /Users/YOUR_USERNAME/airflow-pyspark-k8s  # Replace YOUR_USERNAME
  containerPath: /workspace
```

#### Configure Java Path for Your Architecture

Update [`templates/airflow-configmap.yaml`](templates/airflow-configmap.yaml) based on your CPU architecture:

**For Intel/AMD processors (x86_64):**
```yaml
data:
  JAVA_HOME: "/usr/lib/jvm/java-17-openjdk-amd64"
  # ... other config
```

**For ARM processors (Apple Silicon M1/M2, ARM64):**
```yaml
data:
  JAVA_HOME: "/usr/lib/jvm/java-17-openjdk-arm64"
  # ... other config
```

**How to check your architecture:**
```bash
# On macOS/Linux
uname -m
# x86_64 = Intel/AMD
# arm64 = ARM (Apple Silicon)

# On Windows
echo $env:PROCESSOR_ARCHITECTURE
# AMD64 = Intel/AMD
# ARM64 = ARM
```

### 3. Create Kind Cluster

The [`kind-config.yaml`](kind-config.yaml) configures a local Kubernetes cluster with port forwarding and volume mounts:

```bash
kind create cluster --config kind-config.yaml --name airflow-cluster
```

### 4. Deploy with Helm

```bash
# Install the Helm chart
helm install airflow-pyspark . -f values.yaml

# Check deployment status
kubectl get pods -w
kubectl get services
```

### 5. Access Airflow Web UI

Once deployed, access the Airflow web interface at: http://localhost:30080

**Default Credentials:**
- Username: `admin`
- Password: `admin`

## Configuration

### Values Configuration

The [`values.yaml`](values.yaml) file contains all configuration options:

#### Key Settings

```yaml
airflow:
  image:
    repository: lotfinejad/airflow-pyspark
    tag: latest
  
  webserver:
    port: 8080
    replicas: 1
    
  scheduler:
    replicas: 1
    
  executor: LocalExecutor

postgresql:
  enabled: true
  persistence:
    enabled: true
    size: 2Gi

service:
  type: NodePort
  nodePort: 30080
```

#### Volume Mounts

⚠️ **Important**: Update these paths with your actual username and OS:

```yaml
volumes:
  hostPaths:
    # UPDATE THESE PATHS:
    dags: /Users/YOUR_USERNAME/airflow-pyspark-k8s/dags
    scripts: /Users/YOUR_USERNAME/airflow-pyspark-k8s/scripts
    logs: /Users/YOUR_USERNAME/airflow-pyspark-k8s/logs
    plugins: /Users/YOUR_USERNAME/airflow-pyspark-k8s/plugins
```

**Path Examples by OS:**
```yaml
# macOS
dags: /Users/john/airflow-pyspark-k8s/dags

# Linux  
dags: /home/john/airflow-pyspark-k8s/dags

# Windows (WSL2)
dags: /mnt/c/Users/john/airflow-pyspark-k8s/dags
```

### Architecture-Specific Configuration

#### Java Configuration by CPU Architecture

**Intel/AMD (x86_64) processors:**
```yaml
# In templates/airflow-configmap.yaml
data:
  JAVA_HOME: "/usr/lib/jvm/java-17-openjdk-amd64"
  SPARK_HOME: "/home/airflow/.local/lib/python3.8/site-packages/pyspark"
```

**ARM (Apple Silicon, ARM64) processors:**
```yaml
# In templates/airflow-configmap.yaml  
data:
  JAVA_HOME: "/usr/lib/jvm/java-17-openjdk-arm64"
  SPARK_HOME: "/home/airflow/.local/lib/python3.8/site-packages/pyspark"
```

#### Verify Your Architecture Setup

```bash
# After deployment, verify Java path
kubectl exec -it deployment/airflow-scheduler -- echo $JAVA_HOME
kubectl exec -it deployment/airflow-scheduler -- java -version

# Check available Java installations
kubectl exec -it deployment/airflow-scheduler -- ls -la /usr/lib/jvm/
```

### Docker Image Architecture

The project uses `lotfinejad/airflow-pyspark:latest` which supports multiple architectures. If you encounter image pull issues:

```bash
# For Intel/AMD
docker pull --platform linux/amd64 lotfinejad/airflow-pyspark:latest

# For ARM (Apple Silicon)
docker pull --platform linux/arm64 lotfinejad/airflow-pyspark:latest

# Load into Kind cluster
kind load docker-image lotfinejad/airflow-pyspark:latest --name airflow-cluster
```

## DAGs and Examples

### Available DAGs

1. **Hello World DAG** ([`dags/hello_world_dag.py`](dags/hello_world_dag.py))
   - Simple demonstration DAG
   - Python and Bash operators
   - Daily schedule

2. **Spark WordCount DAG** ([`dags/spark_wordcount.py`](dags/spark_wordcount.py))
   - PySpark job execution via BashOperator
   - Processes [`scripts/sample_text.txt`](scripts/sample_text.txt)
   - Manual trigger only



### Sample PySpark Job

The [`scripts/wordcount.py`](scripts/wordcount.py) demonstrates a complete PySpark application:

```python
# Example usage in DAG
spark_wordcount_task = BashOperator(
    task_id='spark_wordcount_task',
    bash_command="""
    rm -rf /opt/airflow/scripts/output &&
    spark-submit /opt/airflow/scripts/wordcount.py /opt/airflow/scripts/sample_text.txt /opt/airflow/scripts/output
    """,
)
```

## Deployment Components

### Kubernetes Resources

The [`templates/`](templates/) directory contains all Kubernetes manifests:

| Component | File | Purpose |
|-----------|------|---------|
| ConfigMap | [`airflow-configmap.yaml`](templates/airflow-configmap.yaml) | Airflow environment configuration |
| Init Job | [`airflow-init-job.yaml`](templates/airflow-init-job.yaml) | Database initialization and user creation |
| Scheduler | [`airflow-scheduler-deployment.yaml`](templates/airflow-scheduler-deployment.yaml) | Airflow scheduler deployment |
| Webserver | [`airflow-webserver-deployment.yaml`](templates/airflow-webserver-deployment.yaml) | Airflow webserver deployment |
| Service | [`airflow-webserver-service.yaml`](templates/airflow-webserver-service.yaml) | Webserver service exposure |
| PostgreSQL | [`postgresql-deployment.yaml`](templates/postgresql-deployment.yaml) | Database deployment |
| PostgreSQL Service | [`postgresql-service.yaml`](templates/postgresql-service.yaml) | Database service |
| Storage | [`postgresql-pvc.yaml`](templates/postgresql-pvc.yaml) | Persistent volume claim |

### Health Checks

All deployments include comprehensive health checks:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 120
  periodSeconds: 30

readinessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 60
  periodSeconds: 10
```

## Operations Guide

### Starting the Environment

```bash
# 1. Update configuration files (see Configuration section above)

# 2. Create cluster
kind create cluster --config kind-config.yaml --name airflow-cluster

# 3. Deploy application
helm install airflow-pyspark . -f values.yaml    

# 4. Wait for pods to be ready
kubectl wait --for=condition=ready pod --all --timeout=300s
```

### Monitoring

```bash
# Check pod status
kubectl get pods

# View logs
kubectl logs -f deployment/airflow-webserver
kubectl logs -f deployment/airflow-scheduler

# Check services
kubectl get services
```

### Accessing Components

```bash
# Airflow Web UI
open http://localhost:30080

# Port forward for direct access
kubectl port-forward svc/airflow-webserver 8080:8080

# Database access
kubectl port-forward svc/airflow-postgresql 5432:5432
```

### Updating DAGs

DAGs are automatically synchronized since they're mounted as volumes:

```bash
# Edit DAG files directly
vim dags/my_new_dag.py

# Changes are reflected immediately in Airflow
```

## Troubleshooting

### Common Issues

1. **Volume mount issues**
   ```bash
   # Check if paths exist and are accessible
   ls -la /Users/YOUR_USERNAME/airflow-pyspark-k8s/
   
   # Verify paths in values.yaml match your system
   ```

2. **Java architecture mismatch**
   ```bash
   # Check your CPU architecture
   uname -m
   
   # Verify Java path in configmap matches your architecture
   kubectl exec -it deployment/airflow-scheduler -- ls -la /usr/lib/jvm/
   ```

3. **Pods stuck in Pending state**
   ```bash
   kubectl describe pod <pod-name>
   # Check for resource constraints or volume mount issues
   ```

4. **Database connection failures**
   ```bash
   kubectl logs deployment/airflow-init-db
   # Verify PostgreSQL is running and accessible
   ```

5. **PySpark job failures**
   ```bash
   # Check Spark configuration in airflow-configmap.yaml
   kubectl logs deployment/airflow-scheduler
   ```

### Architecture-Specific Troubleshooting

**For Apple Silicon (M1/M2) users:**
```bash
# If experiencing image pull issues
docker pull --platform linux/arm64 lotfinejad/airflow-pyspark:latest
kind load docker-image lotfinejad/airflow-pyspark:latest --name airflow-cluster

# Verify Java path
kubectl exec -it deployment/airflow-scheduler -- ls -la /usr/lib/jvm/java-17-openjdk-arm64
```

**For Intel/AMD users:**
```bash
# If experiencing image pull issues  
docker pull --platform linux/amd64 lotfinejad/airflow-pyspark:latest
kind load docker-image lotfinejad/airflow-pyspark:latest --name airflow-cluster

# Verify Java path
kubectl exec -it deployment/airflow-scheduler -- ls -la /usr/lib/jvm/java-17-openjdk-amd64
```

### Log Locations

- **Airflow Logs**: [`logs/`](logs/) directory
- **Scheduler Logs**: `kubectl logs deployment/airflow-scheduler`
- **Webserver Logs**: `kubectl logs deployment/airflow-webserver`
- **Database Logs**: `kubectl logs deployment/airflow-postgresql`

### Cleanup

```bash
# Uninstall Helm release
helm uninstall airflow-pyspark

# Delete Kind cluster
kind delete cluster --name airflow-cluster

# Clean up volumes (if needed)
docker volume prune
```

## Development

### Custom Docker Image

The setup uses a custom image (`lotfinejad/airflow-pyspark:latest`) that includes:
- Apache Airflow 2.8.1
- PySpark integration
- Java 17 runtime (multi-architecture support)
- Required Python dependencies

### Adding New DAGs

1. Create DAG file in [`dags/`](dags/) directory
2. DAG will be automatically picked up by Airflow
3. Refresh the web UI to see new DAGs

### Modifying Configuration

1. Update [`values.yaml`](values.yaml) with your paths and settings
2. Update [`templates/airflow-configmap.yaml`](templates/airflow-configmap.yaml) for your architecture
3. Upgrade the Helm release:
   ```bash
   helm upgrade airflow-pyspark . --timeout 10m
   ```

## Pre-Deployment Checklist

Before running the deployment, ensure you have:

- [ ] Updated [`values.yaml`](values.yaml) with your username in volume paths
- [ ] Updated [`kind-config.yaml`](kind-config.yaml) with your username in hostPath
- [ ] Set correct `JAVA_HOME` in [`templates/airflow-configmap.yaml`](templates/airflow-configmap.yaml) for your CPU architecture
- [ ] Verified Docker is running
- [ ] Confirmed your current directory is the project root

## Resource Requirements

### Minimum Requirements
- **CPU**: 2 cores
- **Memory**: 4GB RAM
- **Storage**: 10GB available space

### Production Recommendations
- **CPU**: 4+ cores
- **Memory**: 8GB+ RAM
- **Storage**: 50GB+ SSD storage

## Security Considerations

### Default Credentials
⚠️ **Warning**: Change default credentials in production!

```yaml
# In airflow-init-job.yaml
airflow users create \
  --username admin \
  --password admin \  # Change this!
  --role Admin
```

### Database Security
- Use secrets instead of plain text passwords
- Enable SSL/TLS for database connections
- Implement network policies for pod-to-pod communication

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and test locally
4. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For issues and questions:
- Create an issue in the GitHub repository
- Check the troubleshooting section above
- Review Airflow and Kubernetes documentation