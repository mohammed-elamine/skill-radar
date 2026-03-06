# docker/airflow/Dockerfile
# -----------------------------------------------------------------------------
# Custom Airflow image for Skill Radar orchestration
#
# Goals:
# - Airflow 2.9.3 base with LocalExecutor
# - DockerProvider for launching Spark tasks as ephemeral containers
# - boto3 for optional future S3/MinIO manifest sensors
# - No Spark/Java runtime — Airflow is control-plane only
# -----------------------------------------------------------------------------

FROM apache/airflow:2.9.3

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Airflow discovers DAGs from /opt/airflow/dags (mounted in compose)
