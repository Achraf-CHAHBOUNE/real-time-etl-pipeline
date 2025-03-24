# Use a Python base image
FROM apache/airflow:2.1.0-python3.8

# Install any additional dependencies
USER root
RUN pip install -r /requirements.txt

# Copy DAGs and other necessary files
COPY dags /opt/airflow/dags

# Set entrypoint for Airflow
ENTRYPOINT [ "bash", "-c", "airflow webserver" ]
