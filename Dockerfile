# Filename: Dockerfile
FROM apache/airflow:2.8.1

# Switch to root user to grant permissions if needed, then switch back
USER root
RUN chown -R airflow:root /opt/airflow/
USER airflow

# Copy the requirements file into the image
COPY requirements.txt /requirements.txt

# Install the Python packages from the requirements file
RUN pip install --no-cache-dir --user -r /requirements.txt