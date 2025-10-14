# Use Apache Airflow official image
FROM apache/airflow:3.0.1

# Set working directory
WORKDIR /opt/airflow

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your project into the container
COPY . .

# Optional: set environment variables
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
