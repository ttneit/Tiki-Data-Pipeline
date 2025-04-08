# Use the official Apache Airflow image as a base
FROM apache/airflow:2.10.5

# Switch to root to install system packages
USER root

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Install SQL Server ODBC Driver + dependencies
RUN apt-get update && apt-get install -y \
    gnupg2 \
    curl \
    unixodbc \
    unixodbc-dev \
    gcc \
    build-essential

# Install Microsoft ODBC Driver 17 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Switch back to airflow user
USER airflow

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
