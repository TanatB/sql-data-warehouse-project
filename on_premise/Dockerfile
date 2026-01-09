# Custom Airflow image with weather_etl package
# ==============================================
#
# Build: docker build -t weather-airflow:latest .
# 
# This Dockerfile extends the official Apache Airflow image
# and installs the weather_etl package for use in DAGs.

FROM apache/airflow:3.1.0

# Switch to root to install system dependencies if needed
USER root

# Install any system-level dependencies here
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     some-package \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for pip installations
USER airflow

# Copy the package source code
COPY --chown=airflow:root src/ /opt/airflow/packages/weather_etl_src/src/
COPY --chown=airflow:root pyproject.toml /opt/airflow/packages/weather_etl_src/

# Create a minimal README to satisfy pyproject.toml
RUN echo "# Weather ETL Package" > /opt/airflow/packages/weather_etl_src/README.md

# Install the weather_etl package in editable mode for development
# Or use regular install for production: pip install /opt/airflow/packages/weather_etl_src
WORKDIR /opt/airflow/packages/weather_etl_src
RUN pip install --no-cache-dir -e .

# Optionally install additional Python packages
# RUN pip install --no-cache-dir some-other-package
RUN pip install --no-cache-dir \
    openmeteo-requests \
    openmeteo-sdk \
    requests-cache \
    retry-requests \
    pandas \
    psycopg2-binary

# Reset working directory
WORKDIR /opt/airflow

# Verify installation
RUN python -c "from weather_etl import OpenMeteoExtractor; print('weather_etl package installed successfully')"