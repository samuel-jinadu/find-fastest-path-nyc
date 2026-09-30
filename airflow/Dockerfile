FROM apache/airflow:3.1.2

USER root

# System deps (only if wheels are missing; usually not needed for Python 3.12)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     gcc g++ libgdal-dev libpq-dev && \
#     apt-get clean && rm -rf /var/lib/apt/lists/*


USER airflow

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    geopandas \
    minio \
    requests \
    apache-airflow-providers-amazon \
    apache-airflow-providers-postgres

# Dev/test deps — installed into the venv the image's python already uses
COPY --chown=airflow:root requirements-dev.txt /opt/airflow/nyctransport/requirements-dev.txt
RUN pip install --no-cache-dir pytest==8.3.2 pytest-mock==3.14.0

# Package
COPY --chown=airflow:root src /opt/airflow/nyctransport/src
COPY --chown=airflow:root setup.py /opt/airflow/nyctransport/setup.py
RUN pip install --no-cache-dir /opt/airflow/nyctransport