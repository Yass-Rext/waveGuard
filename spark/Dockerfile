# spark/Dockerfile
FROM apache/spark:3.5.0

USER root

# ============================================================
# Installation des dépendances système
# ============================================================

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        python3-dev \
        libpq-dev \
        && rm -rf /var/lib/apt/lists/*

# ============================================================
# Installation des packages Python (sans confluent-kafka)
# ============================================================

RUN pip3 install --no-cache-dir \
    pyspark \
    psycopg2-binary \
    boto3 \
    prometheus-client \
    pg8000

# ============================================================
# Vérification des installations
# ============================================================

RUN python3 -c "import pyspark; print('✅ PySpark OK')" && \
    python3 -c "import psycopg2; print('✅ Psycopg2 OK')" && \
    python3 -c "import boto3; print('✅ Boto3 OK')" && \
    python3 -c "import prometheus_client; print('✅ Prometheus Client OK')" && \
    python3 -c "import pg8000; print('✅ Pg8000 OK')"

# ============================================================
# Répertoire de travail
# ============================================================

WORKDIR /opt/spark/work-dir

USER spark

CMD ["tail", "-f", "/dev/null"]