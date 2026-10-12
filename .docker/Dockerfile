FROM python:3.13-slim-trixie

ENV TZ=America/Bogota \
    SPARK_HOME=/usr/local/lib/python3.13/site-packages/pyspark \
    PYSPARK_PYTHON=/usr/local/bin/python \
    PATH=/usr/local/lib/python3.13/site-packages/pyspark/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless procps curl git tini libblas3 liblapack3 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
CMD ["sleep", "infinity"]
