FROM apache/spark:3.5.0

USER root

# Python
RUN pip3 install --no-cache-dir \
    pyspark \
    confluent-kafka \
    boto3 \
    prometheus-client

WORKDIR /opt/spark/work-dir

CMD ["tail", "-f", "/dev/null"]