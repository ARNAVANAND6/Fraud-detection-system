# Spark 3.5 + Delta Lake 3.2 + Kafka connector. The connector jars are
# downloaded on first start into the ivy cache volume and reused afterwards.
FROM apache/spark:3.5.3-python3
USER root
RUN pip install --no-cache-dir delta-spark==3.2.1
ENV SPARK_PACKAGES=io.delta:delta-spark_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
RUN mkdir -p /home/spark/.ivy2 && chown -R spark /home/spark && mkdir -p /data/lake && chown -R spark /data
WORKDIR /opt/job
COPY services/streaming/stream_job.py services/streaming/reconcile.py ./
USER spark
ENV HOME=/home/spark
CMD /opt/spark/bin/spark-submit --master local[*] --driver-memory 2g \
    --conf spark.jars.ivy=/home/spark/.ivy2 --packages $SPARK_PACKAGES stream_job.py
