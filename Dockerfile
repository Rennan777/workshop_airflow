FROM astrocrpublic.azurecr.io/runtime:3.2-2

USER root

# 🔹 instalar Java + curl
RUN apt-get update && apt-get install -y default-jdk curl && \
    apt-get clean

# 🔹 instalar Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    | tar -xz -C /opt && \
    ln -s /opt/spark-3.5.0-bin-hadoop3 /opt/spark

# driver postgresql
COPY jars/postgresql-42.7.3.jar /opt/spark/jars/

# 🔹 env
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER astro