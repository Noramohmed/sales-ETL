FROM apache/airflow:2.10.4

# Ensure airflow CLI is found even when running as root (airflow-init uses user 0:0)
ENV PATH="/home/airflow/.local/bin:${PATH}"

USER root

# Install Docker CLI so BashOperator can run "docker exec" commands
RUN curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz" \
    | tar xz --strip-components=1 -C /usr/local/bin docker/docker \
    && chmod +x /usr/local/bin/docker

USER airflow

# Python packages needed by DAG tasks
# NOTE: kafka-python-ng is the maintained fork of kafka-python (works with Python 3.12)
# NOTE: apache-airflow-providers-apache-kafka is NOT installed here to avoid
#       dependency conflicts that break the airflow CLI
RUN pip install --no-cache-dir \
    kafka-python-ng \
    pandas
