FROM apache/airflow:3.3.1
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    zip \
    unzip \
    rclone \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
# RUN apt install unzip
# RUN curl https://rclone.org/install.sh | sudo bash
USER airflow

