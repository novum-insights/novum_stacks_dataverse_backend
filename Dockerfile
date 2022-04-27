FROM python:3.8-slim-buster as base

ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

ENV AIRFLOW_HOME=/usr/local/airflow

RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

FROM base as builder

RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        locales \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && pip install -U pip setuptools wheel \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY requirements.txt /requirements.txt
RUN pip install --no-warn-script-location --prefix=/install -r /requirements.txt

FROM base

RUN set -ex \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        curl \
        rsync \
        netcat \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY --from=builder /install /usr/local

ENV AIRFLOW__CORE__EXECUTOR LocalExecutor
ENV POSTGRES_PORT 5432
ENV POSTGRES_USER YOUR_AIRFLOW_POSTGRES_USER
ENV POSTGRES_DB YOUR_AIRFLOW_POSTGRES_PASSWORD

COPY scripts/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY dags ${AIRFLOW_HOME}/dags
COPY etl ${AIRFLOW_HOME}/dags/etl
RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080
USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
