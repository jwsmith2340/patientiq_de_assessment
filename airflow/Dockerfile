FROM redhat/ubi8

USER 0

RUN yum update -y
RUN yum install -y sudo unzip libnss3.so

RUN cd /usr/lib

RUN yes "y" | dnf install wget yum-utils make gcc openssl-devel bzip2-devel libffi-devel zlib-devel && \
    wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz

RUN tar xzf Python-3.11.9.tgz && \
    rm -r Python-3.11.9.tgz

RUN cd Python-3.11.9 && \
    ./configure --with-system-ffi --with-computed-gotos --enable-loadable-sqlite-extensions && \
    make && \
    make altinstall

ARG AIRFLOW_HOME=/opt/airflow

ENV AIRFLOW_HOME=${AIRFLOW_HOME}
ENV PYTHONPATH=${AIRFLOW_HOME}

RUN mkdir -pv ${AIRFLOW_HOME} && \
mkdir -pv ${AIRFLOW_HOME}/dags && \
mkdir -pv ${AIRFLOW_HOME}/logs && \
mkdir -pv ${AIRFLOW_HOME}/files && \
mkdir -pv ${AIRFLOW_HOME}/config && \
mkdir -pv ${AIRFLOW_HOME}/plugins && \
mkdir -pv ${AIRFLOW_HOME}/resources && \
chmod -R 777 ${AIRFLOW_HOME}

COPY ./dags /opt/airflow/dags
COPY ./airflow.cfg /opt/airflow/airflow.cfg
COPY ./requirements.txt /opt/airflow/requirements.txt
COPY ./movies_metadata.csv /opt/airflow/flat_files/movies_metadata.csv

RUN python3.11 -m venv /opt/airflow/airflow_venv
RUN /opt/airflow/airflow_venv/bin/pip install --upgrade pip
RUN /opt/airflow/airflow_venv/bin/pip install -r /opt/airflow/requirements.txt
RUN /opt/airflow/airflow_venv/bin/pip install "apache-airflow[celery,postgres,redis,crypto]==2.10.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt"

CMD ["sleep","3600"]
