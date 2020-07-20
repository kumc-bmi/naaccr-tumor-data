FROM jupyter/pyspark-notebook

RUN pip freeze

WORKDIR /home/jovyan

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ARG ARCHIVE

ADD build/${ARCHIVE} .

# TODO: prune Dockerfile