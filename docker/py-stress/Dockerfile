ARG PYTHON_IMAGE_TAG=3.14.3-slim-trixie

FROM python:$PYTHON_IMAGE_TAG AS python_packages
ARG PYTHON_DRIVER_VERSION=3.29.3
RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y --no-install-recommends build-essential
RUN pip install scylla-driver==$PYTHON_DRIVER_VERSION hdrhistogram==0.9.2

FROM python:$PYTHON_IMAGE_TAG
ENV PYTHONUNBUFFERED=1
COPY --from=python_packages /usr/local /usr/local
ADD --chmod=+x https://raw.githubusercontent.com/enaydanov/scylla-cluster-tests/pystress/utils/pystress.py /usr/local/bin/py-cassandra-stress
