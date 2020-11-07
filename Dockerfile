FROM jupyter/datascience-notebook

USER root

RUN apt update && apt install -y \
    libfreetype6 \
    libfreetype6-dev \
    libgeos-dev \
    libpng-dev \
    libpq-dev \
    pkg-config

USER $NB_UID

ADD requirements.txt .

RUN pip install -U pip

RUN pip install -r ./requirements.txt --use-feature=2020-resolver
