FROM jupyter/datascience-notebook

USER root

RUN apt update && apt install -y \
    libfreetype6* \
    libgeos-dev \
    libpng-dev \
    libpq-dev \
    pkg-config

USER $NB_UID

ADD requirements.txt .

RUN pip install -r ./requirements.txt
