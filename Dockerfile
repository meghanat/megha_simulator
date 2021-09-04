# Use the latest Python Image
FROM python:buster

WORKDIR /Megha_Dev/

ENV PYTHONPATH="$PYTHONPATH:./src/megha_sim"

# This is need for creating the dependency graph of the project
RUN apt-get update && apt-get install -y --no-install-recommends \
    graphviz \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt /Megha_Dev/requirements.txt

RUN pip3 install --no-cache-dir -r requirements.txt
