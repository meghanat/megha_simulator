# Use the latest Python Image
FROM python:latest

WORKDIR /Megha_Dev/

COPY ./requirements.txt /Megha_Dev/requirements.txt

RUN pip3 install -r requirements.txt
