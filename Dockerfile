# Use the latest Python Image
FROM python:3.9.5-buster

WORKDIR /Megha_Dev/

COPY ./requirements.txt /Megha_Dev/requirements.txt

RUN pip3 install --no-cache-dir -r requirements.txt
