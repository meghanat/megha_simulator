#!/bin/bash
docker build -t megha_dev_env_python -f Dockerfile_python
docker run -it --rm --name megha_dev_env_python -v "${PWD}":/Megha_Dev megha_dev_env_python:latest bash
