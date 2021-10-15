#!/bin/bash
docker build -t megha_dev_env .
docker run -it -d --rm --name megha_dev_env -v "${PWD}":/Megha_Dev megha_dev_env:latest bash
