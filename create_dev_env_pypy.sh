#!/bin/bash
docker build -t megha_dev_env .

# 1st container
docker run -it -d --rm --name megha_dev_env_1 -v "${PWD}":/Megha_Dev megha_dev_env:latest bash

# 2nd container
docker run -it -d --rm --name megha_dev_env_2 -v "${PWD}":/Megha_Dev megha_dev_env:latest bash

# 3rd container
docker run -it -d --rm --name megha_dev_env_3 -v "${PWD}":/Megha_Dev megha_dev_env:latest bash

# 4th container
docker run -it -d --rm --name megha_dev_env_4 -v "${PWD}":/Megha_Dev megha_dev_env:latest bash
