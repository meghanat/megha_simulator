#!/bin/bash

GM_APPROACH_CONTAINER=megha_dev_env
THOUSAND_NODE_CONFIG=./simulator_config/config_1000.json
TEN_THOUSAND_NODE_CONFIG=./simulator_config/config_10_000.json

docker exec -d megha_dev_env_1 pypy ./src/runner.py ./traces/input/YH1_sub.tr $THOUSAND_NODE_CONFIG 10 10 10 1 1 1
sleep 1s
docker exec -d megha_dev_env_2 pypy ./src/runner.py ./traces/input/YH2_sub.tr $THOUSAND_NODE_CONFIG 10 10 10 1 1 1
sleep 1s
# docker exec -d $GM_APPROACH_CONTAINER pypy ./src/runner.py ./traces/input/FB_sub.tr $THOUSAND_NODE_CONFIG 10 10 10 1 1 1
# sleep 1s
# ---

docker exec -d megha_dev_env_3 pypy ./src/runner.py ./traces/input/YH1_sub.tr $TEN_THOUSAND_NODE_CONFIG 10 10 100 1 1 1
sleep 1s
docker exec -d megha_dev_env_4 pypy ./src/runner.py ./traces/input/YH2_sub.tr $TEN_THOUSAND_NODE_CONFIG 10 10 100 1 1 1
sleep 1s
# docker exec -d $GM_APPROACH_CONTAINER pypy ./src/runner.py ./traces/input/FB_sub.tr $TEN_THOUSAND_NODE_CONFIG 10 10 100 1 1 1