#!/bin/bash
python3 ./src/runner.py ./traces/input/YH_small.tr ./simulator_config/config_original.json 3 2 3 1 1 1 
python3 ./src/runner.py ./traces/input/YH_small.tr ./simulator_config/config_original.json 3 2 3 1 1 1 > traces/output/YH_OP.tr


# Run the below command:
python3 ./src/runner.py ./traces/input/YH_small.tr ./simulator_config/config_1000.json 10 10 10 1 1 1
python3 ./src/runner.py ./traces/input/GOOG.tr ./simulator_config/config_10_000.json 10 10 100 1 1 1