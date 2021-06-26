#!/bin/bash
python3 ./src/runner.py ./traces/input/YH.tr ./simulator_config/config.json 3 2 3 1 1 1 
python3 ./src/runner.py ./traces/input/YH.tr ./simulator_config/config.json 3 2 3 1 1 1 > traces/output/YH_OP.tr
