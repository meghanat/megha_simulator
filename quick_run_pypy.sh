#!/bin/bash
pypy ./src/runner.py ./traces/input/YH_small.tr ./simulator_config/config_original.json 3 2 3 1 1 1 
pypy ./src/runner.py ./traces/input/YH_small.tr ./simulator_config/config_original.json 3 2 3 1 1 1 > traces/output/YH_OP.tr
