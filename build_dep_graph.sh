#!/bin/bash
pydeps --include-missing --cluster --max-cluster-size=10 --keep-target-cluster -T png --rmprefix=megha_sim ./src/megha_sim -o megha_sim_dep_graph.png
