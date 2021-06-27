#!/bin/bash
pydeps --include-missing --noshow --cluster --max-cluster-size=10 --keep-target-cluster -T png ./src/megha_sim -o ./media/images/megha_sim_dep_graph.png
