#!/bin/bash
pydeps --include-missing --cluster --max-cluster-size=10 --keep-target-cluster -T png --rmprefix=src src
