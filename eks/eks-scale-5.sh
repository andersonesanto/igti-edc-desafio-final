#!/bin/bash 
eksctl scale nodegroup --cluster igtik8s -r us-east-2 \
--nodes-min=3 --nodes-max=5 --nodes=5