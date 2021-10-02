#!/bin/bash 
eksctl scale nodegroup --cluster igtik8s --name ng-igtik8s -r us-east-2 \
--nodes-min=0 --nodes-max=1 --nodes=0
