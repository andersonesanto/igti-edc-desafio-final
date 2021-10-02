#!/bin/bash 
eksctl scale nodegroup --cluster igtik8s --name ng-igtik8s \
-r us-east-2 \
--nodes-min=3 --nodes-max=3 --nodes=3
