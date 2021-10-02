#!/bin/bash 
eksctl scale nodegroup --cluster edcm5k8s --name ng-edcm5k8s \
-r us-east-2 \
--nodes-min=3 --nodes-max=3 --nodes=3
