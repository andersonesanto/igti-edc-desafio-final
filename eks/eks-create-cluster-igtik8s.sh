#!/bin/bash

# eksctl create cluster --name=igtik8s --managed --spot --instance-types=m5.xlarge \
#    --nodes=2 --alb-ingress-access --node-private-networking --region=us-east-2 \
#    --nodes-min=2 --nodes-max=3 --full-ecr-access --asg-access --nodegroup-name=ng-igtik8s

# o número mínimo de nodes para rodar airflow é 3
source ~/.aws/export-credentials.sh

aws s3 mb s3://datalake-edc-m5-597495568095

eksctl create cluster --name=igtik8s --managed --spot --instance-types=m5.xlarge \
    --nodes=3 --alb-ingress-access --node-private-networking --region=us-east-2 \
    --nodes-min=3 --nodes-max=3 --full-ecr-access --asg-access --nodegroup-name=ng-igtik8s
