#!/bin/bash

kubectl create namespace processing
kubectl create serviceaccount spark -n processing
kubectl create clusterrolebinding spark-role-binding --clusterrole=edit --serviceaccount processing:spark -n processing

helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install spark spark-operator/spark-operator --namespace processing --debug