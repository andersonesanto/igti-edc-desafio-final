# prepara o ambiente para airflow e executa a instalação
kubectl create namespace airflow
kubectl apply -f airflow/rolebinding_for_airflow.yaml
kubectl create secret generic aws-credentials --from-literal=aws_access_key_id=$AWS_ACCESS_KEY_ID --from-literal=aws_secret_access_key=$AWS_SECRET_ACCESS_KEY -n airflow
helm install airflow apache-airflow/airflow -f airflow/myvalues.yaml -n airflow --debug