NAME: airflow
LAST DEPLOYED: Sat Sep 25 16:09:59 2021
NAMESPACE: airflow
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
Thank you for installing Apache Airflow 2.1.2!

Your release is named airflow.
You can now access your dashboard(s) by executing the following command(s) and visiting the corresponding port at localhost in your browser:

Airflow Webserver:     kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
Default Webserver (Airflow UI) Login credentials:
    username: andersonesanto
    password: admin
Default Postgres connection credentials:
    username: postgres
    password: postgres
    port: 5432

You can get Fernet Key value by running the following:

    echo Fernet Key: $(kubectl get secret --namespace airflow airflow-fernet-key -o jsonpath="{.data.fernet-key}" | base64 --decode)

WARNING:
    Kubernetes workers task logs may not persist unless you configure log persistence or remote logging!
    Logging options can be found at: https://airflow.apache.org/docs/helm-chart/stable/manage-logs.html
    (This warning can be ignored if logging is configured with environment variables or secrets backend)
