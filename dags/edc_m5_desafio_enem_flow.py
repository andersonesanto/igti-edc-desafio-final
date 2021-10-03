from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3


aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
glue = boto3.client('glue', region_name='us-east-2',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)

s3 = boto3.client('s3', region_name='us-east-2',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)


def create_and_trigger_crawler():
    try:
        glue.get_crawler(Name='m5_enem')
    except glue.exceptions.EntityNotFoundException:
        glue.create_crawler(
            Name='m5_enem',
            Role='arn:aws:iam::597495568095:role/santo-glue-service-role',
            DatabaseName='enem2019',
            Description='igti EDC m5 enem 2019',
            Targets={
                'S3Targets': [
                    {
                        'Path': 's3://datalake-edc-m5-597495568095/servicedata/enem/',
                        'Exclusions': []
                    },
                ]
            }
        )

    glue.start_crawler(
        Name='m5_enem'
    )


with DAG(
    'enem_job',
    default_args={
        'owner': 'Santo',
        'depends_on_past': False,
        'email': ['andersonesanto@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'max_active_runs': 1,
    },
    description='submit spark-pi as sparkApplication on kubernetes',
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'kubernetes', 'batch',
          'enem', 'igti', 'm5', 'desafio-final'],
) as dag:

    ingestion = KubernetesPodOperator(
        namespace='airflow',
        image="597495568095.dkr.ecr.us-east-2.amazonaws.com/igti-edc-m5-ingestion:1.0",
        cmds=["bash", "-c"],
        # arguments=["set -x ; curl https://download.inep.gov.br/microdados/microdados_educacao_superior_2019.zip -o mes2019.zip; unzip -j -d dados mes2019.zip -i '*.CSV' ; aws s3 cp dados s3://datalake-edc-m5-597495568095/rawdata/ --recursive"],
        arguments=["set -x ; curl https://web-597495568095.s3.us-east-2.amazonaws.com/microdados_educacao_superior_2019.zip -o mes2019.zip; unzip -j -d dados mes2019.zip -i '*.CSV' ; aws s3 cp dados s3://datalake-edc-m5-597495568095/rawdata/ --recursive"],
        name="ingestion",
        task_id="ingestion",
        image_pull_policy="Always",
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
    )

    ingestion_sensor = SparkKubernetesSensor(
        task_id='ingestion_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='ingestion')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )    

    converte_parquet = SparkKubernetesOperator(
        task_id='converte_parquet',
        namespace="airflow",
        application_file="enem_converte_parquet.yml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_parquet_sensor = SparkKubernetesSensor(
        task_id='converte_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    create_and_trigger_crawler_enem = PythonOperator(
        task_id='trigger_glue_crawler',
        python_callable=create_and_trigger_crawler
    )


ingestion \
>> ingestion_sensor \
>> converte_parquet \
>> converte_parquet_sensor \
>> create_and_trigger_crawler_enem
