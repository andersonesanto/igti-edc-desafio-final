from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
# set conf
conf = (
    SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    # .set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    # .set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
    # .set("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
)

# apply config
sc = SparkContext(conf=conf).getOrCreate()


if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
        .builder\
        .appName("ENADE Job")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("DEBUG")

    df = (
        spark
        .read
        .format("csv")
        .options(header='true', inferSchema='true', delimiter=';')
        .load("s3a://m4-597495568095/landing-zone/enade/")
    )

    df.printSchema()

    (df
     .write
     .mode("overwrite")
     .format("parquet")
     .save("s3a://m4-597495568095/processing-zone/enade/")
     )

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()
