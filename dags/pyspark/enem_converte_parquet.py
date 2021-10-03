from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import os


aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

# set conf
conf = (
    SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')
    # .set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    # .set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
    .set("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
    .set('spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version', '2')
    .set('spark.speculation', False)
)

# apply config
sc = SparkContext(conf=conf).getOrCreate()


if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
        .builder\
        .appName("ENEM Job")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = t.StructType([
        t.StructField('NU_ANO_CENSO', t.IntegerType()),
        t.StructField('CO_IES', t.IntegerType()),
        t.StructField('TP_CATEGORIA_ADMINISTRATIVA', t.IntegerType()),
        t.StructField('TP_ORGANIZACAO_ACADEMICA', t.IntegerType()),
        t.StructField('CO_CURSO', t.IntegerType()),
        t.StructField('CO_CURSO_POLO', t.IntegerType()),
        t.StructField('TP_TURNO', t.IntegerType()),
        t.StructField('TP_GRAU_ACADEMICO', t.IntegerType()),
        t.StructField('TP_MODALIDADE_ENSINO', t.IntegerType()),
        t.StructField('TP_NIVEL_ACADEMICO', t.IntegerType()),
        t.StructField('CO_CINE_ROTULO', t.StringType()),
        t.StructField('ID_ALUNO', t.StringType()),
        t.StructField('CO_ALUNO_CURSO', t.IntegerType()),
        t.StructField('CO_ALUNO_CURSO_ORIGEM', t.IntegerType()),
        t.StructField('TP_COR_RACA', t.IntegerType()),
        t.StructField('TP_SEXO', t.IntegerType()),
        t.StructField('NU_ANO_NASCIMENTO', t.IntegerType()),
        t.StructField('NU_MES_NASCIMENTO', t.IntegerType()),
        t.StructField('NU_DIA_NASCIMENTO', t.IntegerType()),
        t.StructField('NU_IDADE', t.IntegerType()),
        t.StructField('TP_NACIONALIDADE', t.IntegerType()),
        t.StructField('CO_PAIS_ORIGEM', t.IntegerType()),
        t.StructField('CO_UF_NASCIMENTO', t.IntegerType()),
        t.StructField('CO_MUNICIPIO_NASCIMENTO', t.IntegerType()),
        t.StructField('IN_DEFICIENCIA', t.IntegerType()),
        t.StructField('IN_DEFICIENCIA_AUDITIVA', t.IntegerType()),
        t.StructField('IN_DEFICIENCIA_FISICA', t.IntegerType()),
        t.StructField('IN_DEFICIENCIA_INTELECTUAL', t.IntegerType()),
        t.StructField('IN_DEFICIENCIA_MULTIPLA', t.IntegerType()),
        t.StructField('IN_DEFICIENCIA_SURDEZ', t.IntegerType()),
        t.StructField('IN_DEFICIENCIA_SURDOCEGUEIRA', t.IntegerType()),
        t.StructField('IN_DEFICIENCIA_BAIXA_VISAO', t.IntegerType()),
        t.StructField('IN_DEFICIENCIA_CEGUEIRA', t.IntegerType()),
        t.StructField('IN_DEFICIENCIA_SUPERDOTACAO', t.IntegerType()),
        t.StructField('IN_TGD_AUTISMO', t.IntegerType()),
        t.StructField('IN_TGD_SINDROME_ASPERGER', t.IntegerType()),
        t.StructField('IN_TGD_SINDROME_RETT', t.IntegerType()),
        t.StructField('IN_TGD_TRANSTOR_DESINTEGRATIVO', t.IntegerType()),
        t.StructField('TP_SITUACAO', t.IntegerType()),
        t.StructField('QT_CARGA_HORARIA_TOTAL', t.IntegerType()),
        t.StructField('QT_CARGA_HORARIA_INTEG', t.IntegerType()),
        t.StructField('DT_INGRESSO_CURSO', t.IntegerType()),
        t.StructField('IN_INGRESSO_VESTIBULAR', t.IntegerType()),
        t.StructField('IN_INGRESSO_ENEM', t.IntegerType()),
        t.StructField('IN_INGRESSO_AVALIACAO_SERIADA', t.IntegerType()),
        t.StructField('IN_INGRESSO_SELECAO_SIMPLIFICA', t.IntegerType()),
        t.StructField('IN_INGRESSO_OUTRO_TIPO_SELECAO', t.IntegerType()),
        t.StructField('IN_INGRESSO_VAGA_REMANESC', t.IntegerType()),
        t.StructField('IN_INGRESSO_VAGA_PROG_ESPECIAL', t.IntegerType()),
        t.StructField('IN_INGRESSO_TRANSF_EXOFFICIO', t.IntegerType()),
        t.StructField('IN_INGRESSO_DECISAO_JUDICIAL', t.IntegerType()),
        t.StructField('IN_INGRESSO_CONVENIO_PECG', t.IntegerType()),
        t.StructField('IN_INGRESSO_EGRESSO', t.IntegerType()),
        t.StructField('IN_INGRESSO_OUTRA_FORMA', t.IntegerType()),
        t.StructField('IN_RESERVA_VAGAS', t.IntegerType()),
        t.StructField('IN_RESERVA_ETNICO', t.IntegerType()),
        t.StructField('IN_RESERVA_DEFICIENCIA', t.IntegerType()),
        t.StructField('IN_RESERVA_ENSINO_PUBLICO', t.IntegerType()),
        t.StructField('IN_RESERVA_RENDA_FAMILIAR', t.IntegerType()),
        t.StructField('IN_RESERVA_OUTRA', t.IntegerType()),
        t.StructField('IN_FINANCIAMENTO_ESTUDANTIL', t.IntegerType()),
        t.StructField('IN_FIN_REEMB_FIES', t.IntegerType()),
        t.StructField('IN_FIN_REEMB_ESTADUAL', t.IntegerType()),
        t.StructField('IN_FIN_REEMB_MUNICIPAL', t.IntegerType()),
        t.StructField('IN_FIN_REEMB_PROG_IES', t.IntegerType()),
        t.StructField('IN_FIN_REEMB_ENT_EXTERNA', t.IntegerType()),
        t.StructField('IN_FIN_REEMB_OUTRA', t.IntegerType()),
        t.StructField('IN_FIN_NAOREEMB_PROUNI_INTEGR', t.IntegerType()),
        t.StructField('IN_FIN_NAOREEMB_PROUNI_PARCIAL', t.IntegerType()),
        t.StructField('IN_FIN_NAOREEMB_ESTADUAL', t.IntegerType()),
        t.StructField('IN_FIN_NAOREEMB_MUNICIPAL', t.IntegerType()),
        t.StructField('IN_FIN_NAOREEMB_PROG_IES', t.IntegerType()),
        t.StructField('IN_FIN_NAOREEMB_ENT_EXTERNA', t.IntegerType()),
        t.StructField('IN_FIN_NAOREEMB_OUTRA', t.IntegerType()),
        t.StructField('IN_APOIO_SOCIAL', t.IntegerType()),
        t.StructField('IN_APOIO_ALIMENTACAO', t.IntegerType()),
        t.StructField('IN_APOIO_BOLSA_PERMANENCIA', t.IntegerType()),
        t.StructField('IN_APOIO_BOLSA_TRABALHO', t.IntegerType()),
        t.StructField('IN_APOIO_MATERIAL_DIDATICO', t.IntegerType()),
        t.StructField('IN_APOIO_MORADIA', t.IntegerType()),
        t.StructField('IN_APOIO_TRANSPORTE', t.IntegerType()),
        t.StructField('IN_ATIVIDADE_EXTRACURRICULAR', t.IntegerType()),
        t.StructField('IN_COMPLEMENTAR_ESTAGIO', t.IntegerType()),
        t.StructField('IN_COMPLEMENTAR_EXTENSAO', t.IntegerType()),
        t.StructField('IN_COMPLEMENTAR_MONITORIA', t.IntegerType()),
        t.StructField('IN_COMPLEMENTAR_PESQUISA', t.IntegerType()),
        t.StructField('IN_BOLSA_ESTAGIO', t.IntegerType()),
        t.StructField('IN_BOLSA_EXTENSAO', t.IntegerType()),
        t.StructField('IN_BOLSA_MONITORIA', t.IntegerType()),
        t.StructField('IN_BOLSA_PESQUISA', t.IntegerType()),
        t.StructField('TP_ESCOLA_CONCLUSAO_ENS_MEDIO', t.IntegerType()),
        t.StructField('IN_ALUNO_PARFOR', t.IntegerType()),
        t.StructField('TP_SEMESTRE_CONCLUSAO', t.IntegerType()),
        t.StructField('TP_SEMESTRE_REFERENCIA', t.IntegerType()),
        t.StructField('IN_MOBILIDADE_ACADEMICA', t.IntegerType()),
        t.StructField('TP_MOBILIDADE_ACADEMICA', t.IntegerType()),
        t.StructField('TP_MOBILIDADE_ACADEMICA_INTERN', t.IntegerType()),
        t.StructField('CO_IES_DESTINO', t.IntegerType()),
        t.StructField('CO_PAIS_DESTINO', t.IntegerType()),
        t.StructField('IN_MATRICULA', t.IntegerType()),
        t.StructField('IN_CONCLUINTE', t.IntegerType()),
        t.StructField('IN_INGRESSO_TOTAL', t.IntegerType()),
        t.StructField('IN_INGRESSO_VAGA_NOVA', t.IntegerType()),
        t.StructField('IN_INGRESSO_PROCESSO_SELETIVO', t.IntegerType()),
        t.StructField('NU_ANO_INGRESSO', t.IntegerType()),
    ])

    df_aluno = (
        spark
        .read
        .format("csv")
        .options(header='true', delimiter='|', encoding='ISO-8859-1', inferschema=True)
        .load("s3a://datalake-edc-m5-597495568095/rawdata/enem/SUP_ALUNO_2019.CSV")
    )

    df_aluno.printSchema()

    (df_aluno
     .write
     .mode("overwrite")
     .format("parquet")
     .save("s3a://datalake-edc-m5-597495568095/trusteddata/enem/aluno/")
     )
     
    print("**************************")
    print("ALUNO Escrito com sucesso!")
    print("**************************")

    df_curso = (
        spark
        .read
        .format("csv")
        .options(header='true', delimiter='|', encoding='ISO-8859-1', inferschema=True)
        .load("s3a://datalake-edc-m5-597495568095/rawdata/enem/SUP_CURSO_2019.CSV")
    )

    df_curso.printSchema()

    (df_curso
     .write
     .mode("overwrite")
     .format("parquet")
     .save("s3a://datalake-edc-m5-597495568095/trusteddata/enem/curso/")
     )
     
    print("**************************")
    print("CURSO Escrito com sucesso!")
    print("**************************")    


    df_docente = (
        spark
        .read
        .format("csv")
        .options(header='true', delimiter='|', encoding='ISO-8859-1', inferschema=True)
        .load("s3a://datalake-edc-m5-597495568095/rawdata/enem/SUP_DOCENTE_2019.CSV")
    )

    df_docente.printSchema()

    (df_docente
     .write
     .mode("overwrite")
     .format("parquet")
     .save("s3a://datalake-edc-m5-597495568095/trusteddata/enem/docente/")
     )
     
    print("**************************")
    print("DOCENTE Escrito com sucesso!")
    print("**************************")


    df_ies = (
        spark
        .read
        .format("csv")
        .options(header='true', delimiter='|', encoding='ISO-8859-1', inferschema=True)
        .load("s3a://datalake-edc-m5-597495568095/rawdata/enem/SUP_IES_2019.CSV")
    )

    df_ies.printSchema()

    (df_ies
     .write
     .mode("overwrite")
     .format("parquet")
     .save("s3a://datalake-edc-m5-597495568095/trusteddata/enem/ies/")
     )
     
    print("**************************")
    print("IES Escrito com sucesso!")
    print("**************************")

    df_local_oferta = (
        spark
        .read
        .format("csv")
        .options(header='true', delimiter='|', encoding='ISO-8859-1', inferschema=True)
        .load("s3a://datalake-edc-m5-597495568095/rawdata/enem/SUP_LOCAL_OFERTA_2019.CSV")
    )

    df_local_oferta.printSchema()

    (df_local_oferta
     .write
     .mode("overwrite")
     .format("parquet")
     .save("s3a://datalake-edc-m5-597495568095/trusteddata/enem/local_oferta/")
     )
     
    print("*********************************")
    print("LOCAL_OFERTA Escrito com sucesso!")
    print("*********************************")

    df_aux_cine_brasil = (
        spark
        .read
        .format("csv")
        .options(header='true', delimiter='|', encoding='ISO-8859-1', inferschema=True)
        .load("s3a://datalake-edc-m5-597495568095/rawdata/enem/TB_AUX_CINE_BRASIL_2019.CSV")
    )

    df_aux_cine_brasil.printSchema()

    (df_aux_cine_brasil
     .write
     .mode("overwrite")
     .format("parquet")
     .save("s3a://datalake-edc-m5-597495568095/trusteddata/enem/aux_cine_brasil/")
     )
     
    print("************************************")
    print("AUX_CINE_BRASIL Escrito com sucesso!")
    print("************************************")

    spark.stop()
