# -*- coding: utf-8 -*-

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType


#Inicializa sessão spark
spark = SparkSession \
        .builder \
        .appName("Processa arquivo de dados exemplo") \
        .getOrCreate()

to_value = lambda v: float(v.replace(",","."))
udf_to_value = F.udf(to_value, pyspark.sql.types.FloatType())

df=spark.read.csv("/tmp/spark/201302_Diarias.utf8.csv",header=True,sep="\t")

df2=df.withColumn("value", udf_to_value(df["Valor Pagamento"])) \
    .withColumn("Dtpg", F.to_date(df["Data Pagamento"], format="dd/MM/yyyy"))

df3=df2.select(df2["Nome Órgão Superior"].alias("Orgao"),df2["Data Pagamento"].alias("DtPg"))

df3.show()
