#--------------------------------------------------------
# CSV 파일을 Hive External Table로 생성하기, Sohee Park
#--------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import configparser
from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark.sql.types import StructType, StructField

cloudPath='s3a://go01-demo/sohee'

print("Running as DataLake_Users: ", cloudPath)

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------
spark = SparkSession.builder.appName('INGEST').config("spark.kubernetes.access.hadoopFileSystems", "s3a://go01-demo-aws").getOrCreate()

#-----------------------------------------------------------------------------------
# go01-demo-aws 데이터레이크에 데이터 저장하려고 함.
#-----------------------------------------------------------------------------------
schema = StructType([ \
                     StructField("seq", StringType(), True), \
                     StructField("sigungu", StringType(), True), \
                     StructField("bunji", StringType(), True), \
                     StructField("o_bunji", StringType(), True), \
                     StructField("s_bunji", StringType(), True), \
                     StructField("danji", StringType(), True), \
                     StructField("area", StringType(), True), \
                     StructField("c_yymm", StringType(), True), \
                     StructField("c_dd", StringType(), True), \
                     StructField("amount", IntegerType(), True), \
                     StructField("block", StringType(), True), \
                     StructField("ho", StringType(), True), \
                     StructField("s_party", StringType(), True), \
                     StructField("b_party", StringType(), True), \
                     StructField("c_year", StringType(), True), \
                     StructField("road", StringType(), True), \
                     StructField("c_date", StringType(), True), \
                     StructField("tx_type", StringType(), True), \
                     StructField("agency_loc", StringType(), True), \
                     StructField("reg_date", DateType(), True)])
                     
SEOUL = spark.read.schema(schema).csv("s3a://go01-demo/sohee/seoul.csv")
KG = spark.read.schema(schema).csv("s3a://go01-demo/sohee/kg/kg.csv")
#apt2024  = spark.read.csv(cloudPath + "/apt.csv",  header=false, inferSchema=True)


print("CSV 파일을 읽어서 Exter Table 생성하기 - 데이터베이스, 테이블 생성")
##  DB생성, 테이블 생성, 건수 조회하기
spark.sql("DROP DATABASE IF EXISTS aptdemo CASCADE")

spark.sql("CREATE DATABASE IF NOT EXISTS aptdemo")

#SEOUL.write.mode("overwrite").partitionBy("c_yymm").saveAsTable('aptdemo.SEOUL', format="parquet")
#KG.write.mode("overwrite").partitionBy("c_yymm").saveAsTable('aptdemo.KG', format="parquet")
SEOUL.write.mode("overwrite").saveAsTable('aptdemo.SEOUL', format="parquet")
KG.write.mode("overwrite").saveAsTable('aptdemo.KG', format="parquet")
print("서울 거래수")
spark.sql("SELECT count(1) FROM aptdemo.SEOUL").show()

print("경기도 거래수")
spark.sql("SELECT count(1) FROM aptdemo.KG").show()

print("SEOUL, KG 테이블 생성 및 건수 확인")
