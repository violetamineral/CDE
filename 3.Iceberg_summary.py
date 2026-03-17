#--------------------------------------------------------
# Iceberg Table로 생성하기, Sohee Park
#--------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark.sql.types import StructType, StructField

print("통계정보구하기")
spark = SparkSession.builder.appName('INGEST').config("spark.kubernetes.access.hadoopFileSystems", "s3a://go01-demo-aws").getOrCreate()

spark.sql("drop table if exists aptdemo.summary")

#--통계정보
spark.sql("CREATE TABLE aptdemo.summary \
        USING iceberg AS \
        select gubun, si, gu, dong, li, danji, round(area*0.3025, 0) area, c_yymm, \
                max(amount) max_amount, min(amount) min_amount, round(avg(amount), 0) avg_amount, \
                max(amount) - min(amount) gap, count(amount) cnt \
        from aptdemo.apt \
        group by gubun, si, gu, dong, li, danji, round(area*0.3025,0), c_yymm")

spark.sql("select * from aptdemo.summary limit 10").show()

print("통계정보 테이블 생성 완료 - aptdemo.SUMMARY")