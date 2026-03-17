#--------------------------------------------------------
# Iceberg Table로 생성하기, Sohee Park
#--------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F


print("서울지역 및 경기 지역 데이터를 합한 Iceberg 테이블 생성하기 ")
spark = SparkSession.builder.appName('INGEST').config("spark.kubernetes.access.hadoopFileSystems", "s3a://go01-demo-aws").getOrCreate()

spark.sql("drop table if exists aptdemo.apt")

#---서울 구분
print("서울지역 데이터 입력")
spark.sql("CREATE TABLE aptdemo.apt \
        USING iceberg  TBLPROPERTIES ('format-version' = '2') AS\
        SELECT split(sigungu, ' ')[0] as gubun, \
                        split(sigungu, ' ')[0] as si, \
                        split(sigungu, ' ')[1] as gu, \
                        split(sigungu, ' ')[2] as dong, '' as li, \
                        to_date(substr(c_yymm, 1, 4) || '-' || substr(c_yymm, 5,2) || '-' ||c_dd) as s_year, * \
        FROM aptdemo.seoul ")

spark.sql("select * from aptdemo.apt limit 10").show()


#---경기도 구분
#print("경기지역 데이터 입력")
#spark.sql("INSERT INTO aptdemo.apt \
#            SELECT split(sigungu, ' ')[0] as gubun, \
#                    split(sigungu, ' ')[1] as si, \
#                    split(sigungu, ' ')[2] as gu, \
#                    split(sigungu, ' ')[3] as dong,  * \
#            FROM aptdemo.kg")

#---경기도 구분 - 도, 시, 동
spark.sql("INSERT INTO aptdemo.apt \
            SELECT split(sigungu, ' ')[0] as gubun, \
                    split(sigungu, ' ')[1] as si, split(sigungu, ' ')[1] as gu, \
                    split(sigungu, ' ')[2] as dong, '' as li, to_date(substr(c_yymm, 1, 4) || '-' || substr(c_yymm, 5,2) || '-' ||c_dd) as s_year, *  \
            FROM aptdemo.kg \
            WHERE split(sigungu, ' ')[3] is null ")
            
#---경기도 구분 - 도, 시, 구, 동
spark.sql("INSERT INTO aptdemo.apt \
          SELECT split(sigungu, ' ')[0] as gubun, \
                    split(sigungu, ' ')[1] as si, \
                    split(sigungu, ' ')[2] as gu, \
                    split(sigungu, ' ')[3] as dong, \
                    '' as li, to_date(substr(c_yymm, 1, 4) || '-' || substr(c_yymm, 5,2) || '-' ||c_dd) as s_year,  * \
            FROM aptdemo.kg \
            WHERE split(sigungu, ' ')[3] is not null and split(sigungu, ' ')[4] is null \
                and split(sigungu, ' ')[1] in ('수원시','고양시','부천시','성남시','안산시','용인시')")
            
#---경기도 구분 - 도, 시(군), 동(읍)), 리
spark.sql("INSERT INTO aptdemo.apt \
          SELECT split(sigungu, ' ')[0] as gubun, \
                    split(sigungu, ' ')[1] as si, \
                    split(sigungu, ' ')[1] as gu, \
                    split(sigungu, ' ')[2] as dong, \
                    split(sigungu, ' ')[3] as li, to_date(substr(c_yymm, 1, 4) || '-' || substr(c_yymm, 5,2) || '-' ||c_dd) as s_year, * \
            FROM aptdemo.kg \
            WHERE split(sigungu, ' ')[3] is not null and split(sigungu, ' ')[4] is null \
                and split(sigungu, ' ')[1] in ('양평군','양주시','가평군', '광주시', '김포시','남양주시', '안성시','여주시','연천군','이천시','파주시','평택시','화성시')")

 #---경기도 구분 - 도, 시, 구, 동, 리
spark.sql("INSERT INTO aptdemo.apt \
          SELECT split(sigungu, ' ')[0] as gubun, \
                    split(sigungu, ' ')[1] as si, \
                    split(sigungu, ' ')[2] as gu, \
                    split(sigungu, ' ')[3] as dong, \
                    split(sigungu, ' ')[4] as li, to_date(substr(c_yymm, 1, 4) || '-' || substr(c_yymm, 5,2) || '-' ||c_dd) as s_year, * \
            FROM aptdemo.kg \
            WHERE split(sigungu, ' ')[3] is not null and split(sigungu, ' ')[4] is not null")

print("건수많은 곳 조회")
spark.sql("select dong, danji, count(1) as cnt from aptdemo.apt where si = '서울특별시' group by dong, danji order by cnt desc limit 10").show()

print("테이블 생성 완료 - aptdemo.APT")