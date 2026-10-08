from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("CDE Spark Test") \
        .getOrCreate()

    print("=== CDE PySpark Job 시작 ===")
    
    # 샘플 데이터 프레임 생성
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    
    # 데이터 출력 및 집계
    df.show()
    print(f"총 레코드 수: {df.count()}")
    
    print("=== CDE PySpark Job 완료 ===")
    spark.stop()

if __name__ == "__main__":
    main()