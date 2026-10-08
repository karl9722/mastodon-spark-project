from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MastodonSparkTest")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    spark.range(5).show()
    print("Spark fonctionne correctement.", flush=True)
finally:
    spark.stop()