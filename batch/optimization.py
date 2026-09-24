import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("MastodonSparkOptimization")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# PostgreSQL
# --------------------------------------------------
jdbc_url = "jdbc:postgresql://localhost:5433/mastodon"

connection_properties = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver"
}


df = spark.read.jdbc(
    url=jdbc_url,
    table="toots",
    properties=connection_properties
)


print("\n=== Nombre initial de partitions ===")
print(df.rdd.getNumPartitions())


# --------------------------------------------------
# 1. Test sans cache
# --------------------------------------------------
start = time.perf_counter()

df.groupBy("username").count().collect()
df.groupBy("language").count().collect()

without_cache = time.perf_counter() - start

print(f"\nTemps sans cache : {without_cache:.4f} secondes")


# --------------------------------------------------
# 2. Test avec cache
# --------------------------------------------------
df.cache()

# Force Spark à charger les données dans le cache
df.count()

start = time.perf_counter()

df.groupBy("username").count().collect()
df.groupBy("language").count().collect()

with_cache = time.perf_counter() - start

print(f"Temps avec cache : {with_cache:.4f} secondes")


# --------------------------------------------------
# 3. Repartition
# --------------------------------------------------
repartitioned_df = df.repartition(4)

print("\n=== Après repartition(4) ===")
print("Nombre de partitions :", repartitioned_df.rdd.getNumPartitions())


# Exemple de traitement
start = time.perf_counter()

repartitioned_df.groupBy("username").count().collect()

repartition_time = time.perf_counter() - start

print(f"Temps avec repartition : {repartition_time:.4f} secondes")


# --------------------------------------------------
# 4. Coalesce
# --------------------------------------------------
coalesced_df = repartitioned_df.coalesce(2)

print("\n=== Après coalesce(2) ===")
print("Nombre de partitions :", coalesced_df.rdd.getNumPartitions())

start = time.perf_counter()

coalesced_df.groupBy("username").count().collect()

coalesce_time = time.perf_counter() - start

print(f"Temps avec coalesce : {coalesce_time:.4f} secondes")


# --------------------------------------------------
# Résumé
# --------------------------------------------------
print("\n========== RÉSUMÉ ==========")
print(f"Sans cache      : {without_cache:.4f} s")
print(f"Avec cache      : {with_cache:.4f} s")
print(f"Repartition(4)  : {repartition_time:.4f} s")
print(f"Coalesce(2)     : {coalesce_time:.4f} s")


# Laisse Spark actif pour consulter Spark UI
print("\nSpark UI disponible normalement sur : http://localhost:4040")
input("Appuie sur Entrée pour arrêter Spark...")


df.unpersist()
spark.stop()