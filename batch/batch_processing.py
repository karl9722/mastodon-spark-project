from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --------------------------------------------------
# 1. Création de la SparkSession
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("MastodonBatchProcessing")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Configuration PostgreSQL
# --------------------------------------------------
jdbc_url = "jdbc:postgresql://localhost:5433/mastodon"

connection_properties = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver"
}


# --------------------------------------------------
# 3. Lecture de la table toots
# --------------------------------------------------
df = spark.read.jdbc(
    url=jdbc_url,
    table="toots",
    properties=connection_properties
)


# --------------------------------------------------
# 4. Vérification des données
# --------------------------------------------------
print("\n=== Schema ===")
df.printSchema()

print("\n=== Nombre de toots ===")
print(df.count())

print("\n=== Données ===")
df.show(truncate=False)

from pyspark.sql import functions as F


# --------------------------------------------------
# 6. Nombre de toots par jour
# --------------------------------------------------
print("\n=== Nombre de toots par jour ===")

toots_per_day = (
    df
    .withColumn("date", F.to_date("created_at"))
    .groupBy("date")
    .count()
    .orderBy("date")
)

toots_per_day.show()


# --------------------------------------------------
# 7. Activité par utilisateur
# --------------------------------------------------
print("\n=== Activité par utilisateur ===")

user_activity = (
    df
    .groupBy("user_id", "username")
    .count()
    .orderBy(F.desc("count"))
)

user_activity.show()


# --------------------------------------------------
# 8. Utilisateurs avec plus d'un toot
# --------------------------------------------------
print("\n=== Utilisateurs actifs (> 1 toot) ===")

active_users = user_activity.filter(F.col("count") > 1)

active_users.show()


# --------------------------------------------------
# 9. Hashtags les plus fréquents
# --------------------------------------------------
print("\n=== Hashtags les plus fréquents ===")

hashtags_df = (
    df
    .select(F.explode("hashtags").alias("hashtag"))
    .groupBy("hashtag")
    .count()
    .orderBy(F.desc("count"))
)

hashtags_df.show()


# --------------------------------------------------
# 10. Longueur moyenne des toots
# --------------------------------------------------
print("\n=== Longueur moyenne des toots ===")

average_length = (
    df
    .select(
        F.avg(F.length("content")).alias("average_toot_length")
    )
)

average_length.show()


# --------------------------------------------------
# 11. Longueur moyenne par utilisateur
# --------------------------------------------------
print("\n=== Longueur moyenne par utilisateur ===")

average_length_per_user = (
    df
    .groupBy("username")
    .agg(
        F.avg(F.length("content")).alias("average_toot_length")
    )
    .orderBy(F.desc("average_toot_length"))
)

average_length_per_user.show()










# --------------------------------------------------
# Arrêt de Spark
# --------------------------------------------------
spark.stop()