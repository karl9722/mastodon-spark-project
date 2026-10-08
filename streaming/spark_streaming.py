import html
from html.parser import HTMLParser
from pathlib import Path

import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


class TextExtractor(HTMLParser):
    """Extraire le texte du HTML en conservant des séparateurs."""

    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)

    def handle_starttag(self, tag, attrs):
        if tag in ("p", "br", "div"):
            self.parts.append(" ")

    def handle_endtag(self, tag):
        if tag in ("p", "div"):
            self.parts.append(" ")


@F.udf(returnType=T.StringType())
def clean_html(content):
    if content is None:
        return None

    parser = TextExtractor()
    parser.feed(content)

    return " ".join(
        html.unescape("".join(parser.parts)).split()
    )


def write_to_postgres(batch_df, batch_id):
    """Enregistrer un micro-batch en évitant les doublons."""

    # Utilise les variables PGHOST, PGPORT, PGDATABASE,
    # PGUSER et PGPASSWORD du service Spark dans Docker Compose.
    connection = psycopg2.connect(connect_timeout=10)

    processed = 0
    inserted = 0

    sql = """
        INSERT INTO toots (
            toot_id,
            created_at,
            user_id,
            username,
            content,
            language,
            hashtags,
            favourites_count,
            reblogs_count
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (toot_id) DO NOTHING
    """

    try:
        # Valider la transaction si toutes les écritures réussissent.
        # En cas d'erreur, les écritures du lot sont annulées.
        with connection:
            with connection.cursor() as cursor:
                # Fixer UTC pour les timestamps Python produits
                # par Spark dans ce conteneur.
                cursor.execute("SET LOCAL TIME ZONE 'UTC'")

                rows = (
                    batch_df
                    .select(
                        "toot_id",
                        F.date_format(
                            "created_at",
                            "yyyy-MM-dd HH:mm:ss.SSSSSS",
                        ).alias("created_at"),
                        "user_id",
                        "username",
                        "content",
                        "language",
                        "hashtags",
                        "favourites_count",
                        "reblogs_count",
                    )
                    .toLocalIterator()
                )

                for row in rows:
                    cursor.execute(
                        sql,
                        (
                            row.toot_id,
                            row.created_at,
                            row.user_id,
                            row.username,
                            row.content,
                            row.language,
                            row.hashtags or [],
                            row.favourites_count or 0,
                            row.reblogs_count or 0,
                        ),
                    )

                    processed += 1
                    inserted += cursor.rowcount

        print(
            f"Batch {batch_id} : "
            f"{processed} posts traités, "
            f"{inserted} insérés, "
            f"{processed - inserted} doublons ignorés.",
            flush=True,
        )

    finally:
        connection.close()


spark = (
    SparkSession.builder
    .appName("MastodonStreaming")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Structure des JSON envoyés par le producteur Mastodon.
schema = T.StructType([
    T.StructField("toot_id", T.StringType()),
    T.StructField("created_at", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("username", T.StringType()),
    T.StructField("content", T.StringType()),
    T.StructField("language", T.StringType()),
    T.StructField("hashtags", T.ArrayType(T.StringType())),
    T.StructField("favourites_count", T.IntegerType()),
    T.StructField("reblogs_count", T.IntegerType()),
])

# Checkpoint distinct de celui du test console.
checkpoint = (
    Path(__file__).resolve().parents[1]
    / "checkpoints"
    / "postgres"
)

query = None

try:
    # Lire Kafka via son adresse interne Docker.
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "mastodon_stream")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "1000")
        .load()
    )

    # Convertir les messages Kafka en colonnes structurées.
    parsed_stream = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                schema,
            ).alias("post")
        )
        .select("post.*")
    )

    # Nettoyer le HTML et écarter les publications invalides.
    cleaned_stream = (
        parsed_stream
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("content", clean_html(F.col("content")))
        .filter(
            F.col("toot_id").isNotNull()
            & F.col("user_id").isNotNull()
            & F.col("created_at").isNotNull()
            & (F.length("content") > 0)
        )
        .withColumn("text_length", F.length("content"))
    )

    # Enregistrer chaque micro-batch dans PostgreSQL.
    query = (
        cleaned_stream.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint))
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(
        "Streaming Kafka → PostgreSQL actif. Arrêt avec Ctrl+C.",
        flush=True,
    )

    query.awaitTermination()

except KeyboardInterrupt:
    print("\nArrêt du streaming.", flush=True)

finally:
    if query is not None:
        query.stop()

    spark.stop()