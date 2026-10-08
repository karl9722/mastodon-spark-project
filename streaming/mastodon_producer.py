import json
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer
from mastodon import Mastodon
from mastodon.errors import MastodonNetworkError


# Charger la configuration depuis le .env à la racine du projet.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

API_URL = os.getenv("MASTODON_API_BASE_URL")
ACCESS_TOKEN = os.getenv("MASTODON_ACCESS_TOKEN")

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
TOPIC = os.getenv("KAFKA_TOPIC", "mastodon_stream")

HASHTAG = "AI"
POLL_INTERVAL_SECONDS = 10


def serialize_key(key):
    return key.encode("utf-8")


def serialize_value(value):
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def fetch_posts(mastodon, last_id):
    """Lire les posts récents et paginer si nécessaire."""
    posts = mastodon.timeline_hashtag(
        HASHTAG,
        since_id=last_id,
        limit=40,
    )

    collected = list(posts)

    # Au démarrage, prendre seulement les 40 posts les plus récents.
    # Ensuite, paginer pour récupérer les nouveaux posts disponibles.
    if last_id is not None:
        while len(posts) == 40:
            posts = mastodon.timeline_hashtag(
                HASHTAG,
                since_id=last_id,
                max_id=posts[-1]["id"],
                limit=40,
            )
            collected.extend(posts)

    return sorted(collected, key=lambda post: int(post["id"]))


def send_post(producer, post):
    """Structurer un post puis attendre sa confirmation par Kafka."""
    account = post["account"]

    message = {
        "toot_id": str(post["id"]),
        "created_at": post["created_at"].isoformat(),
        "user_id": str(account["id"]),
        "username": account["acct"],
        "content": post["content"],
        "language": post.get("language"),
        "hashtags": [
            tag["name"] for tag in post.get("tags", [])
        ],
        "favourites_count": post.get("favourites_count", 0),
        "reblogs_count": post.get("reblogs_count", 0),
        "location": None,
    }

    metadata = producer.send(
        TOPIC,
        key=message["user_id"],
        value=message,
    ).get(timeout=30)

    print(
        f"Envoyé : {message['toot_id']} "
        f"| utilisateur={message['username']} "
        f"| partition={metadata.partition}",
        flush=True,
    )


def main():
    if not API_URL or not ACCESS_TOKEN:
        raise ValueError(
            "Renseigne MASTODON_API_BASE_URL et "
            "MASTODON_ACCESS_TOKEN dans le fichier .env."
        )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        acks="all",
        retries=5,
        key_serializer=serialize_key,
        value_serializer=serialize_value,
    )

    try:
        mastodon = Mastodon(
            api_base_url=API_URL,
            access_token=ACCESS_TOKEN,
            request_timeout=30,
        )

        last_id = None

        print(
            f"Collecte de #{HASHTAG} sur {API_URL}",
            flush=True,
        )
        print(
            f"Vérification toutes les {POLL_INTERVAL_SECONDS} secondes. "
            "Arrêt avec Ctrl+C.",
            flush=True,
        )

        while True:
            try:
                posts = fetch_posts(mastodon, last_id)
                sent_count = 0

                for post in posts:
                    # Ignorer les republications.
                    if post.get("reblog") is None:
                        send_post(producer, post)
                        sent_count += 1

                    # Avancer après l'envoi confirmé ou le post ignoré.
                    last_id = post["id"]

                print(
                    f"Vérification terminée : "
                    f"{sent_count} posts envoyés.",
                    flush=True,
                )

            except MastodonNetworkError:
                print(
                    "Connexion Mastodon interrompue ; "
                    "nouvelle tentative après la pause.",
                    flush=True,
                )

            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nArrêt du producteur.", flush=True)

    finally:
        producer.close(timeout=30)


if __name__ == "__main__":
    main()