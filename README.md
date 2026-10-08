# Streaming Mastodon — configuration et démonstration

## Périmètre

Fil Mastodon #AI → Producteur Python → Kafka (`mastodon_stream`) → Spark Structured Streaming → PostgreSQL.

Le producteur interroge l’API REST de Mastodon toutes les 10 secondes, car la connexion au flux natif a échoué sur l’instance choisie. Il s’agit d’une ingestion en quasi temps réel. Spark traite les messages Kafka par micro-batches avec un déclenchement toutes les 10 secondes ; le traitement peut durer plus longtemps que cet intervalle.

Toutes les publications valides sont enregistrées dans `toots`. Les publications en anglais sont utilisées pour les comptages horaires et la longueur moyenne du texte nettoyé par utilisateur et par heure. Les horodatages des événements et les fenêtres utilisent le fuseau UTC. Un watermark d’un jour limite l’état conservé pour les agrégations ; les publications arrivant suffisamment en retard peuvent être exclues. Les statistiques restent provisoires jusqu’à ce que le watermark ferme la fenêtre.

## Prérequis

Docker Desktop démarré, Docker Compose, Python pour le producteur exécuté sur l’ordinateur, un token d’accès d’application Mastodon et une connexion Internet pour les premiers téléchargements des connecteurs Spark. Spark, Python et Jupyter s’exécutent dans Docker ; aucune installation de Java sur l’ordinateur n’est nécessaire.

Conservez les fichiers existants `database/init.sql` et `streaming/mastodon_producer.py`. Ce paquet ne les remplace pas.

## Premier lancement (PowerShell, à la racine du dépôt)

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install Mastodon.py kafka-python python-dotenv

docker compose config --quiet
docker compose up -d --build
docker compose ps
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic mastodon_stream --partitions 3 --replication-factor 1
```

Créez localement le fichier `.env` avec la configuration attendue par le producteur :

```dotenv
MASTODON_API_BASE_URL=https://mastodon.social
MASTODON_ACCESS_TOKEN=remplacez_par_votre_token
```

Ne commitez jamais le véritable fichier `.env`. Si le producteur attend d’autres variables d’environnement, conservez les paramètres existants. Le producteur exécuté sous Windows se connecte à Kafka via `localhost:9092` ; Spark dans Docker se connecte via `kafka:29092`.

Démarrez la collecte dans un terminal séparé :

```powershell
.\.venv\Scripts\Activate.ps1
python streaming/mastodon_producer.py
```

Récupérez localement l’URL et le token d’accès à Jupyter :

```powershell
docker compose logs spark
```

Ouvrez `http://127.0.0.1:8888`, authentifiez-vous avec le token affiché et ouvrez `notebooks/01_streaming.ipynb`. Exécutez les sections 1 à 8 dans l’ordre. Attendez que l’aperçu facultatif affiche des publications avant de l’arrêter. N’exécutez pas la section 9 avant la fin de la démonstration. N’exécutez pas simultanément le script Python Spark de streaming et le notebook.

PostgreSQL est accessible depuis les outils de l’ordinateur sur le port 5433 et depuis les conteneurs via `postgres:5432`. Configuration locale : base `mastodon`, utilisateur `spark`, mot de passe défini dans Compose.

## Résultats

| Table | Contenu |
|---|---|
| `toots` | Publications valides nettoyées, toutes langues confondues ; `toot_id` unique |
| `streaming_posts_per_hour` | Publications distinctes en anglais par heure de publication UTC |
| `streaming_user_stats_hourly` | Nombre de publications en anglais et longueur moyenne du texte par utilisateur et par heure |

Les insertions de publications utilisent `ON CONFLICT DO NOTHING`. Les traitements d’écriture des agrégations insèrent ou mettent à jour les valeurs absolues dans une transaction. Les checkpoints suivent les positions de lecture et l’état des agrégations ; le retraitement d’entrées identiques ne gonfle pas les résultats. L’agrégation par utilisateur déduplique les paires identiques identifiant/longueur, mais pas les versions modifiées d’une publication ayant des longueurs différentes.

Le traitement d’écriture utilise `toLocalIterator()` et une connexion PostgreSQL côté driver, ce qui convient à ce petit jeu de données pédagogique. Un déploiement plus important nécessiterait des écritures adaptées au volume et une évaluation de la mémoire utilisée par les ensembles d’identifiants distincts.

## Démonstration et vérification de la reprise

1. Lancez les trois traitements d’écriture et la cellule de surveillance : chacun doit afficher `Actif : True` et `Erreur : None`.
2. Réexécutez la vérification des données enregistrées après l’arrivée de nouvelles publications et affichez les tables horaires.
3. Arrêtez le producteur avec `Ctrl+C`, laissez Spark terminer le traitement des messages disponibles et relevez les comptages et statistiques.
4. Exécutez la section 9 pour arrêter les requêtes du notebook.
5. Dans le même noyau, réexécutez les cellules de démarrage des sections 4, 6 et 7 avec leurs checkpoints existants. Sans nouvelles données Kafka, les comptages et statistiques doivent rester inchangés. Examinez toute différence avant de considérer le test de reprise comme réussi.
6. Redémarrez le producteur ; les nouvelles publications doivent apparaître. Le redémarrage du producteur peut renvoyer des publications récentes : la contrainte d’unicité de PostgreSQL empêche les doublons dans les données enregistrées, et les publications identiques relues ne gonflent pas les agrégations.

L’aperçu est facultatif et possède son propre checkpoint ; après un redémarrage, il peut donc n’afficher que les nouvelles publications.

## Arrêt et conservation des données

Exécutez la section 9 du notebook, arrêtez le producteur avec `Ctrl+C`, puis lancez :

```powershell
docker compose stop
```

Redémarrez avec `docker compose up -d`. Après le démarrage d’un nouveau noyau, réexécutez le notebook jusqu’à la section 8 incluse. Conservez `checkpoints/` et les volumes Kafka/PostgreSQL. `docker compose down` conserve les volumes nommés ; `docker compose down -v` les supprime. Ne supprimez pas les checkpoints lors d’un redémarrage habituel. Modifier la taille des fenêtres ou le schéma de l’état des agrégations peut nécessiter un nouveau checkpoint et une reconstruction planifiée des tables de résultats correspondantes.

## Collaboration et rendu

Partagez le code, Compose, le Dockerfile, le notebook, le SQL d’initialisation et les modèles de configuration. Les données PostgreSQL/Kafka locales et les dossiers de checkpoints ne sont pas transférés par Git. Les membres du groupe peuvent collecter leurs propres données ou utiliser un export de base de données partagé. L’analyse historique peut lire `toots` ; les tables de statistiques en streaming contiennent uniquement les données en anglais.

Ne commitez pas `.env`, `.venv/`, les checkpoints, les tokens d’accès à Jupyter ou les sorties du notebook contenant des secrets. Intégrez ces instructions de configuration au README principal du groupe lorsque les autres sections seront prêtes.
