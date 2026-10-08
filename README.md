# Analyse de données Mastodon avec Apache Spark

Projet réalisé dans le cadre du module **5SPAR**.

Le projet combine la collecte de publications Mastodon, le traitement
en streaming, l’analyse historique, l’analyse de sentiments et la
visualisation des résultats.

Ce document présente la partie **ingestion et traitement en streaming**.
Les parties batch, machine learning et visualisation seront documentées
dans leurs sections respectives.

## 1. Architecture du streaming

API Mastodon → Producteur Python → Kafka → Spark Structured Streaming → PostgreSQL

| Composant | Rôle |
|---|---|
| API Mastodon | Fournir les publications associées au hashtag `#AI` |
| Producteur Python | Collecter les publications et envoyer les messages JSON dans Kafka |
| Kafka | Conserver les messages dans le topic `mastodon_stream` |
| Spark Structured Streaming | Nettoyer les données et calculer les statistiques |
| PostgreSQL | Stocker les publications et les résultats |
| JupyterLab | Exécuter le notebook et consulter les résultats |

Le producteur interroge l’API REST toutes les **10 secondes**.
Cette collecte en quasi temps réel remplace le flux natif Mastodon,
dont la connexion échouait sur l’instance utilisée.

Spark lit ensuite les messages Kafka avec un déclenchement toutes les
10 secondes. La durée réelle d’un traitement dépend du volume de données.

## 2. Technologies

- Apache Kafka **3.9.1**
- Apache Spark **3.5.3**
- PostgreSQL **16**
- Python : `Mastodon.py`, `kafka-python`, `python-dotenv`
- JupyterLab
- Docker Compose

Spark, Java et Jupyter s’exécutent dans Docker.
Le producteur Python s’exécute sur l’ordinateur hôte.

## 3. Fichiers de la partie streaming

| Fichier | Description |
|---|---|
| `docker-compose.yml` | Configuration des services Kafka, PostgreSQL et Spark/Jupyter |
| `streaming/Dockerfile` | Construction de l’environnement Spark/Jupyter |
| `streaming/mastodon_producer.py` | Collecte Mastodon et envoi des messages dans Kafka |
| `notebooks/01_streaming.ipynb` | Nettoyage, stockage et agrégations en streaming |
| `database/init.sql` | Initialisation de PostgreSQL |
| `.env` | Configuration locale de Mastodon, exclue de Git |
| `checkpoints/` | Progression et état des requêtes Spark, exclus de Git |

## 4. Prérequis

- Docker Desktop démarré.
- Docker Compose disponible.
- Python installé sur l’ordinateur hôte.
- Une application Mastodon et un token autorisant la lecture des publications.
- Une connexion Internet pour télécharger les images, dépendances et connecteurs.

Les commandes suivantes sont destinées à **PowerShell sous Windows**.
Elles doivent être exécutées à la racine du dépôt.

## 5. Configuration du producteur

### Environnement Python

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install Mastodon.py kafka-python python-dotenv
```

### Configuration Mastodon

Créer un fichier `.env` à la racine du projet :

```dotenv
MASTODON_API_BASE_URL=https://mastodon.social
MASTODON_ACCESS_TOKEN=votre_token
```

Le token doit correspondre à l’instance renseignée dans
`MASTODON_API_BASE_URL`.

Le fichier `.env` contient un secret et doit rester exclu du dépôt.

## 6. Démarrage des services

Vérifier la configuration et démarrer les conteneurs :

```powershell
docker compose config --quiet
docker compose up -d --build
docker compose ps
```

Kafka et PostgreSQL doivent atteindre l’état `healthy`.

Le service `kafka-init` prépare les permissions du volume Kafka.
Son arrêt avec le code de sortie `0` est normal.

Créer le topic Kafka :

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic mastodon_stream --partitions 3 --replication-factor 1
```

Le topic possède **3 partitions** et un facteur de réplication de **1**.
Cette configuration locale utilise un seul broker Kafka.

## 7. Collecte des publications

Dans un terminal PowerShell séparé :

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
python streaming/mastodon_producer.py
```

Au démarrage, le producteur récupère les **40 publications les plus
récentes** du hashtag `#AI`, puis recherche les nouvelles publications
toutes les 10 secondes.

Le terminal reste actif pendant la collecte.
Un contrôle indiquant zéro nouvelle publication est normal.

Le redémarrage du producteur peut renvoyer des publications récentes.
La contrainte d’unicité de PostgreSQL évite leur stockage en double.

## 8. Exécution du notebook

Afficher les informations d’accès à Jupyter :

```powershell
docker compose logs spark
```

Ouvrir `http://127.0.0.1:8888` et utiliser le token affiché dans les logs.

Ouvrir **`notebooks/01_streaming.ipynb`**, puis exécuter les sections
**1 à 8 dans l’ordre**.

L’aperçu démarre de manière asynchrone : attendre l’affichage de
publications avant d’exécuter la cellule qui l’arrête.

La **section 9 arrête les traitements**. Elle doit être exécutée
uniquement à la fin de la démonstration.

Le producteur Mastodon doit fonctionner en parallèle du notebook.
L’ancien script Python de traitement Spark ne doit pas être lancé
simultanément : il partage le checkpoint d’écriture des publications
avec le notebook.

## 9. Traitements réalisés

### Lecture et nettoyage

- Lecture du topic Kafka `mastodon_stream`.
- Parsing JSON avec un schéma explicite.
- Conversion des dates de publication en horodatages.
- Conversion du HTML en texte brut.
- Exclusion des publications avec un identifiant requis manquant,
  une date invalide ou un contenu vide.
- Calcul de la longueur du texte nettoyé.

### Stockage des publications

Toutes les publications valides sont enregistrées dans PostgreSQL,
quelle que soit leur langue.

Les insertions utilisent `ON CONFLICT DO NOTHING` sur `toot_id`
pour ignorer les publications déjà enregistrées.

### Statistiques en streaming

Les publications en anglais sont sélectionnées pour calculer :

- Le nombre de publications distinctes par heure.
- Le nombre de publications par utilisateur et par heure.
- La longueur moyenne des publications par utilisateur et par heure.

Les fenêtres sont fondées sur la **date de publication** et utilisent
le fuseau **UTC**.

Un **watermark d’un jour** limite l’état conservé par Spark.
Les publications suffisamment tardives peuvent être exclues des
agrégations. Les résultats restent provisoires jusqu’à la fermeture
des fenêtres par le watermark.

Le comptage horaire déduplique les identifiants des publications.
Les statistiques par utilisateur dédupliquent les paires identiques
`(toot_id, text_length)` ; les versions modifiées avec une longueur
différente ne sont pas dédupliquées par ce calcul.

## 10. Tables PostgreSQL

| Table | Contenu |
|---|---|
| `toots` | Publications nettoyées, toutes langues confondues |
| `streaming_posts_per_hour` | Nombre de publications distinctes en anglais par heure UTC |
| `streaming_user_stats_hourly` | Nombre de publications et longueur moyenne par utilisateur et par heure |

Les agrégations sont enregistrées par **upsert** : les valeurs calculées
remplacent les valeurs précédentes au lieu de s’y ajouter.
Chaque micro-batch est écrit dans une transaction PostgreSQL.

### Nombre de publications enregistrées

```powershell
docker compose exec postgres psql -U spark -d mastodon -c "SELECT COUNT(*) FROM toots;"
```

### Comptages horaires

```powershell
docker compose exec postgres psql -U spark -d mastodon -c "SELECT * FROM streaming_posts_per_hour ORDER BY window_start DESC LIMIT 10;"
```

### Statistiques par utilisateur

```powershell
docker compose exec postgres psql -U spark -d mastodon -c "SELECT window_start, user_id, post_count, ROUND(avg_text_length::numeric, 1) AS avg_characters FROM streaming_user_stats_hourly ORDER BY window_start DESC, post_count DESC LIMIT 10;"
```

## 11. Accès aux services

| Service | Depuis l’ordinateur hôte | Depuis les conteneurs |
|---|---|---|
| Kafka | `localhost:9092` | `kafka:29092` |
| PostgreSQL | `localhost:5433` | `postgres:5432` |
| JupyterLab | `http://127.0.0.1:8888` | — |
| Spark UI | `http://127.0.0.1:4040` | — |

La Spark UI est accessible lorsqu’une application Spark est active.

Configuration PostgreSQL de développement :

- Base : `mastodon`
- Utilisateur : `spark`
- Mot de passe : `spark123`

## 12. Surveillance et reprise

La section de surveillance affiche l’état des trois requêtes :

- `posts_query`
- `hour_query`
- `user_query_hourly`

Les valeurs attendues sont `True` pour l’activité et `None`
pour l’erreur. Le statut `Waiting for next trigger` indique que
Spark attend le prochain déclenchement.

Chaque requête possède son propre checkpoint.
Il conserve la progression de lecture et, pour les agrégations,
l’état des calculs.

Pour vérifier la reprise :

1. Arrêter le producteur avec `Ctrl+C`.
2. Attendre que Spark traite les messages disponibles.
3. Relever les comptages et statistiques.
4. Arrêter les requêtes avec la section 9.
5. Réexécuter les cellules de démarrage des sections 4, 6 et 7
   dans le même noyau, avec les checkpoints existants.
6. Vérifier que les résultats restent inchangés sans nouvelles données.
7. Redémarrer le producteur et vérifier l’arrivée de nouvelles publications.

Après un redémarrage du noyau, réexécuter les sections 1 à 8.

## 13. Arrêt du projet

Exécuter la section 9 du notebook, arrêter le producteur avec
`Ctrl+C`, puis arrêter les services :

```powershell
docker compose stop
```

Pour reprendre :

```powershell
docker compose up -d
```

Les volumes Kafka/PostgreSQL et le dossier `checkpoints/` doivent
être conservés ensemble pour reprendre le pipeline existant.

- `docker compose down` conserve les volumes nommés.
- `docker compose down -v` supprime les volumes et leurs données.

Une modification de la taille des fenêtres ou de la structure
des agrégations peut nécessiter un nouveau checkpoint et une
reconstruction des résultats correspondants.

## 14. Dépannage

### Erreur Spark `ConnectionRefusedError`

Redémarrer le noyau Jupyter et réexécuter la cellule d’initialisation
de Spark.

Si l’erreur persiste :

```powershell
docker compose restart spark
docker compose logs --tail 80 spark
```

### Configuration Mastodon manquante

Vérifier que `.env` se trouve à la racine du dépôt et contient
l’URL de l’instance et le token d’accès.

### Aucun nouveau résultat dans l’aperçu

L’aperçu possède son propre checkpoint. Il peut avoir déjà lu
les messages disponibles et attendre de nouvelles publications.

### Logs au premier démarrage

Spark télécharge ses connecteurs Kafka et PostgreSQL lors du premier
lancement. Les messages de téléchargement sont attendus.

## 15. Collaboration et limites

Le dépôt contient le code et la configuration. Les données des volumes
Docker et les checkpoints restent locaux et ne sont pas transférés
par Git.

Les autres parties du projet peuvent lire la table `toots` pour
l’analyse historique. Pour utiliser les mêmes données sur plusieurs
ordinateurs, un export PostgreSQL peut être partagé séparément.

Le fichier `.env`, l’environnement `.venv/` et les checkpoints sont
exclus du dépôt via `.gitignore`.

Les écritures utilisent `toLocalIterator()` et une connexion PostgreSQL
côté driver. Cette approche convient au volume pédagogique du projet.
Un volume plus important nécessiterait des écritures plus adaptées
et une évaluation de la mémoire utilisée par les agrégations distinctes.

## 16. Analyse batch et optimisation

À compléter avec la documentation de la partie batch.

## 17. Analyse de sentiments

À compléter avec la documentation de la partie machine learning.

## 18. Visualisation

À compléter avec la documentation des graphiques et des résultats.
