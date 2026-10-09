# Analyse de données Mastodon avec Apache Spark

Projet réalisé dans le cadre du module **5SPAR**.

Le projet combine la collecte de publications Mastodon, le traitement
en streaming, l’analyse historique, l’analyse de sentiments et la
visualisation des résultats.

Ce document présente les parties **ingestion et traitement en streaming**,
**analyse batch et optimisation Spark**, ainsi que **Machine Learning pour
l'analyse de sentiment**. La partie visualisation sera documentée dans sa
section dédiée.

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

## 16. Analyse batch et optimisation Spark

La partie batch est réalisée dans **`notebooks/02_batch.ipynb`**.
Elle exploite les publications Mastodon historiques enregistrées dans
PostgreSQL par le pipeline de streaming.

Le notebook charge la table `toots` dans un DataFrame Spark via JDBC :

```text
PostgreSQL → JDBC → Spark DataFrame → analyses batch
```

Les traitements réalisés comprennent :

- le comptage de l'activité par utilisateur ;
- l'identification des utilisateurs ayant publié plus d'un seuil configurable
  de toots ;
- le nombre de publications par jour ;
- l'extraction et le comptage des hashtags avec `explode()` ;
- l'identification des hashtags les plus fréquents ;
- le calcul de la longueur moyenne des publications ;
- des statistiques complémentaires par utilisateur ;
- la reproduction de plusieurs analyses avec **Spark SQL** ;
- l'utilisation de `explain()` pour inspecter les plans d'exécution.

Le notebook utilise directement les données Mastodon réelles stockées dans
PostgreSQL. Dans l'environnement Docker, la connexion JDBC utilise le service
interne :

```text
jdbc:postgresql://postgres:5432/mastodon
```

### Optimisations testées

Plusieurs stratégies Spark sont comparées sur le même workload :

- exécution sans cache ;
- mise en cache avec `cache()` et matérialisation ;
- redistribution des données avec `repartition()` ;
- réduction du nombre de partitions avec `coalesce()`.

Les temps d'exécution sont mesurés afin de comparer les différentes
configurations. Les résultats peuvent varier selon le volume disponible :
sur un petit jeu de données, le coût des shuffles et de la gestion des
partitions peut être supérieur au gain attendu.

La **Spark UI** permet de compléter cette analyse en observant les jobs,
stages, tâches, shuffles et données mises en cache. Lorsque le notebook
streaming utilise déjà le port `4040`, l'application batch peut être
accessible sur `4041`.

### Exécution

Avec les services Docker déjà démarrés, ouvrir dans JupyterLab :

```text
notebooks/02_batch.ipynb
```

Puis exécuter le notebook dans l'ordre. La collecte streaming peut continuer
en parallèle : le notebook batch lit un instantané des données disponibles
dans PostgreSQL au moment du chargement.

---

## 17. Analyse de sentiment avec Spark MLlib

La partie Machine Learning est réalisée dans **`notebooks/03_ml.ipynb`**.
L'objectif est d'entraîner un modèle de classification de sentiment à partir
du jeu de données **Sentiment140**, puis d'appliquer le modèle retenu aux
publications Mastodon collectées par le projet.

Le pipeline général est :

```text
Sentiment140
→ nettoyage du texte
→ tokenisation
→ suppression des stop words
→ CountVectorizer
→ IDF / TF-IDF
→ entraînement et évaluation
→ prédiction des toots Mastodon
→ PostgreSQL
```

### Préparation des données

Le nettoyage est appliqué de manière cohérente aux données Sentiment140 et
aux publications Mastodon. Il comprend notamment :

- le passage en minuscules ;
- la suppression des URL et mentions ;
- la suppression de la ponctuation et des chiffres ;
- la normalisation des espaces ;
- la conservation de négations importantes pour le sentiment.

Le texte est ensuite tokenisé avec `RegexTokenizer`. Les mots vides sont
retirés avec `StopWordsRemover`, tout en conservant certaines négations
comme `no`, `not` et `nor`.

Sentiment140 contient **1 600 000 publications** dans le notebook. Les données
sont séparées de manière reproductible avec `seed=42` :

- **80 %** pour l'entraînement ;
- **20 %** pour le test.

### Modèles comparés

Deux modèles Spark MLlib sont entraînés et comparés :

- **Logistic Regression** ;
- **Naive Bayes multinomial**.

La représentation numérique des textes repose sur `CountVectorizer` puis
`IDF`, afin d'obtenir des caractéristiques de type TF-IDF.

Le meilleur modèle est ensuite appliqué aux publications Mastodon en anglais
présentes dans PostgreSQL.

Comme l'entraînement repose sur deux classes principales, **positive** et
**négative**, le notebook ajoute également une zone d'incertitude pour les
prédictions dont la probabilité est trop proche du seuil de décision.

### Stockage des prédictions

Les résultats de l'analyse de sentiment appliquée aux publications Mastodon
sont enregistrés dans PostgreSQL, notamment dans la table :

```text
toot_sentiments
```

Cette étape relie la partie Machine Learning au reste du pipeline :

```text
Mastodon → Kafka → Spark Streaming → PostgreSQL
                                  ↓
                         Spark MLlib
                                  ↓
                         toot_sentiments
```

### Exécution

Ouvrir dans JupyterLab :

```text
notebooks/03_ml.ipynb
```

Puis exécuter les cellules dans l'ordre après avoir vérifié que PostgreSQL
est accessible et que les données nécessaires à l'entraînement sont
disponibles.


## 18. Visualisation et synthèse des résultats

La partie visualisation est réalisée dans **`notebooks/04_visualisation.ipynb`**. Elle exploite les données produites par les différentes étapes du projet afin de proposer une synthèse graphique de l'activité Mastodon, des traitements batch et des résultats du modèle de sentiment.

Le principe général est :

```text
PostgreSQL
    ↓
Pandas
    ↓
Matplotlib
    ↓
Visualisations et interprétation
```

Les données utilisées proviennent principalement des tables :
- toots pour les publications collectées ;
- streaming_posts_per_hour pour l'activité horaire calculée en streaming ;
- toot_sentiments pour les prédictions issues du modèle de Machine Learning.

### Analyse de l'activité

Plusieurs visualisations permettent d'explorer l'activité des publications collectées :
- le nombre de publications par fenêtre horaire ;
- les utilisateurs les plus actifs ;
- les hashtags les plus fréquents hors hashtags directement utilisés pour la collecte ;
- la relation entre le nombre de publications d'un utilisateur et la longueur moyenne de ses contenus.

L'analyse de la longueur des publications utilise un nuage de points afin de comparer l'activité des utilisateurs et la taille moyenne de leurs messages.
Un coefficient de corrélation est également calculé afin de compléter l'analyse graphique. La comparaison avec et sans utilisateur atypique permet d'illustrer l'influence qu'un outlier peut avoir sur une mesure statistique.

### Visualisation des sentiments

Les prédictions enregistrées dans toot_sentiments sont utilisées pour analyser la répartition des sentiments des publications en anglais.

Trois catégories sont affichées :
- **positive** ;
- **negative** ;
- **uncertain**.

La catégorie **uncertain** ne correspond pas à une troisième classe apprise par le modèle. Elle représente une zone de confiance intermédiaire définie à partir de la probabilité produite par la régression logistique.

Les visualisations réalisées comprennent :
- un graphique en secteurs présentant la répartition globale des sentiments ;
- un graphique en barres horizontales empilées à 100 % comparant le profil de sentiment des utilisateurs les plus actifs ;
- une heatmap représentant l'évolution de la répartition des sentiments selon les heures de collecte.

Pour les analyses par utilisateur, le nombre de publications est également conservé afin d'éviter de comparer de la même manière un profil calculé sur quelques messages et un profil reposant sur un volume plus important.

### Mise à jour des prédictions

Lorsque de nouvelles publications sont collectées après la première application du modèle, le modèle **Logistic Regression** sauvegardé peut être rechargé afin de prédire uniquement les publications encore absentes de **toot_sentiments**.
Cette approche évite de réentraîner le modèle sur **Sentiment140** et permet une utilisation incrémentale du modèle :

```text
Nouvelles publications Mastodon
            ↓
Sélection des toots non prédits
            ↓
Nettoyage du texte
            ↓
Modèle ML sauvegardé
            ↓
Nouvelles prédictions
            ↓
toot_sentiments
            ↓
Visualisations
```

### Interprétation et limites

Les visualisations permettent d'identifier des tendances dans les données collectées, mais les résultats doivent être interprétés avec prudence.

Le volume de publications dépend directement de la durée et des périodes de collecte. Certaines fenêtres horaires peuvent contenir très peu de publications et ne sont donc pas représentatives d'une tendance générale.

L'analyse de sentiment présente également une limite importante : le modèle a été entraîné sur **Sentiment140**, alors que les publications Mastodon collectées autour de l'intelligence artificielle sont souvent techniques, informatives ou issues de flux d'actualité.

Une prédiction positive ou négative ne signifie donc pas nécessairement que l'auteur exprime explicitement une opinion positive ou négative.

### Exécution

Avec PostgreSQL accessible et les traitements précédents terminés, ouvrir dans JupyterLab :

```text
notebooks/04_visualisation.ipynb
```

Puis exécuter le notebook dans l'ordre. Les visualisations sont générées et commentées au fil des cellules.

Pour obtenir une analyse cohérente, il est recommandé de terminer la collecte streaming et de mettre à jour les prédictions de sentiment avant d'exécuter la partie consacrée aux visualisations ML.

# 19. Synthèse du pipeline

Le projet met en œuvre un pipeline complet allant de la collecte en temps réel de publications Mastodon à l'analyse de sentiment et à la visualisation des résultats, en passant par le traitement distribué et le stockage dans PostgreSQL.

### Vue d'ensemble

### Composants principaux

1. **Collecte** : extraction des publications Mastodon en temps réel via l'API et publication dans Kafka.
2. **Streaming** : traitement distribué des messages via Spark Streaming, enrichissement et enregistrement dans PostgreSQL.
3. **Batch** : analyse de données historiques stockées dans PostgreSQL, mise à l'échelle et visualisation.
4. **Machine Learning** : entraînement d'un modèle de sentiment sur **Sentiment140** et application aux publications Mastodon.
5. **Visualisation** : analyse des tendances d'activité et des résultats de sentiment via des graphiques et tableaux.

### Technologies utilisées

- **Spark** : traitement batch et streaming, MLlib
- **Kafka** : messagerie asynchrone
- **PostgreSQL** : persistance des données
- **Docker** : orchestration des services
- **Pandas / Matplotlib** : analyse et visualisation