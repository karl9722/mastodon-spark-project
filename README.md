# Mastodon streaming — setup and demonstration

## Scope

Mastodon #AI timeline → Python producer → Kafka (`mastodon_stream`) → Spark Structured Streaming → PostgreSQL.

The producer polls the Mastodon REST API every 10 seconds because the native streaming connection failed on the selected instance. This is near-real-time ingestion. Spark processes Kafka records in micro-batches with a 10-second trigger; processing may take longer than the trigger interval.

All valid posts are stored in `toots`. English posts are used for hourly counts and average cleaned text length per user/hour. Event timestamps and windows use UTC. A one-day watermark limits retained aggregation state; sufficiently late records may be excluded. Statistics remain provisional until the watermark closes the window.

## Requirements

Docker Desktop running, Docker Compose, Python for the host producer, a Mastodon application access token, and Internet access for the first Spark connector downloads. Spark/Python/Jupyter run inside Docker; no host Java installation is required.

Keep the existing `database/init.sql` and `streaming/mastodon_producer.py`. This package does not replace them.

## First launch (PowerShell, repository root)

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

Create `.env` locally using the producer's expected configuration:

```dotenv
MASTODON_API_BASE_URL=https://mastodon.social
MASTODON_ACCESS_TOKEN=replace_with_your_own_token
```

Never commit the real `.env`. If the producer expects additional environment variables, retain its existing settings. The Windows producer connects to Kafka at `localhost:9092`; Spark inside Docker connects at `kafka:29092`.

Start ingestion in a separate terminal:

```powershell
.\.venv\Scripts\Activate.ps1
python streaming/mastodon_producer.py
```

Find Jupyter's access URL/token locally:

```powershell
docker compose logs spark
```

Open `http://127.0.0.1:8888`, authenticate with the displayed token, and open `notebooks/01_streaming.ipynb`. Run sections 1–8 sequentially. Wait for the optional preview to show records before stopping it. Do not run section 9 until the demonstration is finished. Do not run the standalone Spark streaming script concurrently with the notebook.

PostgreSQL is available to host tools on port 5433 and to containers on `postgres:5432`. Local development database: `mastodon`, user `spark`, password as configured in Compose.

## Outputs

| Table | Contents |
|---|---|
| `toots` | Cleaned valid posts, all languages; unique `toot_id` |
| `streaming_posts_per_hour` | Distinct English posts by UTC publication hour |
| `streaming_user_stats_hourly` | English post count and average text length per user/hour |

Post inserts use `ON CONFLICT DO NOTHING`. Aggregation writers upsert absolute values in a transaction. Checkpoints track offsets and aggregation state; retrying identical inputs does not inflate these results. The user aggregation deduplicates identical ID/length pairs, not differing edited versions of a post.

The writer uses `toLocalIterator()` and one driver-side PostgreSQL connection, appropriate for this small teaching dataset. A larger deployment would need scalable writes and a review of exact distinct-set memory costs.

## Demo and restart check

1. Run the three writers and the monitoring cell: each should show `Active: True`, `Error: None`.
2. Rerun the stored-data check after new posts arrive and show the hourly tables.
3. Stop the producer with Ctrl+C, allow Spark to catch up, and record counts/statistics.
4. Run section 9 to stop notebook queries.
5. In the same kernel, rerun the start cells in sections 4, 6 and 7 using their existing checkpoints. With no new Kafka input, counts/statistics should remain unchanged. Investigate any difference before declaring the restart check successful.
6. Restart the producer; newly published posts should appear. A producer restart may replay recent posts: PostgreSQL's unique constraint prevents duplicate stored rows, and identical replayed records do not inflate the aggregates.

The preview is optional and has its own checkpoint, so a restart may show only new records.

## Stop and preserve data

Run notebook section 9, stop the producer with Ctrl+C, then:

```powershell
docker compose stop
```

Restart with `docker compose up -d`. After a new kernel, rerun the notebook through section 8. Keep `checkpoints/` and the Kafka/PostgreSQL volumes. `docker compose down` preserves named volumes; `docker compose down -v` deletes them. Do not delete checkpoints as a routine restart step. Changing window sizes or aggregation state schema may require a new checkpoint and a deliberate rebuild of matching output tables.

## Collaboration and submission

Share code, Compose, Dockerfile, notebook, initialization SQL and configuration templates. Local PostgreSQL/Kafka data and checkpoint folders are not transferred through Git. Teammates can collect their own data or use an explicitly shared database export. Historical analysis can read `toots`; the streaming statistic tables contain English-only data.

Do not commit `.env`, `.venv/`, checkpoints, Jupyter access tokens or notebook outputs containing secrets. Merge these setup instructions into the team's main README once other sections are ready.
