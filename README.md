# ObRail ETL — Bloc 1 C1

Depot de certification RNCP 37827 "Developpeur en Intelligence Artificielle" (Simplon.co / EPSI).
Competence visee : **C1 - Automatiser l'extraction de donnees** (Bloc 1).

---

## Contenu du depot

| Fichier | Role |
|---|---|
| `etl_obrail_mysql.py` | Pipeline ETL complet — cible MySQL |
| `etl_obrail_postgres.py` | Pipeline ETL complet — cible PostgreSQL |
| `etl_sources_extra.py` | Extraction API REST + Scraping + PySpark |
| `sql/00_create_schema_mysql.sql` | Schema MySQL (day_trips / night_trips separes) |
| `sql/01_create_schema_postgres.sql` | Schema PostgreSQL (table trips unifiee) |
| `.env.example` | Template de configuration |

---

## Sources de donnees (critere C1 Simplon)

### 1. Fichiers CSV

Fichiers : `eu_trips.csv`, `eu_trips_night.csv`, `de_night.csv`

Donnees GTFS open source des trajets ferroviaires europeens (trains de jour et de nuit).
Fournit la masse principale de donnees du projet ObRail (gares, routes, horaires).
Scripts : `etl_obrail_mysql.py` et `etl_obrail_postgres.py` — fonctions `extract()` et `transform()`.

### 2. API REST — Eurostat RAIL_PA_TOTAL

Endpoint : `https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/RAIL_PA_TOTAL`

API REST JSON-stat 2.0 publiee par l'Office statistique de l'UE.
Retourne le nombre de passagers ferroviaires annuels par pays membre.
Pourquoi : contextualise le volume d'usage du train par pays pour valider les hypotheses du modele IA.
Script : `etl_sources_extra.py` — fonction `extract_api_eurostat()`.
Sortie : `output/eurostat_rail_passengers.json`

### 3. Scraping HTML — Wikipedia EuroNight

URL : `https://en.wikipedia.org/wiki/EuroNight`

Scraping BeautifulSoup de la liste des services EuroNight (liaisons, operateurs, pays).
Complete les donnees GTFS avec des metadonnees qualitatives (noms commerciaux, operateurs).
Script : `etl_sources_extra.py` — fonction `extract_scraping_wikipedia()`.
Sortie : `output/wikipedia_euronight.json`

### 4. Big Data — PySpark local (local[*])

Analyse de `eu_trips.csv` via `SparkSession` en mode local, sans cluster.

Requete Spark SQL representative :

```sql
SELECT country, COUNT(*) AS nb_trips,
       ROUND(AVG(duration_minutes), 1) AS avg_duration_min,
       COUNT(CASE WHEN is_night_trip = true THEN 1 END) AS night_trips
FROM eu_trips
WHERE origin_stop_lat BETWEEN -90 AND 90
GROUP BY country ORDER BY nb_trips DESC
```

Pourquoi : demontre la capacite a traiter de grands volumes sans cluster externe.
Script : `etl_sources_extra.py` — fonction `extract_spark_local()`.
Sortie : `output/spark_analysis/` (Parquet) + `output/spark_summary.json`

### 5. Base de donnees — MySQL et PostgreSQL

MySQL : `etl_obrail_mysql.py` charge dans `day_trips` et `night_trips` (tables separees).
PostgreSQL : `etl_obrail_postgres.py` charge dans une table unifiee `trips` avec `is_night_trip`.

---

## Installation

```bash
git clone https://github.com/AkramSatou/obrail-etl-bloc1.git
cd obrail-etl-bloc1

python -m venv .venv
# Linux/Mac :
source .venv/bin/activate
# Windows :
.venv\Scripts\activate

pip install pandas pymysql psycopg2-binary python-dotenv \
            requests beautifulsoup4 lxml pyspark

cp .env.example .env
# Editer .env avec vos identifiants BDD

# Placer les CSV dans data/
cp /chemin/vers/eu_trips.csv data/
cp /chemin/vers/eu_trips_night.csv data/
cp /chemin/vers/de_night.csv data/
```

---

## Commandes d'execution

### Creer les schemas SQL

```bash
# MySQL
mysql -u root -p < sql/00_create_schema_mysql.sql

# PostgreSQL
psql -U postgres -d obrail_europe_db -f sql/01_create_schema_postgres.sql
```

### Lancer les ETL

```bash
# Version MySQL (CSV -> MySQL)
python etl_obrail_mysql.py

# Version PostgreSQL (CSV -> PostgreSQL)
python etl_obrail_postgres.py

# Sources additionnelles (API + Scraping + Spark) — toutes en une fois
python etl_sources_extra.py

# Une source a la fois
python etl_sources_extra.py --source api
python etl_sources_extra.py --source scraping
python etl_sources_extra.py --source spark
```

---

## Structure des scripts (critere C1 Simplon)

| Etape | Description | Ou dans le code |
|---|---|---|
| Point de lancement | `if __name__ == "__main__": main()` | Bas de chaque fichier |
| Init dependances/connexions | `load_dotenv()`, `get_connection()`, `SparkSession.builder` | Debut de `main()` |
| Regles de traitement | `transform()`, `infer_country_day()`, `safe()`, `safe_int()`, `safe_float()` | Etape 2 |
| Gestion des erreurs | `try/except pymysql.Error`, `psycopg2.Error`, `requests.exceptions.*` | Partout |
| Fin de traitement | Log du resume final, fermeture connexion (`conn.close()`, `spark.stop()`) | Fin de `main()` |
| Sauvegarde | `bulk_insert_pg()`, `execute_batch()`, ecriture JSON/Parquet | Etape 3 |

---

## Verification MySQL vers PostgreSQL

### Resultats de reference (MySQL)

> Completer apres execution de `python etl_obrail_mysql.py`

| Metrique | Valeur MySQL |
|---|---|
| Gares (stops) | a remplir |
| Trajets de jour (day_trips) | a remplir |
| Trajets de nuit (night_trips) | a remplir |
| Total trajets | a remplir |
| Routes | a remplir |

### Resultats de validation (PostgreSQL)

> Completer apres execution de `python etl_obrail_postgres.py`

| Metrique | Valeur PostgreSQL |
|---|---|
| Gares (stops) | a remplir |
| Trajets nuit (is_night_trip=TRUE) | a remplir |
| Trajets jour (is_night_trip=FALSE) | a remplir |
| Total trips | a remplir |
| Duree moyenne (min) | a remplir |

### Constat

Equivalence : [ ] validee / [ ] en cours de validation

Difference attendue : la version PostgreSQL utilise une table `trips` unifiee
au lieu de `day_trips` + `night_trips` separees en MySQL.
Le nombre total de trajets doit etre identique.

### Justification du choix PostgreSQL (C4)

| Critere | MySQL | PostgreSQL |
|---|---|---|
| Typage JSON avance | JSON basique | JSONB + index GIN |
| Vues materialisees | Non | Oui |
| Integration FastAPI/SQLAlchemy | Oui | Oui (deja utilise dans obrail-mspr3) |
| Coherence avec l'existant | Non | Oui (obrail-mspr3 utilise PostgreSQL) |

Decision : PostgreSQL est retenu comme base principale car il est deja utilise
dans le projet obrail-mspr3 (coherence d'architecture) et offre JSONB pour
`etl_logs.details` et les vues analytiques.

---

## Licence

Code : MIT. Donnees CSV : Open Data (licences sources dans la table `data_sources`).
