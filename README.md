# ObRail ETL — Bloc 1

Entrepôt de données ferroviaires européennes — Pipeline ETL multi-sources.

**Certification :** RNCP 37827 — Développeur en Intelligence Artificielle (Simplon / EPSI)
**Soutenance :** septembre 2026

---

## Contexte du projet

### Acteurs

| Rôle | Personne |
|------|----------|
| Apprenant / développeur | Akram |
| Formation | EPSI — filière IA, cursus Simplon |
| Jury cible | Certification RNCP 37827 |

### Objectifs

ObRail est un système de visualisation des trajets ferroviaires en Europe.
Ce dépôt (`obrail-etl-bloc1`) couvre le **Bloc 1 — Gérer les données** :
construire un pipeline ETL robuste qui extrait des données depuis 5 sources
hétérogènes, les transforme et les charge dans un entrepôt PostgreSQL.

### Périmètre fonctionnel

- **173 662** trajets journaliers et **22 292** trajets de nuit chargés dans le schéma `entrepot`
- **5 946** arrêts ferroviaires référencés, **3 034** routes
- Couverture : ~20 pays européens (SNCB, DB, SBB, SNCF, ...)
- 14 contraintes FK garantissent l'intégrité référentielle

---

## Architecture

```
obrail-etl-bloc1/
├── etl.py                  # ETL principal : CSV → PostgreSQL entrepot
├── models.py               # Modèles SQLAlchemy (11 tables, 17 FK)
├── orchestrator.py         # Orchestrateur 5 sources → toutes en base
├── extractors/
│   ├── api_extractor.py    # Source 2 : API REST Eurostat
│   ├── scraping_extractor.py # Source 3 : Wikipedia (robots.txt vérifié)
│   ├── spark_pipeline.py   # Source 4 : PySpark local[*]
│   └── db_extractor.py     # Source 5 : requêtes entrepot PostgreSQL
├── importers/              # Import CSV → PostgreSQL (sources 2, 3, 4)
│   ├── eurostat_importer.py  # → entrepot.eurostat_rail_passengers
│   ├── wikipedia_importer.py # → entrepot.wikipedia_named_trains
│   └── spark_importer.py     # → entrepot.spark_route_aggregations
├── tests/
│   ├── conftest.py           # Fixtures pytest (schéma entrepot isolé)
│   ├── test_etl.py           # 3 tests ETL principal (count, idempotence, FK)
│   ├── test_extractors.py    # Tests 4 extracteurs (mocks + intégration)
│   └── test_new_tables.py    # 9 tests nouvelles tables (count, idempotence, FK)
├── docs/
│   ├── RGPD.md               # Registre de traitement (art. 30 RGPD)
│   ├── SOURCES.md            # Fiche par source (URL, licence, robots.txt)
│   └── SQL_DOCUMENTATION.md  # Merise + choix SQL (C2)
├── sql/
│   ├── 00_create_database_mysql.sql
│   └── 01_create_schema.sql
├── outputs/                # CSV extracteurs + rapport_insertions.md
└── .github/workflows/ci.yml # CI : PostgreSQL + MySQL
```

### Sources de données (C1 / C3 / C4)

Toutes les 5 sources sont désormais **effectivement insérées en base PostgreSQL** :

| N° | Type | Source | Module extraction | Table PostgreSQL |
|----|------|--------|-------------------|------------------|
| 1 | CSV | `eu_trips.csv` (173 662 + 22 292 lignes) | `etl.py` | `day_trips` + `night_trips` |
| 2 | API REST | Eurostat RAIL_PA_QUARTAL (JSON) | `extractors/api_extractor.py` | `eurostat_rail_passengers` |
| 3 | Scraping | Wikipedia — trains passagers nommés UE | `extractors/scraping_extractor.py` | `wikipedia_named_trains` |
| 4 | Big Data | PySpark local[*] agrégations | `extractors/spark_pipeline.py` | `spark_route_aggregations` |
| 5 | Base de données | Schéma `entrepot` PostgreSQL | `extractors/db_extractor.py` | *(extraction seule)* |

---

## Environnements

### Prérequis

| Outil | Version | Usage |
|-------|---------|-------|
| Python | 3.11.15 (conda `obrail`) | Runtime ETL |
| PostgreSQL | 15 (Docker) | Entrepôt principal |
| MySQL | 8.3.0 (WAMP) | Cible alternative (`DB_TARGET=mysql`) |
| PySpark | 3.5.3 | Pipeline big data |
| Docker Compose | 2.x | Orchestration conteneurs |

### Docker (PostgreSQL)

```bash
# Démarrer les conteneurs (depuis le dépôt obrail-mspr3)
docker compose -f docker/docker-compose.yml up -d

# Vérifier que le port 5432 est accessible
docker exec obrail-db pg_isready -U obrail
```

---

## Installation

```bash
# Créer/activer l'environnement Conda
conda activate obrail

# Installer les dépendances
pip install -r requirements.txt

# Copier et configurer les variables d'environnement
cp .env.example .env
# Éditer .env si nécessaire
```

---

## Utilisation

### ETL principal (CSV → PostgreSQL entrepot)

```bash
python etl.py
```

Produit : schéma `entrepot` avec ~196 000 trajets chargés (~195s sur SSD).

### Orchestrateur complet (5 sources)

```bash
# Avec ETL (re-charge les données)
python orchestrator.py

# Sans ETL (entrepôt déjà peuplé)
python orchestrator.py --skip-etl

# Répertoire de sortie personnalisé
python orchestrator.py --output-dir resultats/
```

Les résultats sont sauvegardés en CSV dans `outputs/` et toutes les sources
sont insérées en base PostgreSQL :
- `eurostat_rail_passengers.csv` → `entrepot.eurostat_rail_passengers`
- `wikipedia_named_trains.csv` → `entrepot.wikipedia_named_trains`
- `spark_aggregations.csv` → `entrepot.spark_route_aggregations`
- `db_volumes.csv`, `db_top_routes.csv`, `db_etl_log.csv` *(extraction depuis l'entrepôt)*
- `rapport_insertions.md` — récapitulatif du nombre de lignes insérées par source

### Cible MySQL (alternative)

```bash
DB_TARGET=mysql DB_HOST=localhost DB_PORT=3306 DB_USER=root DB_PASSWORD="" \
DB_NAME=obrail_europe_db python etl.py
```

---

## Tests

```bash
conda activate obrail

# Tests ETL (PostgreSQL Docker requis)
python -m pytest tests/test_etl.py -v

# Tests extracteurs (aucune connexion externe requise)
python -m pytest tests/test_extractors.py -v

# Tous les tests
python -m pytest tests/ -v --tb=short

# Tests d'intégration (réseau + base réelle)
OBRAIL_INTEGRATION=1 python -m pytest tests/ -v -m integration
```

### Résultats attendus

```
tests/test_etl.py::test_comptage_lignes_chargees_egale_lignes_valides  PASSED
tests/test_etl.py::test_deux_executions_ne_doublent_pas_les_donnees    PASSED
tests/test_etl.py::test_contrainte_fk_rejette_stop_id_invalide         PASSED
tests/test_extractors.py  (14 tests)                                   PASSED
tests/test_new_tables.py::TestEurostatImport::test_comptage            PASSED
tests/test_new_tables.py::TestEurostatImport::test_idempotence         PASSED
tests/test_new_tables.py::TestEurostatImport::test_fk                  PASSED
tests/test_new_tables.py::TestWikipediaImport::test_comptage           PASSED
tests/test_new_tables.py::TestWikipediaImport::test_idempotence        PASSED
tests/test_new_tables.py::TestWikipediaImport::test_fk                 PASSED
tests/test_new_tables.py::TestSparkImport::test_comptage               PASSED
tests/test_new_tables.py::TestSparkImport::test_idempotence            PASSED
tests/test_new_tables.py::TestSparkImport::test_fk                     PASSED
```

---

## Variables d'environnement

| Variable | Défaut | Description |
|----------|--------|-------------|
| `DB_TARGET` | `postgresql` | Cible SGBD (`postgresql` ou `mysql`) |
| `DB_HOST` | `localhost` | Hôte de la base de données |
| `DB_PORT` | `5432` | Port (5432 PostgreSQL / 3306 MySQL) |
| `DB_USER` | `obrail` | Utilisateur |
| `DB_PASSWORD` | `obrail` | Mot de passe |
| `DB_NAME` | `obrail` | Nom de la base |
| `ENTREPOT_SCHEMA` | `entrepot` | Schéma PostgreSQL (ignoré pour MySQL) |
| `TEST_DATABASE_URL` | *(voir ci-dessus)* | URL complète pour pytest |
| `OBRAIL_INTEGRATION` | `0` | `1` pour activer les tests d'intégration réseau |

---

## Intégration continue (CI)

Le workflow `.github/workflows/ci.yml` exécute deux jobs :

| Job | SGBD | Tests |
|-----|------|-------|
| `etl-tests` | PostgreSQL 15 | `pytest tests/ -v` |
| `etl-mysql` | MySQL 8.3 | Smoke test `python etl.py` |

---

## Contraintes & bonnes pratiques

- **RGPD :** aucune donnée personnelle — voir `docs/RGPD.md`
- **Robots.txt :** vérifié avant chaque scraping — voir `docs/SOURCES.md`
- **Idempotence :** `TRUNCATE … RESTART IDENTITY CASCADE` garantit que deux exécutions successives produisent le même état
- **Résilience :** chaque extracteur retourne un DataFrame vide en cas de panne réseau, sans bloquer l'orchestrateur
- **Sécurité :** credentials dans `.env` (hors git), pas d'API key, pas de service payant

---

## Références

- [Eurostat API](https://ec.europa.eu/eurostat/web/api-usage-guide)
- [Wikimedia robots.txt](https://en.wikipedia.org/robots.txt)
- [PySpark 3.5 docs](https://spark.apache.org/docs/3.5.0/)
- [SQLAlchemy 2.0 docs](https://docs.sqlalchemy.org/en/20/)
- [RNCP 37837 fiche](https://www.francecompetences.fr/recherche/rncp/37827/)
