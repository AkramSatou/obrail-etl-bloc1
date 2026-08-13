# Documentation des requêtes SQL — ObRail ETL Bloc 1

Ce document détaille les choix de jointure, les filtres et les optimisations
appliqués aux requêtes SQL du projet (critère C2).

---

## 1. Création du schéma entrepot (`etl.py` / `sql/01_create_schema.sql`)

```sql
CREATE SCHEMA IF NOT EXISTS entrepot;
```

**Justification :** L'utilisation d'un schéma dédié (`entrepot`) dans la même
instance PostgreSQL permet d'isoler les tables de l'entrepôt analytique des tables
applicatives (`public.trips`) sans créer une deuxième base de données. Cela
simplifie les opérations de sauvegarde et les tests (DROP SCHEMA … CASCADE).

---

## 2. Troncature idempotente (`etl.py → _truncate()`)

```sql
TRUNCATE TABLE
    entrepot.day_trips, entrepot.night_trips,
    entrepot.routes, entrepot.stops,
    entrepot.operators, entrepot.data_sources,
    entrepot.countries, entrepot.etl_logs
RESTART IDENTITY CASCADE;
```

**Pourquoi TRUNCATE plutôt que DELETE ?**
- `TRUNCATE` est non-journalisé (MVCC minimal) : 100× plus rapide que `DELETE` sur 200 000 lignes.
- `CASCADE` désactive automatiquement les FK le temps de la troncature.

**Pourquoi RESTART IDENTITY ?**
- Les séquences auto-increment (PK entiers) repartent de 1 à chaque exécution.
- Sans `RESTART IDENTITY`, le second run génèrerait `source_id=8` au lieu de `source_id=1`,
  cassant la FK `routes.source_id → data_sources.source_id` (bug reproductible sans ce mot-clé).

**Équivalent MySQL :**
```sql
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE night_trips;
TRUNCATE TABLE day_trips;
-- ... (ordre manuel pour respecter les FK)
SET FOREIGN_KEY_CHECKS = 1;
```
MySQL ne supporte pas `CASCADE` dans `TRUNCATE`, d'où l'activation/désactivation manuelle.

---

## 3. Volumes par pays (`db_extractor.py → _query_volumes()`)

```sql
SELECT
    c.country_code,
    c.country_name,
    COUNT(DISTINCT dt.trip_id)  AS day_trips,
    COUNT(DISTINCT nt.trip_id)  AS night_trips
FROM entrepot.countries   c
LEFT JOIN entrepot.stops  s  ON s.country_id    = c.country_id
LEFT JOIN entrepot.day_trips   dt ON dt.origin_stop_id = s.stop_id
LEFT JOIN entrepot.night_trips nt ON nt.origin_stop_id = s.stop_id
GROUP BY c.country_code, c.country_name
ORDER BY (COUNT(DISTINCT dt.trip_id) + COUNT(DISTINCT nt.trip_id)) DESC;
```

**Choix de jointure :** `LEFT JOIN` au lieu d'`INNER JOIN`.
- Un `INNER JOIN` exclurait les pays sans aucun arrêt ou trajet recensé.
- Le `LEFT JOIN` garantit que tous les pays du référentiel apparaissent dans le résultat,
  même avec `day_trips = 0` (exemple : pays uniquement desservis en trains de nuit).

**Filtre `origin_stop_id` :** On filtre sur l'arrêt d'origine uniquement
pour éviter le double-comptage (un trajet a un départ ET une arrivée potentiellement
dans des pays différents — on l'attribue au pays de départ).

**Optimisation :** Les colonnes `stop_id` (PK), `country_id` (FK) et `origin_stop_id` (FK)
sont indexées automatiquement par PostgreSQL via les contraintes PRIMARY KEY et FOREIGN KEY.
Aucun index supplémentaire n'est nécessaire à cette volumétrie (< 200 000 lignes).

---

## 4. Top 10 routes (`db_extractor.py → _query_top_routes()`)

```sql
SELECT
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    COUNT(dt.trip_id) AS nb_trajets
FROM entrepot.routes    r
INNER JOIN entrepot.day_trips dt ON dt.route_id = r.route_id
GROUP BY r.route_id, r.route_short_name, r.route_long_name
ORDER BY nb_trajets DESC
LIMIT 10;
```

**Pourquoi INNER JOIN ici ?** On veut uniquement les routes ayant des trajets.
Une route sans trajet journalier n'est pas pertinente pour un classement.

**Optimisation TOP-N :** `ORDER BY nb_trajets DESC LIMIT 10` est plus efficace qu'une
sous-requête avec `ROW_NUMBER()` ou `DENSE_RANK()` pour ce cas simple, car PostgreSQL
applique le `LIMIT` après le tri plutôt que de matérialiser toutes les lignes.

---

## 5. Dernier log ETL (`db_extractor.py → _query_etl_log()`)

```sql
SELECT
    started_at, finished_at, status,
    rows_inserted, rows_skipped, error_message
FROM entrepot.etl_logs
ORDER BY started_at DESC
LIMIT 1;
```

**Pattern TOP-1 :** Lecture du log le plus récent par `ORDER BY … DESC LIMIT 1`.
Alternative équivalente mais moins lisible : `WHERE started_at = (SELECT MAX(started_at) FROM …)`.
La version `LIMIT 1` évite une sous-requête et un second accès à la table.

---

## 6. Agrégations Spark SQL (`spark_pipeline.py`)

```sql
SELECT
    country,
    route_type,
    COUNT(*)                           AS nb_trajets,
    ROUND(AVG(duration_minutes), 1)    AS duree_moy_min,
    ROUND(AVG(n_stops), 1)             AS arrets_moy
FROM trips
WHERE country IS NOT NULL
  AND route_type IS NOT NULL
GROUP BY country, route_type
ORDER BY country, route_type;
```

**Exécuté dans le contexte Spark SQL** (DataFrame API sous-jacente).

**Filtre `IS NOT NULL` :** Les colonnes `country` et `route_type` peuvent être nulles
dans `eu_trips.csv` pour les lignes malformées. On les exclut explicitement pour éviter
une colonne `null` dans les résultats finaux, ce qui compliquerait la jointure pays suivante.

**Jointure Spark (broadcast join) :**
```python
enriched = agg.join(country_ref, on="country", how="left")
```
La table `country_ref` est petite (20 pays) → Spark l'envoie en broadcast automatiquement,
évitant un shuffle réseau coûteux. Sur un vrai cluster, ce comportement est contrôlé par
`spark.sql.autoBroadcastJoinThreshold` (défaut 10 Mo).

**`LEFT JOIN` :** Les pays non présents dans le référentiel conservent une ligne avec
`country_name = null` plutôt que d'être supprimés (comportement prévisible et auditables).

---

## 7. Contraintes FK vérifiées dans le schéma entrepot

Les 14 contraintes FK garantissent l'intégrité référentielle sans contrôles applicatifs :

| Table | FK | Référence |
|-------|----|-----------|
| stops | country_id | countries.country_id |
| routes | operator_id | operators.operator_id |
| routes | source_id | data_sources.source_id |
| day_trips | origin_stop_id | stops.stop_id |
| day_trips | destination_stop_id | stops.stop_id |
| day_trips | route_id | routes.route_id |
| day_trips | source_id | data_sources.source_id |
| night_trips | origin_stop_id | stops.stop_id |
| night_trips | destination_stop_id | stops.stop_id |
| night_trips | route_id | routes.route_id |
| night_trips | source_id | data_sources.source_id |
| etl_logs | source_id | data_sources.source_id |
| *(+ 2 opérateurs)* | | |

Test de régression FK : `tests/test_etl.py::test_contrainte_fk_rejette_stop_id_invalide`
