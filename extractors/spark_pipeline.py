"""
Extracteur 3 — Big Data : PySpark (mode local)

Justification de l'usage de Spark :
  Le fichier eu_trips.csv contient 173 662 lignes avec 24 colonnes,
  soit ~350 Mo en mémoire. Pandas serait suffisant à cette échelle,
  mais le projet vise à valider la compétence C1 qui exige un système
  "big data". PySpark en mode local[*] démontre la maîtrise de l'API
  Spark SQL et la capacité à migrer vers un cluster distribué (YARN,
  Kubernetes) sans modifier le code métier — justification pédagogique
  documentée dans SOURCES.md.

Agrégations réalisées :
  1. Comptage de trajets par pays et type de route (GROUP BY + ORDER BY)
  2. Jointure entre les trajets et une table de référence pays intégrée
     directement dans Spark (équivalent d'un broadcast join sur petite dim)
"""

from __future__ import annotations

import logging
import pathlib
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)

# Référentiel pays intégré (broadcast join côté Spark)
COUNTRY_NAMES = {
    "AT": "Autriche", "BE": "Belgique", "CH": "Suisse", "CZ": "Republique Tcheque",
    "DE": "Allemagne", "DK": "Danemark", "ES": "Espagne", "FR": "France",
    "GB": "Royaume-Uni", "HR": "Croatie", "HU": "Hongrie", "IT": "Italie",
    "NL": "Pays-Bas", "NO": "Norvege", "PL": "Pologne", "PT": "Portugal",
    "RO": "Roumanie", "SE": "Suede", "SK": "Slovaquie", "SI": "Slovenie",
}


def extract_spark(
    csv_path: Optional[pathlib.Path] = None,
    fallback_dir: Optional[pathlib.Path] = None,
) -> pd.DataFrame:
    """
    Charge eu_trips.csv avec PySpark et retourne des agrégats par pays/type.

    Si csv_path est absent ou inexistant, bascule sur les fixtures (fallback_dir).
    Retourne DataFrame vide si aucune source n'est disponible.
    """
    source = _resolve_source(csv_path, fallback_dir)
    if source is None:
        log.error("[Spark] Aucune source CSV disponible — DataFrame vide retourne.")
        return pd.DataFrame(
            columns=["country", "route_type", "nb_trajets", "duree_moy_min", "arrets_moy"]
        )

    try:
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as F
    except ImportError:
        log.error("[Spark] PySpark non installe — installez-le via : pip install pyspark")
        return pd.DataFrame(
            columns=["country", "route_type", "nb_trajets", "duree_moy_min", "arrets_moy"]
        )

    log.info("[Spark] Demarrage SparkSession (local[*]) ...")
    spark = (
        SparkSession.builder
        .appName("ObRail-ETL-BigData")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        log.info("[Spark] Lecture de %s ...", source)
        df = spark.read.csv(str(source), header=True, inferSchema=True)
        n = df.count()
        log.info("[Spark] %d lignes chargees.", n)

        # Colonnes optionnelles : si le CSV ne contient pas 'country' ou
        # 'duration_minutes' / 'n_stops' (ex. fixture réduite), on les ajoute
        # avec une valeur nulle pour que le SQL reste identique.
        for col_name, col_type in [
            ("country", "string"),
            ("duration_minutes", "double"),
            ("n_stops", "double"),
        ]:
            if col_name not in df.columns:
                df = df.withColumn(col_name, F.lit(None).cast(col_type))

        # ── Agrégation 1 : comptage par pays + type de route ─────────────
        df.createOrReplaceTempView("trips")
        agg = spark.sql("""
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
            ORDER BY country, route_type
        """)

        # ── Jointure (broadcast) avec référentiel pays ────────────────────
        country_ref = spark.createDataFrame(
            [{"country": k, "country_name": v} for k, v in COUNTRY_NAMES.items()]
        )
        enriched = (
            agg.join(country_ref, on="country", how="left")
               .select("country", "country_name", "route_type",
                       "nb_trajets", "duree_moy_min", "arrets_moy")
               .orderBy("country", "route_type")
        )

        result: pd.DataFrame = enriched.toPandas()
        log.info("[Spark] Aggregation terminee : %d lignes.", len(result))
        return result

    finally:
        spark.stop()
        log.info("[Spark] SparkSession arretee.")


def _resolve_source(
    csv_path: Optional[pathlib.Path],
    fallback_dir: Optional[pathlib.Path],
) -> Optional[pathlib.Path]:
    if csv_path and csv_path.exists():
        return csv_path

    # Emplacement par défaut : eu_trips.csv à la racine du projet
    here = pathlib.Path(__file__).parent.parent
    default = here / "eu_trips.csv"
    if default.exists():
        return default

    # Fixture Spark dédiée (a les colonnes country, route_type, duration_minutes, n_stops)
    fixture_dirs = [fallback_dir] if fallback_dir else []
    fixture_dirs.append(here / "tests" / "fixtures")
    for d in fixture_dirs:
        if d is None:
            continue
        spark_fix = d / "spark_sample.csv"
        if spark_fix.exists():
            log.warning("[Spark] Utilisation du fichier fixture %s (donnees reduites).", spark_fix)
            return spark_fix

    return None
