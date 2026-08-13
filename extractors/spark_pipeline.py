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
  2. Enrichissement du code ISO pays → nom complet via une chaîne F.when() JVM.

Inférence du pays (heuristique 4 valeurs) :
  eu_trips.csv ne contient pas de colonne 'country'. Elle est déduite depuis
  route_id / origin_stop_id / route_long_name avec une logique identique à
  etl.py::infer_country_day() : FR, ES, IT sont détectés par motifs ; tout
  le reste est classé DE par défaut. Simplification assumée : seuls 4 codes
  pays sont produits (DE/ES/FR/IT), cohérente avec les données disponibles.
"""

from __future__ import annotations

import logging
import pathlib
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)

# Référentiel pays pour l'enrichissement F.when() (utilisé dans extract_spark).
# Note : seuls FR/ES/IT/DE sont produits par l'inférence ; les 16 autres codes
# restent présents ici pour complétude sémantique mais ne sont jamais atteints
# avec eu_trips.csv (aucun motif distinctif identifié pour les pays restants).
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

        # ── Inférence du pays ────────────────────────────────────────────────
        # eu_trips.csv n'a pas de colonne 'country' ; on la déduit depuis
        # route_id / origin_stop_id / route_long_name, exactement comme
        # etl.py::infer_country_day() — via l'API Column native (JVM,
        # pas de Python worker → évite le bug Windows Store Python).
        if "country" not in df.columns:
            _ES = (
                "MADRID|GETAFE|LEGANE|MOSTOLES|HUMANES|MAJADAHONDA|"
                "MONTSERRAT|ALCORCON|VILLALBA|ALCALA|POZUELO|"
                "FUENLABRADA|PARLA|PINTO|MONCLOA|TORREJ|COSLADA"
            )
            _IT = (
                "FIRENZE|PISA|LIVORNO|ROMA|MILANO|NAPOLI|VENEZIA|"
                "BOLOGNA|TORINO|PALERMO|LA SPEZIA|PARMA|GENOVA|"
                "LUCCA|SIENA|AREZZO|VIAREGGIO|AULLA|FAENZA|PISTOIA|PORRETTA"
            )
            _rid = F.col("route_id").cast("string")
            _oid = F.col("origin_stop_id").cast("string")
            _rln = F.upper(F.col("route_long_name").cast("string"))
            df = df.withColumn(
                "country",
                F.when(
                    _rid.startswith("FR:") | _oid.contains("OCETrain"),
                    F.lit("FR"),
                ).when(
                    _oid.contains("par_") | _rln.rlike(_ES),
                    F.lit("ES"),
                ).when(
                    _rln.rlike(_IT),
                    F.lit("IT"),
                ).otherwise(F.lit("DE"))
            )
            log.info("[Spark] Colonne 'country' inferee depuis route_id / origin_stop_id / route_long_name.")

        # Colonnes numériques optionnelles (fixtures de tests réduites uniquement)
        for col_name, col_type in [("duration_minutes", "double"), ("n_stops", "double")]:
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

        # ── Enrichissement pays via F.when (JVM natif — évite createDataFrame Python) ──
        # createDataFrame depuis une liste Python crée un Python RDD, ce qui déclenche
        # des Python workers lors du shuffle → échec sur Windows (Store Python alias).
        _cn = (
            F.when(F.col("country") == "AT", "Autriche")
             .when(F.col("country") == "BE", "Belgique")
             .when(F.col("country") == "CH", "Suisse")
             .when(F.col("country") == "CZ", "Republique Tcheque")
             .when(F.col("country") == "DE", "Allemagne")
             .when(F.col("country") == "DK", "Danemark")
             .when(F.col("country") == "ES", "Espagne")
             .when(F.col("country") == "FR", "France")
             .when(F.col("country") == "GB", "Royaume-Uni")
             .when(F.col("country") == "HR", "Croatie")
             .when(F.col("country") == "HU", "Hongrie")
             .when(F.col("country") == "IT", "Italie")
             .when(F.col("country") == "NL", "Pays-Bas")
             .when(F.col("country") == "NO", "Norvege")
             .when(F.col("country") == "PL", "Pologne")
             .when(F.col("country") == "PT", "Portugal")
             .when(F.col("country") == "RO", "Roumanie")
             .when(F.col("country") == "SE", "Suede")
             .when(F.col("country") == "SK", "Slovaquie")
             .when(F.col("country") == "SI", "Slovenie")
             .otherwise(F.lit(None))
        )
        enriched = (
            agg.withColumn("country_name", _cn)
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

    log.error("[Spark] eu_trips.csv introuvable. Chemin recherche : %s", default)
    return None
