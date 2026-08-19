"""
============================================================
ObRail Europe — Sources additionnelles (Bloc 1 C1)
============================================================
Auteur  : Équipe ObRail Europe — MSPR EPSI Bloc E6.1
Version : 1.0.0
Certification : RNCP 37827 — Développeur IA, Bloc 1, C1

Description :
    Démontre l'extraction depuis 3 sources supplémentaires
    exigées par la grille Simplon C1 :

    1. API REST   — Eurostat (statistiques ferroviaires UE)
    2. Scraping   — Wikipedia (liste trains de nuit Europe)
    3. Big Data   — PySpark local (analyse eu_trips.csv)

    Chaque source est isolée dans sa propre fonction avec :
    - point d'entrée explicite
    - initialisation de la connexion / session
    - règles de traitement
    - gestion des erreurs et exceptions
    - sauvegarde du résultat

Prérequis :
    pip install requests beautifulsoup4 lxml pyspark python-dotenv

Usage :
    python etl_sources_extra.py
    python etl_sources_extra.py --source api
    python etl_sources_extra.py --source scraping
    python etl_sources_extra.py --source spark
============================================================
"""

import sys
import json
import logging
import argparse
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_sources_extra.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

BASE_DIR   = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
DATA_DIR   = BASE_DIR / "data"

# ── Timeout HTTP et tentatives ───────────────────────────────────────────────
HTTP_TIMEOUT  = 15   # secondes
HTTP_RETRIES  = 3
RETRY_BACKOFF = 2    # secondes entre chaque tentative


# ════════════════════════════════════════════════════════════════════════════
# UTILITAIRE : requête HTTP robuste
# ════════════════════════════════════════════════════════════════════════════

def fetch_url(url: str, params: dict = None, headers: dict = None) -> requests.Response:
    """
    Requête GET avec gestion des erreurs HTTP, timeout et retry.
    Lève RuntimeError si toutes les tentatives échouent.
    """
    default_headers = {
        "User-Agent": "ObRail-ETL/1.0 (EPSI RNCP37827; contact: obrail@epsi.fr)",
        "Accept": "application/json, text/html;q=0.9",
    }
    if headers:
        default_headers.update(headers)

    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            log.info(f"  GET {url}  (tentative {attempt}/{HTTP_RETRIES})")
            resp = requests.get(url, params=params, headers=default_headers,
                                timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            log.warning(f"  Timeout (tentative {attempt})")
        except requests.exceptions.HTTPError as e:
            log.error(f"  Erreur HTTP {e.response.status_code} : {e}")
            raise RuntimeError(f"HTTP {e.response.status_code} sur {url}") from e
        except requests.exceptions.ConnectionError as e:
            log.warning(f"  Connexion échouée (tentative {attempt}) : {e}")

        if attempt < HTTP_RETRIES:
            log.info(f"  Nouvel essai dans {RETRY_BACKOFF}s ...")
            time.sleep(RETRY_BACKOFF)

    raise RuntimeError(f"Impossible de joindre {url} après {HTTP_RETRIES} tentatives.")


# ════════════════════════════════════════════════════════════════════════════
# SOURCE 1 — API REST : Eurostat
# ════════════════════════════════════════════════════════════════════════════

EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EUROSTAT_DATASET = "RAIL_PA_TOTAL"   # Passagers ferroviaires total par pays

def extract_api_eurostat() -> dict:
    """
    Extrait les statistiques de passagers ferroviaires depuis l'API REST Eurostat.

    Endpoint : GET /statistics/1.0/data/RAIL_PA_TOTAL
    Format   : JSON-stat 2.0
    Licence  : EC Open Data — réutilisation libre (CC BY 4.0)

    Résultat sauvegardé dans output/eurostat_rail_passengers.json
    """
    log.info("═" * 60)
    log.info("SOURCE 1 — API REST Eurostat (RAIL_PA_TOTAL)")
    log.info("═" * 60)

    started = datetime.now()

    # ── Paramètres de l'API ──────────────────────────────────────────────────
    params = {
        "format": "JSON",
        "lang":   "FR",
        "freq":   "A",        # Annual
        "unit":   "THS_PAS",  # Milliers de passagers
    }
    url = f"{EUROSTAT_BASE}/{EUROSTAT_DATASET}"

    # ── Extraction ───────────────────────────────────────────────────────────
    try:
        resp = fetch_url(url, params=params)
        raw  = resp.json()
    except RuntimeError as e:
        log.error(f"Extraction Eurostat impossible : {e}")
        return {"status": "FAILURE", "error": str(e), "records": 0}
    except json.JSONDecodeError as e:
        log.error(f"Réponse Eurostat non JSON : {e}")
        return {"status": "FAILURE", "error": str(e), "records": 0}

    # ── Transformation ───────────────────────────────────────────────────────
    try:
        dims   = raw.get("dimension", {})
        values = raw.get("value", {})

        # Extraction des labels géographiques (pays)
        geo_dim = dims.get("geo", {}).get("category", {})
        geo_index  = geo_dim.get("index", {})    # {"AT": 0, "BE": 1, ...}
        geo_labels = geo_dim.get("label", {})    # {"AT": "Autriche", ...}

        # Extraction des années disponibles
        time_dim   = dims.get("time", {}).get("category", {})
        time_index = time_dim.get("index", {})   # {"2022": 0, "2023": 1, ...}

        # Dimensions pour calculer l'index de valeur
        n_time = len(time_index)

        records = []
        for geo_code, geo_pos in geo_index.items():
            for year, time_pos in time_index.items():
                flat_idx = str(geo_pos * n_time + time_pos)
                val = values.get(flat_idx)
                if val is not None:
                    records.append({
                        "country_code": geo_code,
                        "country_name": geo_labels.get(geo_code, geo_code),
                        "year":         int(year),
                        "passengers_thousands": val,
                        "unit": "THS_PAS",
                        "source": "Eurostat RAIL_PA_TOTAL",
                    })

        log.info(f"  → {len(records)} observations extraites")

        # Filtre : garder seulement les pays du projet ObRail
        obrail_countries = {"AT", "BE", "CH", "CZ", "DE", "DK", "ES", "FI",
                            "FR", "GB", "HR", "HU", "IT", "LT", "LU", "NL",
                            "NO", "PL", "PT", "RO", "SE", "SI", "SK"}
        records = [r for r in records if r["country_code"] in obrail_countries]
        log.info(f"  → {len(records)} après filtre pays ObRail")

    except (KeyError, TypeError) as e:
        log.error(f"Parsing Eurostat échoué (structure inattendue) : {e}")
        return {"status": "FAILURE", "error": f"Parsing : {e}", "records": 0}

    # ── Sauvegarde ───────────────────────────────────────────────────────────
    OUTPUT_DIR.mkdir(exist_ok=True)
    out_path = OUTPUT_DIR / "eurostat_rail_passengers.json"
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({
                "extracted_at": started.isoformat(),
                "source":       f"{EUROSTAT_BASE}/{EUROSTAT_DATASET}",
                "records":      records,
            }, f, ensure_ascii=False, indent=2)
        log.info(f"  → Sauvegardé : {out_path}")
    except OSError as e:
        log.error(f"Écriture du fichier JSON impossible : {e}")
        return {"status": "FAILURE", "error": str(e), "records": len(records)}

    elapsed = (datetime.now() - started).total_seconds()
    log.info(f"  → Durée extraction Eurostat : {elapsed:.2f}s")
    return {"status": "SUCCESS", "records": len(records), "output": str(out_path)}


# ════════════════════════════════════════════════════════════════════════════
# SOURCE 2 — SCRAPING : Wikipedia (trains de nuit européens)
# ════════════════════════════════════════════════════════════════════════════

WIKI_URL = "https://en.wikipedia.org/wiki/EuroNight"

def extract_scraping_wikipedia() -> dict:
    """
    Scrape la page Wikipedia EuroNight pour extraire la liste des services
    de trains de nuit européens (liaisons, opérateurs, pays desservis).

    Gestion explicite :
    - Page inaccessible (timeout, ConnectionError)
    - Changement de structure HTML (aucune table trouvée)
    - Encodage non UTF-8

    Résultat sauvegardé dans output/wikipedia_euronight.json
    """
    log.info("═" * 60)
    log.info("SOURCE 2 — Scraping Wikipedia (EuroNight)")
    log.info("═" * 60)

    started = datetime.now()

    # ── Extraction ───────────────────────────────────────────────────────────
    try:
        resp = fetch_url(WIKI_URL, headers={"Accept": "text/html"})
        html = resp.text
    except RuntimeError as e:
        log.error(f"Scraping Wikipedia impossible : {e}")
        return {"status": "FAILURE", "error": str(e), "records": 0}

    # ── Parsing HTML ─────────────────────────────────────────────────────────
    try:
        soup   = BeautifulSoup(html, "lxml")
        tables = soup.find_all("table", class_="wikitable")

        if not tables:
            log.warning("  Aucune table wikitable trouvée — structure de page modifiée.")
            log.warning("  Tentative de récupération via paragraphes ...")
            # Fallback : extraire les listes de la section "Services"
            items = _scrape_wiki_fallback(soup)
            records = [{"name": i, "source": "Wikipedia EuroNight (texte)"} for i in items]
        else:
            records = _parse_wiki_tables(tables)

        log.info(f"  → {len(records)} entrées extraites")

    except Exception as e:
        log.error(f"Parsing HTML Wikipedia échoué : {e}")
        return {"status": "FAILURE", "error": str(e), "records": 0}

    # ── Sauvegarde ───────────────────────────────────────────────────────────
    OUTPUT_DIR.mkdir(exist_ok=True)
    out_path = OUTPUT_DIR / "wikipedia_euronight.json"
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({
                "extracted_at": started.isoformat(),
                "source":       WIKI_URL,
                "records":      records,
            }, f, ensure_ascii=False, indent=2)
        log.info(f"  → Sauvegardé : {out_path}")
    except OSError as e:
        log.error(f"Écriture impossible : {e}")
        return {"status": "FAILURE", "error": str(e), "records": len(records)}

    elapsed = (datetime.now() - started).total_seconds()
    log.info(f"  → Durée scraping Wikipedia : {elapsed:.2f}s")
    return {"status": "SUCCESS", "records": len(records), "output": str(out_path)}


def _parse_wiki_tables(tables) -> list:
    """Parse les tables wikitable et retourne une liste de dicts."""
    records = []
    for table in tables:
        headers = []
        header_row = table.find("tr")
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]

        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells:
                continue
            if headers and len(cells) == len(headers):
                record = dict(zip(headers, cells))
            else:
                record = {f"col_{i}": v for i, v in enumerate(cells)}
            record["source"] = "Wikipedia EuroNight (table)"
            records.append(record)

    return records


def _scrape_wiki_fallback(soup) -> list:
    """Extrait des éléments de liste si aucune table n'est disponible."""
    items = []
    for li in soup.find_all("li"):
        text = li.get_text(strip=True)
        # Heuristique : garder les lignes qui mentionnent des villes ou pays
        if len(text) > 10 and any(kw in text for kw in
                                   ["–", "→", "Vienna", "Paris", "Berlin",
                                    "Rome", "Barcelona", "Brussels", "Night"]):
            items.append(text[:300])
    return items[:100]  # limite raisonnable


# ════════════════════════════════════════════════════════════════════════════
# SOURCE 3 — BIG DATA : PySpark local
# ════════════════════════════════════════════════════════════════════════════

EU_TRIPS_PATH = str(DATA_DIR / "eu_trips.csv")
SPARK_OUTPUT  = str(OUTPUT_DIR / "spark_analysis")

def extract_spark_local() -> dict:
    """
    Analyse eu_trips.csv avec PySpark en mode local (sans cluster).

    Requête Spark SQL représentative :
      Nombre de trajets et durée moyenne par pays d'origine,
      classé par volume décroissant.

    Résultat écrit en Parquet dans output/spark_analysis/
    + résumé JSON dans output/spark_summary.json

    Gestion des erreurs :
    - Fichier CSV absent
    - PySpark non installé
    - Erreur Spark lors de l'exécution
    """
    log.info("═" * 60)
    log.info("SOURCE 3 — PySpark local (eu_trips.csv)")
    log.info("═" * 60)

    started = datetime.now()

    # ── Vérification prérequis ────────────────────────────────────────────────
    csv_path = Path(EU_TRIPS_PATH)
    if not csv_path.exists():
        log.error(f"Fichier CSV absent : {EU_TRIPS_PATH}")
        log.error("  Placez eu_trips.csv dans le dossier data/ du projet.")
        return {"status": "FAILURE", "error": f"Fichier introuvable : {EU_TRIPS_PATH}", "records": 0}

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        log.error("PySpark non installé. Exécutez : pip install pyspark")
        return {"status": "FAILURE", "error": "ImportError: pyspark", "records": 0}

    # ── Initialisation SparkSession ───────────────────────────────────────────
    try:
        log.info("  Initialisation SparkSession (local[*]) ...")
        spark = (SparkSession.builder
                 .master("local[*]")
                 .appName("ObRail-ETL-Bloc1")
                 .config("spark.driver.memory", "2g")
                 .config("spark.sql.shuffle.partitions", "4")
                 .config("spark.ui.enabled", "false")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        log.info(f"  Spark version : {spark.version}")
    except Exception as e:
        log.error(f"Impossible d'initialiser Spark : {e}")
        return {"status": "FAILURE", "error": str(e), "records": 0}

    try:
        # ── Lecture CSV ───────────────────────────────────────────────────────
        log.info(f"  Lecture de {csv_path.name} ...")
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .option("nullValue", "")
              .csv(EU_TRIPS_PATH))
        total_rows = df.count()
        log.info(f"  → {total_rows} lignes chargées, {len(df.columns)} colonnes")

        # ── Vue temporaire pour Spark SQL ─────────────────────────────────────
        df.createOrReplaceTempView("eu_trips")

        # ── Requête Spark SQL représentative ──────────────────────────────────
        log.info("  Exécution requête Spark SQL ...")
        query = """
            SELECT
                COALESCE(country, 'UNKNOWN')  AS country,
                COUNT(*)                       AS nb_trips,
                ROUND(AVG(duration_minutes), 1) AS avg_duration_min,
                ROUND(AVG(n_stops), 1)          AS avg_stops,
                COUNT(CASE WHEN is_night_trip = true  THEN 1 END) AS night_trips,
                COUNT(CASE WHEN is_night_trip = false THEN 1 END) AS day_trips
            FROM eu_trips
            WHERE origin_stop_lat  BETWEEN -90 AND 90
              AND origin_stop_lon  BETWEEN -180 AND 180
            GROUP BY country
            ORDER BY nb_trips DESC
        """
        result_df = spark.sql(query)
        result_rows = result_df.collect()
        log.info(f"  → {len(result_rows)} pays agrégés")

        # ── Écriture résultat en Parquet ──────────────────────────────────────
        OUTPUT_DIR.mkdir(exist_ok=True)
        log.info(f"  Écriture Parquet → {SPARK_OUTPUT}")
        (result_df.coalesce(1)
                  .write
                  .mode("overwrite")
                  .option("header", "true")
                  .parquet(SPARK_OUTPUT))
        log.info("  Écriture Parquet terminée.")

        # ── Résumé JSON lisible ───────────────────────────────────────────────
        records = [row.asDict() for row in result_rows]
        summary_path = OUTPUT_DIR / "spark_summary.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump({
                "extracted_at":  started.isoformat(),
                "source_file":   EU_TRIPS_PATH,
                "total_rows":    total_rows,
                "spark_version": spark.version,
                "query":         query.strip(),
                "results":       records,
            }, f, ensure_ascii=False, indent=2)
        log.info(f"  → Résumé sauvegardé : {summary_path}")

    except Exception as e:
        log.error(f"Erreur Spark pendant le traitement : {e}", exc_info=True)
        return {"status": "FAILURE", "error": str(e), "records": 0}
    finally:
        spark.stop()
        log.info("  SparkSession arrêtée.")

    elapsed = (datetime.now() - started).total_seconds()
    log.info(f"  → Durée Spark : {elapsed:.2f}s")
    return {"status": "SUCCESS", "records": len(records), "output": SPARK_OUTPUT}


# ════════════════════════════════════════════════════════════════════════════
# MAIN — point d'entrée unique
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="ObRail ETL — Sources additionnelles (API, Scraping, Spark)")
    parser.add_argument(
        "--source",
        choices=["api", "scraping", "spark", "all"],
        default="all",
        help="Source à extraire (défaut: all)",
    )
    args = parser.parse_args()

    t_start = time.time()
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║   ObRail Europe — Sources additionnelles v1.0.0         ║")
    log.info("║   Bloc 1 C1 — RNCP 37827 Développeur IA                ║")
    log.info("╚══════════════════════════════════════════════════════════╝")

    results = {}

    if args.source in ("api", "all"):
        results["api_eurostat"] = extract_api_eurostat()

    if args.source in ("scraping", "all"):
        results["scraping_wikipedia"] = extract_scraping_wikipedia()

    if args.source in ("spark", "all"):
        results["spark_pyspark"] = extract_spark_local()

    # ── Résumé ───────────────────────────────────────────────────────────────
    elapsed = round(time.time() - t_start, 2)
    log.info("═" * 60)
    log.info("RÉSUMÉ GLOBAL")
    log.info("═" * 60)
    for src, res in results.items():
        status  = res.get("status", "?")
        records = res.get("records", 0)
        err     = res.get("error", "")
        if status == "SUCCESS":
            log.info(f"  {src:<30} SUCCESS  {records} enregistrements")
        else:
            log.error(f"  {src:<30} FAILURE  {err}")
    log.info(f"  Durée totale : {elapsed}s")

    # Sortie non nulle si au moins une source a échoué
    if any(r.get("status") != "SUCCESS" for r in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
