"""
Orchestrateur ObRail — combine les 5 sources hétérogènes (C1)

Sources :
  1. CSV          : eu_trips.csv via etl.run() → schéma entrepot PostgreSQL
  2. API REST     : Eurostat RAIL_PA_QUARTAL (open, sans auth)
  3. Scraping     : Wikipedia liste trains de nuit (robots.txt vérifié)
  4. PySpark      : agrégations big data sur eu_trips.csv (local[*])
  5. Base données : consultation du schéma entrepot après chargement

Usage :
  python orchestrator.py [--skip-etl] [--output-dir outputs/]

Options :
  --skip-etl    : ne re-lance pas etl.run() si le schéma entrepot est déjà peuplé
  --output-dir  : répertoire de sortie pour les CSV (défaut : outputs/)
"""

from __future__ import annotations

import argparse
import logging
import pathlib
import sys

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("orchestrator")

HERE = pathlib.Path(__file__).parent
DEFAULT_OUTPUT = HERE / "outputs"


def _save(df: pd.DataFrame, name: str, output_dir: pathlib.Path) -> None:
    if df.empty:
        log.warning("Source '%s' — DataFrame vide, fichier non produit.", name)
        return
    path = output_dir / f"{name}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    log.info("Source '%s' sauvegardee : %s (%d lignes)", name, path.name, len(df))


def run_all(skip_etl: bool = False, output_dir: pathlib.Path = DEFAULT_OUTPUT) -> dict[str, pd.DataFrame]:
    output_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, pd.DataFrame] = {}

    # ── Source 1 : CSV → PostgreSQL entrepot ─────────────────────────────────
    if not skip_etl:
        log.info("=== Source 1/5 : ETL CSV → entrepot (PostgreSQL) ===")
        try:
            import etl
            counts = etl.run()
            log.info("ETL termine : %s", counts)
        except Exception as exc:
            log.error("ETL echoue : %s", exc)
    else:
        log.info("=== Source 1/5 : ETL CSV — ignore (--skip-etl) ===")

    # ── Source 2 : API REST Eurostat ──────────────────────────────────────────
    log.info("=== Source 2/5 : API REST Eurostat ===")
    from extractors.api_extractor import extract_api
    df_api = extract_api()
    results["eurostat_rail_passengers"] = df_api
    _save(df_api, "eurostat_rail_passengers", output_dir)

    # ── Source 3 : Scraping Wikipedia ────────────────────────────────────────
    log.info("=== Source 3/5 : Scraping Wikipedia (night trains) ===")
    from extractors.scraping_extractor import extract_scraping
    df_scraping = extract_scraping()
    results["wikipedia_night_trains"] = df_scraping
    _save(df_scraping, "wikipedia_night_trains", output_dir)

    # ── Source 4 : PySpark big data ───────────────────────────────────────────
    log.info("=== Source 4/5 : PySpark (agregations eu_trips.csv) ===")
    from extractors.spark_pipeline import extract_spark
    df_spark = extract_spark()
    results["spark_aggregations"] = df_spark
    _save(df_spark, "spark_aggregations", output_dir)

    # ── Source 5 : Base de données entrepot ──────────────────────────────────
    log.info("=== Source 5/5 : Base de donnees entrepot (PostgreSQL) ===")
    from extractors.db_extractor import extract_db
    db_results = extract_db()
    for key, df in db_results.items():
        results[f"db_{key}"] = df
        _save(df, f"db_{key}", output_dir)

    log.info("=== Orchestration terminee. Resultats dans %s/ ===", output_dir)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ObRail ETL Orchestrateur")
    parser.add_argument("--skip-etl", action="store_true", help="Ne pas relancer etl.run()")
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    run_all(skip_etl=args.skip_etl, output_dir=args.output_dir)
    sys.exit(0)
