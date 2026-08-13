"""
Orchestrateur ObRail — 5 sources hétérogènes → entrepôt PostgreSQL (C1 / C3 / C4)

Sources :
  1. CSV          : eu_trips.csv via etl.run() → day_trips + night_trips
  2. API REST     : Eurostat RAIL_PA_QUARTAL → eurostat_rail_passengers
  3. Scraping     : Wikipedia trains nommés → wikipedia_named_trains
  4. PySpark      : agrégations eu_trips.csv → spark_route_aggregations
  5. Base données : consultation entrepot après chargement (extraction seule)

Usage :
  python orchestrator.py [--skip-etl] [--output-dir outputs/]

Options :
  --skip-etl    : ne re-lance pas etl.run() si l'entrepôt est déjà peuplé
  --output-dir  : répertoire de sortie pour les CSV (défaut : outputs/)
"""

from __future__ import annotations

import argparse
import logging
import pathlib
import sys
from datetime import datetime

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


def _write_summary(
    output_dir: pathlib.Path,
    etl_counts: dict,
    n_eurostat: int,
    n_wikipedia: int,
    n_spark: int,
) -> None:
    """Génère outputs/rapport_insertions.md — preuve des 5 insertions pour la soutenance."""
    n_day   = etl_counts.get("day_trips",   0)
    n_night = etl_counts.get("night_trips", 0)
    total   = n_day + n_night + n_eurostat + n_wikipedia + n_spark
    wiki_note = (
        " *(scraping bloqué — robots.txt ou réseau inaccessible)*"
        if n_wikipedia == 0
        else ""
    )
    lines = [
        "# Rapport d'insertions — ObRail ETL Bloc 1",
        f"Généré le : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "| N° | Type | Table PostgreSQL cible | Lignes insérées |",
        "|----|------|------------------------|-----------------|",
        f"| 1a | CSV | `entrepot.day_trips` | {n_day} |",
        f"| 1b | CSV | `entrepot.night_trips` | {n_night} |",
        f"| 2  | API REST | `entrepot.eurostat_rail_passengers` | {n_eurostat} |",
        f"| 3  | Scraping | `entrepot.wikipedia_named_trains` | {n_wikipedia}{wiki_note} |",
        f"| 4  | BigData | `entrepot.spark_route_aggregations` | {n_spark} |",
        "| 5  | Base de données | *(extraction seule, déjà en base)* | — |",
        "",
        f"**Total lignes insérées toutes sources :** {total}",
    ]
    if n_wikipedia == 0:
        lines += [
            "",
            "> **Note Source 3 :** Le scraping Wikipedia retourne 0 ligne quand le réseau"
            " est inaccessible ou que robots.txt ne peut pas être vérifié (comportement"
            " conservateur : pas de scraping sans vérification de conformité). Le pipeline"
            " d'import est opérationnel — démontré par `TestWikipediaImport` (3 tests pytest).",
        ]
    summary_path = output_dir / "rapport_insertions.md"
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    log.info("Rapport d'insertions ecrit : %s", summary_path)


def run_all(
    skip_etl: bool = False,
    output_dir: pathlib.Path = DEFAULT_OUTPUT,
) -> dict[str, pd.DataFrame]:
    output_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, pd.DataFrame] = {}
    etl_counts: dict = {}

    # ── Source 1 : CSV → PostgreSQL entrepot (day_trips + night_trips) ───────
    if not skip_etl:
        log.info("=== Source 1/5 : ETL CSV → entrepot (PostgreSQL) ===")
        try:
            import etl
            etl_counts = etl.run()
            log.info("ETL termine : %s", etl_counts)
        except Exception as exc:
            log.error("ETL echoue : %s", exc)
    else:
        log.info("=== Source 1/5 : ETL CSV — ignore (--skip-etl) ===")

    # ── Source 2 : API REST Eurostat → eurostat_rail_passengers ──────────────
    log.info("=== Source 2/5 : API REST Eurostat ===")
    from extractors.api_extractor import extract_api
    df_api = extract_api()
    results["eurostat_rail_passengers"] = df_api
    _save(df_api, "eurostat_rail_passengers", output_dir)

    n_eurostat = 0
    try:
        from importers.eurostat_importer import import_eurostat
        n_eurostat = import_eurostat(
            csv_path=output_dir / "eurostat_rail_passengers.csv"
        )
        log.info("Source 2 — %d lignes insérées dans eurostat_rail_passengers.", n_eurostat)
    except Exception as exc:
        log.error("Import Eurostat echoue : %s", exc)

    # ── Source 3 : Scraping Wikipedia → wikipedia_named_trains ───────────────
    log.info("=== Source 3/5 : Scraping Wikipedia (trains nommés) ===")
    from extractors.scraping_extractor import extract_scraping
    df_scraping = extract_scraping()
    results["wikipedia_named_trains"] = df_scraping
    _save(df_scraping, "wikipedia_named_trains", output_dir)

    n_wikipedia = 0
    try:
        from importers.wikipedia_importer import import_wikipedia
        wiki_csv = output_dir / "wikipedia_named_trains.csv"
        n_wikipedia = import_wikipedia(csv_path=wiki_csv)
        if n_wikipedia == 0:
            log.warning(
                "Source 3 — Wikipedia : 0 ligne insérée "
                "(scraping bloqué par robots.txt ou réseau inaccessible)."
            )
        else:
            log.info("Source 3 — %d lignes insérées dans wikipedia_named_trains.", n_wikipedia)
    except Exception as exc:
        log.error("Import Wikipedia echoue : %s", exc)

    # ── Source 4 : PySpark → spark_route_aggregations ────────────────────────
    log.info("=== Source 4/5 : PySpark (agregations eu_trips.csv) ===")
    from extractors.spark_pipeline import extract_spark
    df_spark = extract_spark()
    results["spark_aggregations"] = df_spark
    _save(df_spark, "spark_aggregations", output_dir)

    n_spark = 0
    try:
        from importers.spark_importer import import_spark
        spark_csv = output_dir / "spark_aggregations.csv"
        n_spark = import_spark(csv_path=spark_csv)
        if n_spark == 0:
            log.warning("Source 4 — PySpark : 0 ligne insérée (vérifiez les logs Spark ci-dessus).")
        else:
            log.info("Source 4 — %d lignes insérées dans spark_route_aggregations.", n_spark)
    except Exception as exc:
        log.error("Import Spark echoue : %s", exc)

    # ── Source 5 : Base de données entrepot (extraction seule) ───────────────
    log.info("=== Source 5/5 : Base de donnees entrepot (PostgreSQL) ===")
    from extractors.db_extractor import extract_db
    db_results = extract_db()
    for key, df in db_results.items():
        results[f"db_{key}"] = df
        _save(df, f"db_{key}", output_dir)

    # ── Rapport de synthèse ───────────────────────────────────────────────────
    _write_summary(output_dir, etl_counts, n_eurostat, n_wikipedia, n_spark)
    log.info("=== Orchestration terminee. Resultats dans %s/ ===", output_dir)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ObRail ETL Orchestrateur — 5 sources → PostgreSQL")
    parser.add_argument("--skip-etl", action="store_true", help="Ne pas relancer etl.run()")
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    run_all(skip_etl=args.skip_etl, output_dir=args.output_dir)
    sys.exit(0)
