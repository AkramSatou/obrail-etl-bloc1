"""
Importeur — PySpark agrégations → entrepot.spark_route_aggregations

Lit le CSV produit par extractors/spark_pipeline.py (ou un chemin fourni),
nettoie les données et insère dans la table PostgreSQL.
Idempotent : TRUNCATE … RESTART IDENTITY avant chaque insertion.
"""
from __future__ import annotations

import logging
import math
import os
import pathlib

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

log = logging.getLogger(__name__)

_BASE = pathlib.Path(__file__).parent.parent
DEFAULT_CSV = _BASE / "outputs" / "spark_aggregations.csv"

_SOURCE_NAME = "spark_route_aggregations"
_SOURCE_TYPE = "BigData"
_SOURCE_URL  = None
_SOURCE_DESC = (
    "Agrégations PySpark local[*] sur eu_trips.csv — "
    "nombre de trajets, durée et arrêts moyens par pays et type de route"
)


def _build_engine():
    user = os.getenv("DB_USER", "obrail")
    pwd  = os.getenv("DB_PASSWORD", "obrail")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db   = os.getenv("DB_NAME", "obrail")
    return create_engine(
        f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}",
        pool_pre_ping=True,
    )


def _get_or_create_source(session) -> int:
    from models import DataSource
    existing = session.query(DataSource).filter_by(source_name=_SOURCE_NAME).first()
    if existing:
        return existing.source_id
    new_src = DataSource(
        source_name=_SOURCE_NAME,
        source_type=_SOURCE_TYPE,
        source_url=_SOURCE_URL,
        description=_SOURCE_DESC,
        license_type="Open Data",
        is_active=True,
    )
    session.add(new_src)
    session.flush()
    return new_src.source_id


def _safe_int(v) -> int | None:
    try:
        f = float(v)
        return int(round(f)) if not math.isnan(f) else None
    except (TypeError, ValueError):
        return None


def _safe_num(v, decimals: int = 1) -> float | None:
    try:
        f = float(v)
        return round(f, decimals) if not math.isnan(f) else None
    except (TypeError, ValueError):
        return None


def _clean_str(v, max_len: int) -> str | None:
    try:
        is_na = pd.isna(v)
    except (TypeError, ValueError):
        is_na = False
    if is_na:
        return None
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    return s[:max_len]


def import_spark(engine=None, csv_path: pathlib.Path | str | None = None) -> int:
    """
    Lit le CSV Spark, normalise et insère dans spark_route_aggregations.

    Retourne le nombre de lignes insérées (0 si CSV absent ou vide).
    """
    from models import SparkRouteAggregation

    path = pathlib.Path(csv_path) if csv_path else DEFAULT_CSV
    if not path.exists():
        log.warning("[Spark Import] CSV introuvable : %s — aucune insertion.", path)
        return 0

    df = pd.read_csv(path)
    df["country"] = df["country"].astype(str).str.strip()
    df["route_type"] = pd.to_numeric(df.get("route_type"), errors="coerce")
    df = df.dropna(subset=["country", "route_type"])
    df = df[df["country"].str.len() > 0]
    df = df[~df["country"].str.lower().isin(("nan", "none"))]
    df = df.drop_duplicates(subset=["country", "route_type"])

    eng = engine or _build_engine()
    Session = sessionmaker(bind=eng)
    session = Session()

    try:
        session.execute(text(
            "TRUNCATE TABLE entrepot.spark_route_aggregations RESTART IDENTITY CASCADE"
        ))
        session.commit()

        source_id = _get_or_create_source(session)
        session.commit()

        rows = []
        for _, r in df.iterrows():
            rt = _safe_int(r["route_type"])
            if rt is None:
                continue
            rows.append({
                "country":       r["country"][:2],
                "country_name":  _clean_str(r.get("country_name"), 99),
                "route_type":    rt,
                "nb_trajets":    _safe_int(r.get("nb_trajets")),
                "duree_moy_min": _safe_num(r.get("duree_moy_min")),
                "arrets_moy":    _safe_num(r.get("arrets_moy")),
                "source_id":     source_id,
            })

        if rows:
            session.bulk_insert_mappings(SparkRouteAggregation, rows)
            session.commit()

        log.info("[Spark Import] %d lignes insérées dans spark_route_aggregations.", len(rows))
        return len(rows)

    except Exception as exc:
        session.rollback()
        log.error("[Spark Import] Erreur : %s", exc)
        raise
    finally:
        session.close()
