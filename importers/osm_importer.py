"""
Importeur — Overpass API (OSM) → entrepot.osm_railway_stations

Lit le CSV produit par extractors/osm_extractor.py (ou un chemin fourni),
nettoie les données et insère dans la table PostgreSQL.
Idempotent : TRUNCATE … RESTART IDENTITY avant chaque insertion.
"""
from __future__ import annotations

import logging
import os
import pathlib

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

log = logging.getLogger(__name__)

_BASE = pathlib.Path(__file__).parent.parent
DEFAULT_CSV = _BASE / "outputs" / "osm_railway_stations.csv"

_SOURCE_NAME = "osm_overpass_stations"
_SOURCE_TYPE = "Base de données"
_SOURCE_URL  = "https://overpass-api.de/api/interpreter"
_SOURCE_DESC = (
    "Overpass API — gares ferroviaires (métro exclu), base de données OpenStreetMap "
    "interrogée en Overpass QL"
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
    """Retourne l'id de la source OSM, en l'insérant si elle n'existe pas encore."""
    from models import DataSource
    existing = session.query(DataSource).filter_by(source_name=_SOURCE_NAME).first()
    if existing:
        return existing.source_id
    new_src = DataSource(
        source_name=_SOURCE_NAME,
        source_type=_SOURCE_TYPE,
        source_url=_SOURCE_URL,
        description=_SOURCE_DESC,
        license_type="ODbL",
        is_active=True,
    )
    session.add(new_src)
    session.flush()
    return new_src.source_id


def import_osm(engine=None, csv_path: pathlib.Path | str | None = None) -> int:
    """
    Lit le CSV des gares OSM, normalise et insère dans osm_railway_stations.

    Paramètres :
        engine   : SQLAlchemy engine (injection pour les tests ; sinon env vars)
        csv_path : chemin vers le CSV ; par défaut outputs/osm_railway_stations.csv

    Retourne le nombre de lignes insérées (0 si CSV absent ou vide).
    """
    from models import OSMRailwayStation

    path = pathlib.Path(csv_path) if csv_path else DEFAULT_CSV
    if not path.exists():
        log.warning("[OSM Import] CSV introuvable : %s — aucune insertion.", path)
        return 0

    df = pd.read_csv(path)
    if df.empty:
        log.warning("[OSM Import] CSV vide — aucune insertion.")
        return 0

    df["osm_node_id"]  = df["osm_node_id"].astype(str).str.strip()
    df["station_name"] = df.get("station_name", "").fillna("").astype(str).str.slice(0, 300)
    df["latitude"]     = pd.to_numeric(df.get("latitude"), errors="coerce")
    df["longitude"]    = pd.to_numeric(df.get("longitude"), errors="coerce")
    df = df.dropna(subset=["osm_node_id", "latitude", "longitude"])
    df = df[df["osm_node_id"].str.len() > 0]
    df = df.drop_duplicates(subset=["osm_node_id"])

    eng = engine or _build_engine()
    Session = sessionmaker(bind=eng)
    session = Session()

    try:
        session.execute(text(
            "TRUNCATE TABLE entrepot.osm_railway_stations RESTART IDENTITY CASCADE"
        ))
        session.commit()

        source_id = _get_or_create_source(session)
        session.commit()

        rows = []
        for _, r in df.iterrows():
            rows.append({
                "osm_node_id":  r["osm_node_id"][:30],
                "station_name": r["station_name"],
                "latitude":     round(float(r["latitude"]), 6),
                "longitude":    round(float(r["longitude"]), 6),
                "source_id":    source_id,
            })

        if rows:
            session.bulk_insert_mappings(OSMRailwayStation, rows)
            session.commit()

        log.info("[OSM Import] %d lignes insérées dans osm_railway_stations.", len(rows))
        return len(rows)

    except Exception as exc:
        session.rollback()
        log.error("[OSM Import] Erreur : %s", exc)
        raise
    finally:
        session.close()
