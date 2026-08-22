"""
Importeur — API Eurostat → entrepot.eurostat_rail_passengers

Lit le CSV produit par extractors/api_extractor.py (ou un chemin fourni),
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
DEFAULT_CSV = _BASE / "outputs" / "eurostat_rail_passengers.csv"

_SOURCE_NAME = "eurostat_rail_api"
_SOURCE_TYPE = "API"
_SOURCE_URL  = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/RAIL_PA_QUARTAL"
)
_SOURCE_DESC = "API Eurostat RAIL_PA_QUARTAL — passagers ferroviaires trimestriels par pays UE"


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
    """Retourne l'id de la source Eurostat, en l'insérant si elle n'existe pas encore."""
    from models import DataSource
    existing = session.query(DataSource).filter_by(source_name=_SOURCE_NAME).first()
    if existing:
        return existing.source_id
    new_src = DataSource(
        source_name=_SOURCE_NAME,
        source_type=_SOURCE_TYPE,
        source_url=_SOURCE_URL,
        description=_SOURCE_DESC,
        license_type="CC BY 4.0",
        is_active=True,
    )
    session.add(new_src)
    session.flush()
    return new_src.source_id


def import_eurostat(engine=None, csv_path: pathlib.Path | str | None = None) -> int:
    """
    Lit le CSV Eurostat, normalise et insère dans eurostat_rail_passengers.

    Paramètres :
        engine   : SQLAlchemy engine (injection pour les tests ; sinon env vars)
        csv_path : chemin vers le CSV ; par défaut outputs/eurostat_rail_passengers.csv

    Retourne le nombre de lignes insérées (0 si CSV absent ou vide).
    """
    from models import EurostatRailPassenger

    path = pathlib.Path(csv_path) if csv_path else DEFAULT_CSV
    if not path.exists():
        log.warning("[Eurostat Import] CSV introuvable : %s — aucune insertion.", path)
        return 0

    df = pd.read_csv(path)
    df["country_code"] = df["country_code"].astype(str).str.strip()
    df["period"]       = df["period"].astype(str).str.strip()
    df["passengers_k"] = pd.to_numeric(df.get("passengers_k"), errors="coerce")
    df = df.dropna(subset=["country_code", "period"])
    df = df[df["country_code"].str.len() > 0]
    # L'API Eurostat peut retourner des doublons (country, "??") pour les périodes
    # non définies — on garde la première occurrence pour respecter la contrainte UNIQUE.
    df = df.drop_duplicates(subset=["country_code", "period"])

    eng = engine or _build_engine()
    Session = sessionmaker(bind=eng)
    session = Session()

    try:
        # Idempotence : vider la table avant insertion
        session.execute(text(
            "TRUNCATE TABLE entrepot.eurostat_rail_passengers RESTART IDENTITY CASCADE"
        ))
        session.commit()

        source_id = _get_or_create_source(session)
        session.commit()

        rows = []
        for _, r in df.iterrows():
            pax = r["passengers_k"]
            rows.append({
                "country_code": r["country_code"][:10],
                "period":       r["period"][:10],
                "passengers_k": None if pd.isna(pax) else round(float(pax), 1),
                "source_id":    source_id,
            })

        if rows:
            session.bulk_insert_mappings(EurostatRailPassenger, rows)
            session.commit()

        log.info("[Eurostat Import] %d lignes insérées dans eurostat_rail_passengers.", len(rows))
        return len(rows)

    except Exception as exc:
        session.rollback()
        log.error("[Eurostat Import] Erreur : %s", exc)
        raise
    finally:
        session.close()
