"""
Importeur — Scraping Wikipedia → entrepot.wikipedia_named_trains

Lit le CSV produit par extractors/scraping_extractor.py (ou un chemin fourni),
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
DEFAULT_CSV = _BASE / "outputs" / "wikipedia_named_trains.csv"

_SOURCE_NAME = "wikipedia_named_trains"
_SOURCE_TYPE = "Scraping"
_SOURCE_URL  = "https://en.wikipedia.org/wiki/List_of_named_passenger_trains_of_Europe"
_SOURCE_DESC = "Scraping Wikipedia — trains passagers nommés d'Europe (CC BY-SA 4.0)"


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
        license_type="CC BY-SA 4.0",
        is_active=True,
    )
    session.add(new_src)
    session.flush()
    return new_src.source_id


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


def import_wikipedia(engine=None, csv_path: pathlib.Path | str | None = None) -> int:
    """
    Lit le CSV Wikipedia, normalise et insère dans wikipedia_named_trains.

    Retourne le nombre de lignes insérées (0 si CSV absent ou vide).
    """
    from models import WikipediaNamedTrain

    path = pathlib.Path(csv_path) if csv_path else DEFAULT_CSV
    if not path.exists():
        log.warning("[Wikipedia Import] CSV introuvable : %s — aucune insertion.", path)
        return 0

    df = pd.read_csv(path)
    for col in ("train_name", "operator", "countries", "source_url"):
        if col not in df.columns:
            df[col] = ""

    df["train_name"] = df["train_name"].astype(str).str.strip()
    df = df[df["train_name"].str.len() > 0]
    df = df[~df["train_name"].str.lower().isin(("nan", "none"))]
    df = df.drop_duplicates(subset=["train_name"])

    eng = engine or _build_engine()
    Session = sessionmaker(bind=eng)
    session = Session()

    try:
        session.execute(text(
            "TRUNCATE TABLE entrepot.wikipedia_named_trains RESTART IDENTITY CASCADE"
        ))
        session.commit()

        source_id = _get_or_create_source(session)
        session.commit()

        rows = [
            {
                "train_name": r["train_name"][:299],
                "operator":   _clean_str(r.get("operator"), 299),
                "countries":  _clean_str(r.get("countries"), 199),
                "source_url": _clean_str(r.get("source_url"), 499),
                "source_id":  source_id,
            }
            for _, r in df.iterrows()
        ]

        if rows:
            session.bulk_insert_mappings(WikipediaNamedTrain, rows)
            session.commit()

        log.info("[Wikipedia Import] %d lignes insérées dans wikipedia_named_trains.", len(rows))
        return len(rows)

    except Exception as exc:
        session.rollback()
        log.error("[Wikipedia Import] Erreur : %s", exc)
        raise
    finally:
        session.close()
