"""
Extracteur 4 — Base de données : consultation du schéma entrepot (PostgreSQL)

Requêtes SQL documentées dans docs/SQL_DOCUMENTATION.md.
Expose des statistiques agrégées sur les données déjà chargées par etl.py.
"""

from __future__ import annotations

import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

DB_TARGET = os.getenv("DB_TARGET", "postgresql")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "obrail")
DB_PASSWORD = os.getenv("DB_PASSWORD", "obrail")
DB_NAME = os.getenv("DB_NAME", "obrail")


def _default_url() -> str:
    if DB_TARGET == "postgresql":
        return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"


def extract_db(engine=None) -> dict[str, pd.DataFrame]:
    """
    Interroge le schéma entrepot et retourne un dict de DataFrames :
      - 'volumes'    : nombre de trajets jour/nuit par pays
      - 'top_routes' : 10 routes les plus fréquentes
      - 'etl_log'    : dernière exécution ETL enregistrée

    Retourne des DataFrames vides si la base est inaccessible.
    """
    try:
        eng = engine or create_engine(_default_url(), pool_pre_ping=True)
        with eng.connect() as conn:
            volumes = _query_volumes(conn)
            top_routes = _query_top_routes(conn)
            etl_log = _query_etl_log(conn)
        log.info("[DB] Extraction entrepot : volumes=%d, top_routes=%d", len(volumes), len(top_routes))
        return {"volumes": volumes, "top_routes": top_routes, "etl_log": etl_log}
    except Exception as exc:
        log.error("[DB] Echec connexion entrepot : %s — DataFrames vides retournes.", exc)
        return {
            "volumes": pd.DataFrame(columns=["country_code", "day_trips", "night_trips"]),
            "top_routes": pd.DataFrame(columns=["route_id", "route_name", "nb_trajets"]),
            "etl_log": pd.DataFrame(columns=["started_at", "status", "rows_inserted"]),
        }


# ── Requêtes SQL ──────────────────────────────────────────────────────────────
# Chaque requête est commentée (choix de jointure, filtres, optimisations)
# et documentée dans docs/SQL_DOCUMENTATION.md.

def _query_volumes(conn) -> pd.DataFrame:
    """
    Volumes de trajets par pays (LEFT JOIN : inclut pays sans trajet nuit).

    Jointure :   stops → day_trips / night_trips → countries
    Filtre :     seuls les arrêts d'origine (évite double-comptage)
    Index hint : index sur stop_id (PK) et country_id (FK) automatiques
    """
    sql = text("""
        SELECT
            c.country_code,
            c.country_name,
            COUNT(DISTINCT dt.trip_id)  AS day_trips,
            COUNT(DISTINCT nt.trip_id)  AS night_trips
        FROM entrepot.countries   c
        LEFT JOIN entrepot.stops  s  ON s.country_code  = c.country_code
        LEFT JOIN entrepot.day_trips   dt ON dt.origin_stop_id = s.stop_id
        LEFT JOIN entrepot.night_trips nt ON nt.origin_stop_id = s.stop_id
        GROUP BY c.country_code, c.country_name
        ORDER BY (COUNT(DISTINCT dt.trip_id) + COUNT(DISTINCT nt.trip_id)) DESC
    """)
    return pd.read_sql(sql, conn)


def _query_top_routes(conn) -> pd.DataFrame:
    """
    10 routes les plus fréquentes en termes de trajets journaliers.

    INNER JOIN : on ne garde que les routes avec au moins un trajet.
    LIMIT 10   : top-N, optimisé par ORDER BY + LIMIT plutôt que sous-requête.
    """
    sql = text("""
        SELECT
            r.route_id,
            r.route_short_name,
            r.route_long_name,
            COUNT(dt.trip_id) AS nb_trajets
        FROM entrepot.routes    r
        INNER JOIN entrepot.day_trips dt ON dt.route_id = r.route_id
        GROUP BY r.route_id, r.route_short_name, r.route_long_name
        ORDER BY nb_trajets DESC
        LIMIT 10
    """)
    return pd.read_sql(sql, conn)


def _query_etl_log(conn) -> pd.DataFrame:
    """
    Dernière exécution ETL enregistrée dans la table etl_logs.

    ORDER BY started_at DESC LIMIT 1 : lecture unique du dernier log.
    """
    sql = text("""
        SELECT
            started_at,
            ended_at,
            status,
            records_inserted,
            records_rejected,
            error_message
        FROM entrepot.etl_logs
        ORDER BY started_at DESC
        LIMIT 1
    """)
    return pd.read_sql(sql, conn)
