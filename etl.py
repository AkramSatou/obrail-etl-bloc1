"""
ObRail Europe — ETL Bloc 1 (SQLAlchemy, multi-SGBD)
====================================================
Lit les fichiers CSV sources, transforme les données et charge le schéma
`entrepot` d'une base PostgreSQL ou MySQL.

Variables d'environnement :
    DB_TARGET   postgresql (défaut) | mysql
    DB_HOST     localhost
    DB_PORT     5432 (postgresql) / 3306 (mysql)
    DB_USER     obrail
    DB_PASSWORD obrail
    DB_NAME     obrail (postgresql) / obrail_europe_db (mysql)

Fichiers sources (répertoire du script) :
    eu_trips.csv          → day_trips
    eu_trips_night.csv    → night_trips (EU)
    de_night.csv          → night_trips (DE)

Usage :
    python etl.py
    DB_TARGET=mysql python etl.py
"""

import logging
import math
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv(Path(__file__).resolve().parent / ".env")

# ── Configuration ─────────────────────────────────────────────────────────────

DB_TARGET = os.getenv("DB_TARGET", "postgresql")

_DEFAULTS = {
    "postgresql": dict(host="localhost", port="5432",
                       user="obrail", password="obrail", db="obrail"),
    "mysql":      dict(host="localhost", port="3306",
                       user="root",   password="",      db="obrail_europe_db"),
}

_d = _DEFAULTS[DB_TARGET]
DB_HOST     = os.getenv("DB_HOST",     _d["host"])
DB_PORT     = os.getenv("DB_PORT",     _d["port"])
DB_USER     = os.getenv("DB_USER",     _d["user"])
DB_PASSWORD = os.getenv("DB_PASSWORD", _d["password"])
DB_NAME     = os.getenv("DB_NAME",     _d["db"])

CHUNK_SIZE = 500

BASE_DIR = Path(__file__).resolve().parent
FILES = {
    "day":      Path(os.getenv("CSV_DAY",      str(BASE_DIR / "eu_trips.csv"))),
    "night_eu": Path(os.getenv("CSV_NIGHT_EU", str(BASE_DIR / "eu_trips_night.csv"))),
    "night_de": Path(os.getenv("CSV_NIGHT_DE", str(BASE_DIR / "de_night.csv"))),
}

# ── Données de référence (identiques à etl_obrail.py) ────────────────────────

COUNTRIES = [
    ("AT", "Autriche",           "Europe/Vienna"),
    ("BE", "Belgique",           "Europe/Brussels"),
    ("CH", "Suisse",             "Europe/Zurich"),
    ("CZ", "République tchèque", "Europe/Prague"),
    ("DE", "Allemagne",          "Europe/Berlin"),
    ("ES", "Espagne",            "Europe/Madrid"),
    ("FR", "France",             "Europe/Paris"),
    ("IT", "Italie",             "Europe/Rome"),
    ("NL", "Pays-Bas",           "Europe/Amsterdam"),
    ("PL", "Pologne",            "Europe/Warsaw"),
]

DATA_SOURCES = [
    ("eu_trips_day",     "CSV",  None, "Trajets trains de jour européens — eu_trips.csv",       "Open Data",           True),
    ("eu_trips_night",   "CSV",  None, "Trajets trains de nuit européens — eu_trips_night.csv", "Open Data",           True),
    ("de_night",         "CSV",  None, "Trajets trains de nuit Allemagne GTFS — de_night.csv",  "Open Data",           True),
    ("mobilitydatabase", "API",  "https://mobilitydatabase.org", "Mobility Database — référentiel GTFS", "CC BY 4.0", True),
    ("sncf_gtfs",        "GTFS", "https://ressources.data.sncf.com", "GTFS SNCF",               "Open License Etalab", True),
    ("db_gtfs",          "GTFS", "https://data.deutschebahn.com",    "GTFS Deutsche Bahn",      "CC BY 4.0",           True),
    ("renfe_gtfs",       "GTFS", "https://data.renfe.com",           "GTFS Renfe",              "Open Data",           True),
]

OPERATORS = [
    ("SNCF",       "SNCF Voyageurs",              "FR", "https://www.sncf.com",       False, True),
    ("DB",         "Deutsche Bahn",               "DE", "https://www.bahn.de",        False, True),
    ("RENFE",      "Renfe Operadora",             "ES", "https://www.renfe.com",      False, True),
    ("TRENITALIA", "Trenitalia",                  "IT", "https://www.trenitalia.com", True,  True),
    ("OBB",        "Österreichische Bundesbahnen","AT", "https://www.oebb.at",        True,  True),
    ("NJ",         "Nightjet (ÖBB)",              "AT", "https://www.nightjet.com",   True,  False),
    ("INTERCITES", "Intercités de Nuit (SNCF)",   "FR", "https://www.sncf.com",       True,  False),
    ("EURONIGHT",  "EuroNight (DB/ÖBB/PKP)",      "DE", None,                         True,  False),
]

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(BASE_DIR / "etl_obrail.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Connexion ─────────────────────────────────────────────────────────────────


def build_engine(target: str = DB_TARGET, host=DB_HOST, port=DB_PORT,
                 user=DB_USER, password=DB_PASSWORD, db=DB_NAME):
    """Construit le moteur SQLAlchemy selon la cible SGBD."""
    if target == "postgresql":
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    elif target == "mysql":
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    else:
        raise ValueError(f"DB_TARGET inconnu : {target!r}. Valeurs acceptées : postgresql, mysql")
    engine = create_engine(url, pool_pre_ping=True)
    log.info("Moteur créé : %s", engine.url.render_as_string(hide_password=True))
    return engine


def ensure_schema(engine):
    """Crée le schéma 'entrepot' si la cible est PostgreSQL."""
    if engine.dialect.name == "postgresql":
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS entrepot"))
            conn.commit()
        log.info("Schéma 'entrepot' vérifié/créé.")


# ── Utilitaires ───────────────────────────────────────────────────────────────

def safe(v):
    if v is None:
        return None
    try:
        if math.isnan(float(v)):
            return None
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    return None if s in ("", "nan", "None", "NaN") else s


def safe_int(v):
    v = safe(v)
    if v is None:
        return None
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def safe_float(v, decimals=6):
    v = safe(v)
    if v is None:
        return None
    try:
        f = float(v)
        return round(f, decimals) if not (math.isnan(f) or math.isinf(f)) else None
    except (TypeError, ValueError):
        return None


def infer_country_day(row):
    rid = str(row.get("route_id", ""))
    rln = str(row.get("route_long_name", "")).upper()
    oid = str(row.get("origin_stop_id", ""))
    if rid.startswith("FR:") or "OCETrain" in oid:
        return "FR"
    ES_KW = ["MADRID", "GETAFE", "LEGANE", "MOSTOLES", "HUMANES", "MAJADAHONDA",
             "MONTSERRAT", "ALCORCON", "VILLALBA", "ALCALA", "POZUELO",
             "FUENLABRADA", "PARLA", "PINTO", "MONCLOA", "TORREJ", "COSLADA"]
    if "par_" in oid or any(k in rln for k in ES_KW):
        return "ES"
    IT_KW = ["FIRENZE", "PISA", "LIVORNO", "ROMA", "MILANO", "NAPOLI", "VENEZIA",
             "BOLOGNA", "TORINO", "PALERMO", "LA SPEZIA", "PARMA", "GENOVA",
             "LUCCA", "SIENA", "AREZZO", "VIAREGGIO", "AULLA", "FAENZA",
             "PISTOIA", "PORRETTA"]
    if any(k in rln for k in IT_KW):
        return "IT"
    return "DE"


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ── Extraction ────────────────────────────────────────────────────────────────

def extract(csv_paths: dict | None = None) -> dict:
    paths = csv_paths or FILES
    log.info("═" * 60)
    log.info("ÉTAPE 1 — EXTRACTION")
    log.info("═" * 60)
    dfs = {}
    for key, path in paths.items():
        path = Path(path)
        if not path.exists():
            log.error("Fichier introuvable : %s", path)
            sys.exit(1)
        log.info("Lecture de %s …", path.name)
        dfs[key] = pd.read_csv(path, low_memory=False)
        log.info("  → %d lignes", len(dfs[key]))
    return dfs


# ── Transformation ────────────────────────────────────────────────────────────

def transform(dfs: dict) -> dict:
    log.info("═" * 60)
    log.info("ÉTAPE 2 — TRANSFORMATION")
    log.info("═" * 60)
    day      = dfs["day"].copy()
    night_eu = dfs["night_eu"].copy()
    night_de = dfs["night_de"].copy()

    log.info("Inférence pays pour eu_trips …")
    day["country"] = day.apply(infer_country_day, axis=1)
    log.info("  → %s", day["country"].value_counts().to_dict())

    night_eu["orig_ext"] = (night_eu["origin_stop_name"].astype(str)
                            + "_" + night_eu["origin_stop_lat"].astype(str))
    night_eu["dest_ext"] = (night_eu["destination_stop_name"].astype(str)
                            + "_" + night_eu["destination_stop_lat"].astype(str))

    # Référentiel stops
    log.info("Construction du référentiel des gares …")
    frames = []
    for df, ec, nc, latc, lonc, cc in [
        (day,      "origin_stop_id",      "origin_stop_name",      "origin_stop_lat",      "origin_stop_lon",      "country"),
        (day,      "destination_stop_id", "destination_stop_name", "destination_stop_lat", "destination_stop_lon", "country"),
        (night_eu, "orig_ext",            "origin_stop_name",      "origin_stop_lat",      "origin_stop_lon",      "country"),
        (night_eu, "dest_ext",            "destination_stop_name", "destination_stop_lat", "destination_stop_lon", "country"),
        (night_de, "origin_stop_id",      "origin_stop_name",      "origin_stop_lat",      "origin_stop_lon",      "country"),
        (night_de, "destination_stop_id", "destination_stop_name", "destination_stop_lat", "destination_stop_lon", "country"),
    ]:
        sub = df[[ec, nc, latc, lonc, cc]].copy()
        sub.columns = ["ext_id", "name", "lat", "lon", "country"]
        frames.append(sub)

    stops = (pd.concat(frames)
               .drop_duplicates(subset=["ext_id"])
               .dropna(subset=["lat", "lon"])
               .reset_index(drop=True))
    stops = stops[stops["ext_id"].astype(str).str.strip().ne("")
                  & stops["ext_id"].astype(str).ne("nan")]
    stops["stop_db_id"] = range(1, len(stops) + 1)
    stop_lookup = dict(zip(stops["ext_id"].astype(str), stops["stop_db_id"]))
    log.info("  → %d gares uniques", len(stops))

    # Référentiel routes
    log.info("Construction du référentiel des lignes …")
    r_day = day[["route_id", "route_long_name", "route_type"]].drop_duplicates("route_id").copy()
    r_day["service_type"] = "jour"; r_day["source_id"] = 1

    r_de = night_de[["route_id", "route_long_name", "route_type"]].drop_duplicates("route_id").copy()
    r_de["service_type"] = "nuit"; r_de["source_id"] = 3

    r_eu = night_eu[["route_id"]].drop_duplicates().copy()
    r_eu["route_long_name"] = None; r_eu["route_type"] = 2
    r_eu["service_type"] = "nuit"; r_eu["source_id"] = 2

    routes = (pd.concat([r_day, r_de, r_eu])
                .drop_duplicates("route_id")
                .reset_index(drop=True))
    routes["route_db_id"] = range(1, len(routes) + 1)
    route_lookup = dict(zip(routes["route_id"].astype(str), routes["route_db_id"]))
    log.info("  → %d lignes uniques", len(routes))

    return dict(day=day, night_eu=night_eu, night_de=night_de,
                stops=stops, routes=routes,
                stop_lookup=stop_lookup, route_lookup=route_lookup)


# ── Chargement ────────────────────────────────────────────────────────────────

def _truncate(session, engine):
    """Vide toutes les tables dans l'ordre inverse des FK."""
    dialect = engine.dialect.name
    log.info("Vidage des tables (dialecte=%s) …", dialect)
    if dialect == "postgresql":
        session.execute(text(
            "TRUNCATE TABLE entrepot.day_trips, entrepot.night_trips, "
            "entrepot.routes, entrepot.stops, entrepot.operators, "
            "entrepot.data_sources, entrepot.countries, entrepot.etl_logs "
            "RESTART IDENTITY CASCADE"
        ))
    else:
        session.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for t in ["night_trips", "day_trips", "routes", "stops",
                  "operators", "data_sources", "countries", "etl_logs"]:
            session.execute(text(f"TRUNCATE TABLE `{t}`"))
        session.execute(text("SET FOREIGN_KEY_CHECKS=1"))
    session.commit()
    log.info("  → tables vidées.")


def _bulk(session, model, rows: list[dict], label: str) -> int:
    """INSERT en batch via SQLAlchemy bulk_insert_mappings."""
    if not rows:
        return 0
    inserted = 0
    for batch in chunks(rows, CHUNK_SIZE):
        session.bulk_insert_mappings(model, batch)
        session.commit()
        inserted += len(batch)
    log.info("  → %d insérés  [%s]", inserted, label)
    return inserted


def load_referentiels(session):
    from models import Country, DataSource, Operator
    _bulk(session, Country,
          [{"country_code": cc, "country_name": cn, "timezone": tz}
           for cc, cn, tz in COUNTRIES], "countries")
    _bulk(session, DataSource,
          [{"source_name": n, "source_type": t, "source_url": u,
            "description": d, "license_type": l, "is_active": a}
           for n, t, u, d, l, a in DATA_SOURCES], "data_sources")
    _bulk(session, Operator,
          [{"operator_code": c, "operator_name": n, "country_code": co,
            "website": w, "is_night_train_operator": nt, "is_day_train_operator": dt}
           for c, n, co, w, nt, dt in OPERATORS], "operators")


def load_stops(session, stops_df) -> int:
    from models import Stop
    rows = []
    for _, r in stops_df.iterrows():
        ext = safe(r["ext_id"])
        lat = safe_float(r["lat"])
        lon = safe_float(r["lon"])
        if not ext or lat is None or lon is None:
            continue
        rows.append({
            "stop_id": int(r["stop_db_id"]), "external_stop_id": ext,
            "stop_name": safe(r["name"]) or ext,
            "stop_name_normalized": None,
            "latitude": lat, "longitude": lon,
            "country_code": safe(r["country"]),
            "city": None, "is_major_station": False, "source_id": None,
        })
    return _bulk(session, Stop, rows, "stops")


def load_routes(session, routes_df) -> int:
    from models import Route
    rows = []
    for _, r in routes_df.iterrows():
        ext = safe(r["route_id"])
        if not ext:
            continue
        rows.append({
            "route_id": int(r["route_db_id"]), "external_route_id": ext,
            "route_short_name": None,
            "route_long_name": safe(r.get("route_long_name")),
            "route_type": safe_int(r.get("route_type", 2)) or 2,
            "service_type": safe(r["service_type"]),
            "operator_id": None, "source_id": safe_int(r["source_id"]),
        })
    return _bulk(session, Route, rows, "routes")


def load_day_trips(session, day_df, stop_lookup, route_lookup) -> tuple[int, int]:
    from models import DayTrip
    started = datetime.now()
    rows, skipped = [], 0
    for _, r in day_df.iterrows():
        o = stop_lookup.get(str(r["origin_stop_id"]))
        d = stop_lookup.get(str(r["destination_stop_id"]))
        if not o or not d:
            skipped += 1
            continue
        dep = safe_int(r.get("departure_minutes"))
        arr = safe_int(r.get("arrival_minutes"))
        rows.append({
            "external_trip_id":    safe(r["trip_id"]),
            "external_service_id": safe(r.get("service_id")),
            "route_id":            route_lookup.get(str(r["route_id"])),
            "route_short_name":    None,
            "route_long_name":     safe(r.get("route_long_name")),
            "route_type":          safe_int(r.get("route_type", 2)) or 2,
            "origin_stop_id":      o,
            "destination_stop_id": d,
            "origin_stop_name":    safe(r["origin_stop_name"]),
            "destination_stop_name": safe(r["destination_stop_name"]),
            "origin_stop_lat":     safe_float(r["origin_stop_lat"]),
            "origin_stop_lon":     safe_float(r["origin_stop_lon"]),
            "destination_stop_lat": safe_float(r["destination_stop_lat"]),
            "destination_stop_lon": safe_float(r["destination_stop_lon"]),
            "country":             safe(r["country"]),
            "source_id":           1,
            "departure_minutes":   dep,
            "arrival_minutes":     arr,
            "departure_hour":      (dep // 60) % 24 if dep is not None else None,
            "arrival_hour":        (arr // 60) % 24 if arr is not None else None,
            "duration_minutes":    safe_int(r.get("duration_minutes")),
            "n_stops":             safe_int(r.get("n_stops", 2)) or 2,
            "distance_km":         None,
            "is_international":    False,
        })
    inserted = _bulk(session, DayTrip, rows, "day_trips")
    _write_log(session, "load_day_trips", 1, "SUCCESS", started,
               len(day_df), inserted, 0, skipped)
    return inserted, skipped


def load_night_trips(session, night_eu_df, night_de_df,
                     stop_lookup, route_lookup) -> tuple[int, int]:
    from models import NightTrip
    started = datetime.now()
    rows, skipped = [], 0

    for _, r in night_eu_df.iterrows():
        o_ext = f"{r['origin_stop_name']}_{r['origin_stop_lat']}"
        d_ext = f"{r['destination_stop_name']}_{r['destination_stop_lat']}"
        o = stop_lookup.get(o_ext)
        d = stop_lookup.get(d_ext)
        if not o or not d:
            skipped += 1
            continue
        rows.append({
            "external_trip_id":    safe(r["trip_id"]),
            "external_service_id": None,
            "route_id":            route_lookup.get(str(r["route_id"])),
            "route_short_name":    None, "route_long_name": None, "route_type": 2,
            "origin_stop_id":      o, "destination_stop_id": d,
            "origin_stop_name":    safe(r["origin_stop_name"]),
            "destination_stop_name": safe(r["destination_stop_name"]),
            "origin_stop_lat":     safe_float(r["origin_stop_lat"]),
            "origin_stop_lon":     safe_float(r["origin_stop_lon"]),
            "destination_stop_lat": safe_float(r["destination_stop_lat"]),
            "destination_stop_lon": safe_float(r["destination_stop_lon"]),
            "country": safe(r["country"]), "source_id": 2,
            "departure_minutes": None, "arrival_minutes": None,
            "departure_hour": None, "arrival_hour": None,
            "duration_minutes": safe_int(r.get("duration_minutes")),
            "n_stops": safe_int(r.get("n_stops", 2)) or 2,
            "distance_km": None, "is_international": False,
        })

    for _, r in night_de_df.iterrows():
        o = stop_lookup.get(str(r["origin_stop_id"]))
        d = stop_lookup.get(str(r["destination_stop_id"]))
        if not o or not d:
            skipped += 1
            continue
        dep = safe_int(r.get("departure_minutes"))
        arr = safe_int(r.get("arrival_minutes"))
        rows.append({
            "external_trip_id":    safe(r["trip_id"]),
            "external_service_id": safe(r.get("service_id")),
            "route_id":            route_lookup.get(str(r["route_id"])),
            "route_short_name":    None,
            "route_long_name":     safe(r.get("route_long_name")),
            "route_type":          safe_int(r.get("route_type", 2)) or 2,
            "origin_stop_id":      o, "destination_stop_id": d,
            "origin_stop_name":    safe(r["origin_stop_name"]),
            "destination_stop_name": safe(r["destination_stop_name"]),
            "origin_stop_lat":     safe_float(r["origin_stop_lat"]),
            "origin_stop_lon":     safe_float(r["origin_stop_lon"]),
            "destination_stop_lat": safe_float(r["destination_stop_lat"]),
            "destination_stop_lon": safe_float(r["destination_stop_lon"]),
            "country": safe(r["country"]), "source_id": 3,
            "departure_minutes": dep, "arrival_minutes": arr,
            "departure_hour": safe_int(r.get("departure_hh")),
            "arrival_hour":   safe_int(r.get("arrival_hh")),
            "duration_minutes": safe_int(r.get("duration_minutes")),
            "n_stops": safe_int(r.get("n_stops", 2)) or 2,
            "distance_km": None, "is_international": False,
        })

    inserted = _bulk(session, NightTrip, rows, "night_trips")
    _write_log(session, "load_night_trips", 2, "SUCCESS", started,
               len(night_eu_df) + len(night_de_df), inserted, 0, skipped)
    return inserted, skipped


def _write_log(session, job, source_id, status, started,
               processed=0, inserted=0, updated=0, rejected=0, error=None):
    from models import EtlLog
    session.add(EtlLog(
        job_name=job, source_id=source_id, status=status,
        started_at=started, ended_at=datetime.now(),
        records_processed=processed, records_inserted=inserted,
        records_updated=updated, records_rejected=rejected,
        error_message=error,
    ))
    session.commit()


# ── Orchestrateur principal ───────────────────────────────────────────────────

def run(engine=None, csv_paths: dict | None = None) -> dict:
    """
    Lance l'ETL complet. Retourne un dict de comptages.
    Paramètre `engine` utile pour les tests (engine de test injecté).
    """
    from models import Base

    if engine is None:
        engine = build_engine()

    ensure_schema(engine)
    Base.metadata.create_all(bind=engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    t_start = time.time()

    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║       ObRail Europe — ETL Bloc 1  (SQLAlchemy)          ║")
    log.info("║       Cible : %-42s ║", engine.url.render_as_string(hide_password=True))
    log.info("╚══════════════════════════════════════════════════════════╝")

    try:
        dfs  = extract(csv_paths)
        data = transform(dfs)

        log.info("═" * 60)
        log.info("ÉTAPE 3 — CHARGEMENT")
        log.info("═" * 60)

        _truncate(session, engine)
        load_referentiels(session)
        n_stops  = load_stops(session, data["stops"])
        n_routes = load_routes(session, data["routes"])
        d_ok, d_ko = load_day_trips(session, data["day"],
                                    data["stop_lookup"], data["route_lookup"])
        n_ok, n_ko = load_night_trips(session, data["night_eu"], data["night_de"],
                                      data["stop_lookup"], data["route_lookup"])

        elapsed = round(time.time() - t_start, 2)
        log.info("═" * 60)
        log.info("RÉSUMÉ FINAL — %ss", elapsed)
        log.info("  Gares        : %d", n_stops)
        log.info("  Routes       : %d", n_routes)
        log.info("  Day trips    : %d  (ignorés : %d)", d_ok, d_ko)
        log.info("  Night trips  : %d  (ignorés : %d)", n_ok, n_ko)
        log.info("ETL termine avec succes.")

        return dict(stops=n_stops, routes=n_routes,
                    day_trips=d_ok, day_skipped=d_ko,
                    night_trips=n_ok, night_skipped=n_ko)

    except Exception as e:
        log.error("❌ Erreur fatale : %s", e, exc_info=True)
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    run()
