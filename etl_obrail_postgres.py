"""
============================================================
ObRail Europe — Script ETL (version PostgreSQL)
============================================================
Auteur  : Équipe ObRail Europe — MSPR EPSI Bloc E6.1
Version : 1.0.0
Certification : RNCP 37827 — Développeur IA, Bloc 1, C1

Description :
    Adaptation PostgreSQL du pipeline MySQL (etl_obrail_mysql.py).
    Charge les données dans obrail_europe_db (PostgreSQL), en
    utilisant le schéma unifié (01_create_schema_postgres.sql).

Différences avec la version MySQL :
    - pymysql → psycopg2
    - Guillemets doubles pour les identifiants
    - AUTO_INCREMENT → SERIAL (géré dans le schéma SQL)
    - TRUNCATE ... RESTART IDENTITY CASCADE
    - day_trips + night_trips → table trips unifiée avec is_night_trip
    - train_type ajouté à etl_logs pour cohérence avec MySQL

Prérequis :
    pip install pandas psycopg2-binary python-dotenv

Configuration :
    Copier .env.example en .env et renseigner les valeurs PG_*.
============================================================
"""

import os
import sys
import math
import time
import logging
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path


# ── Configuration logging ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_obrail_postgres.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ── Chargement .env ──────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

PG_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "user":     os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
    "dbname":   os.getenv("PG_DATABASE", "obrail_europe_db"),
}

DATA_DIR   = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
CHUNK_SIZE = 500


# ── Fichiers sources ─────────────────────────────────────────────────────────
FILES = {
    "day":      DATA_DIR / "eu_trips.csv",
    "night_eu": DATA_DIR / "eu_trips_night.csv",
    "night_de": DATA_DIR / "de_night.csv",
}


# ── Données de référence ─────────────────────────────────────────────────────
COUNTRIES = [
    ("AT", "Autriche",           "Europe/Vienna"),
    ("BE", "Belgique",           "Europe/Brussels"),
    ("CH", "Suisse",             "Europe/Zurich"),
    ("CZ", "République tchèque", "Europe/Prague"),
    ("DE", "Allemagne",          "Europe/Berlin"),
    ("DK", "Danemark",           "Europe/Copenhagen"),
    ("ES", "Espagne",            "Europe/Madrid"),
    ("FI", "Finlande",           "Europe/Helsinki"),
    ("FR", "France",             "Europe/Paris"),
    ("GB", "Royaume-Uni",        "Europe/London"),
    ("HR", "Croatie",            "Europe/Zagreb"),
    ("HU", "Hongrie",            "Europe/Budapest"),
    ("IT", "Italie",             "Europe/Rome"),
    ("LU", "Luxembourg",         "Europe/Luxembourg"),
    ("NL", "Pays-Bas",           "Europe/Amsterdam"),
    ("NO", "Norvège",            "Europe/Oslo"),
    ("PL", "Pologne",            "Europe/Warsaw"),
    ("PT", "Portugal",           "Europe/Lisbon"),
    ("SE", "Suède",              "Europe/Stockholm"),
    ("SK", "Slovaquie",          "Europe/Bratislava"),
]

DATA_SOURCES = [
    ("eu_trips_day",   "CSV",  None,                                "Trajets trains de jour européens — eu_trips.csv",          "Open Data"),
    ("eu_trips_night", "CSV",  None,                                "Trajets trains de nuit européens — eu_trips_night.csv",    "Open Data"),
    ("de_night",       "CSV",  None,                                "Trajets trains de nuit Allemagne GTFS — de_night.csv",     "Open Data"),
    ("mobilitydatabase","API", "https://mobilitydatabase.org",      "Mobility Database — référentiel GTFS open source",         "CC BY 4.0"),
    ("sncf_gtfs",      "GTFS", "https://ressources.data.sncf.com", "GTFS SNCF — TER/TGV/Intercités France",                   "Open License Etalab"),
    ("eurostat",       "API",  "https://ec.europa.eu/eurostat",    "Statistiques ferroviaires Eurostat",                       "CC BY 4.0"),
    ("wikipedia_scraping","HTML","https://en.wikipedia.org/wiki/EuroNight","Scraping Wikipedia — trains de nuit européens","CC BY-SA 4.0"),
]

OPERATORS = [
    ("SNCF",       "SNCF Voyageurs",              "FR", "https://www.sncf.com",       True),
    ("DB",         "Deutsche Bahn",               "DE", "https://www.bahn.de",        True),
    ("RENFE",      "Renfe Operadora",             "ES", "https://www.renfe.com",      False),
    ("TRENITALIA", "Trenitalia",                  "IT", "https://www.trenitalia.com", True),
    ("OBB",        "Österreichische Bundesbahnen","AT", "https://www.oebb.at",        True),
    ("NJ",         "Nightjet (ÖBB)",              "AT", "https://www.nightjet.com",   True),
    ("INTERCITES", "Intercités de Nuit (SNCF)",   "FR", "https://www.sncf.com",       True),
    ("EURONIGHT",  "EuroNight (DB/ÖBB/PKP)",      "DE", None,                         True),
]


# ════════════════════════════════════════════════════════════════════════════
# UTILITAIRES (identiques à la version MySQL)
# ════════════════════════════════════════════════════════════════════════════

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


# ════════════════════════════════════════════════════════════════════════════
# CONNEXION
# ════════════════════════════════════════════════════════════════════════════

def get_connection():
    """Ouvre une connexion psycopg2 vers PostgreSQL."""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        conn.autocommit = False
        log.info(f"Connecté à PostgreSQL — {PG_CONFIG['host']}:{PG_CONFIG['port']} / {PG_CONFIG['dbname']}")
        return conn
    except psycopg2.Error as e:
        log.error(f"Impossible de se connecter à PostgreSQL : {e}")
        sys.exit(1)


# ════════════════════════════════════════════════════════════════════════════
# ETL LOGS
# ════════════════════════════════════════════════════════════════════════════

def write_etl_log(conn, job_name, source_id, train_type, status,
                  started_at, records_processed=0, records_inserted=0,
                  records_updated=0, records_rejected=0, error_message=None):
    """
    Insère une entrée dans la table etl_logs.
    La colonne train_type est ajoutée par ALTER dans le schéma postgres
    pour maintenir la parité avec la version MySQL.
    """
    sql = """
        INSERT INTO etl_logs
            (job_name, source_id, train_type, status, started_at, ended_at,
             records_processed, records_inserted, records_updated,
             records_rejected, error_message)
        VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (job_name, source_id, train_type, status, started_at,
                          records_processed, records_inserted, records_updated,
                          records_rejected, error_message))
    conn.commit()


# ════════════════════════════════════════════════════════════════════════════
# ÉTAPE 1 — EXTRACTION
# ════════════════════════════════════════════════════════════════════════════

def extract():
    log.info("═" * 60)
    log.info("ÉTAPE 1 — EXTRACTION")
    log.info("═" * 60)

    dfs = {}
    for key, path in FILES.items():
        if not path.exists():
            log.error(f"Fichier introuvable : {path}")
            sys.exit(1)
        log.info(f"Lecture de {path.name} ...")
        dfs[key] = pd.read_csv(path, low_memory=False)
        log.info(f"  → {len(dfs[key])} lignes chargées")

    return dfs


# ════════════════════════════════════════════════════════════════════════════
# ÉTAPE 2 — TRANSFORMATION
# ════════════════════════════════════════════════════════════════════════════

def transform(dfs):
    log.info("═" * 60)
    log.info("ÉTAPE 2 — TRANSFORMATION")
    log.info("═" * 60)

    day      = dfs["day"].copy()
    night_eu = dfs["night_eu"].copy()
    night_de = dfs["night_de"].copy()

    log.info("Inférence des pays pour eu_trips ...")
    day["country"] = day.apply(infer_country_day, axis=1)
    log.info(f"  → {day['country'].value_counts().to_dict()}")

    night_eu["orig_ext"] = (night_eu["origin_stop_name"].astype(str)
                            + "_" + night_eu["origin_stop_lat"].astype(str))
    night_eu["dest_ext"] = (night_eu["destination_stop_name"].astype(str)
                            + "_" + night_eu["destination_stop_lat"].astype(str))

    log.info("Construction du référentiel des gares ...")
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
    log.info(f"  → {len(stops)} gares uniques")

    log.info("Construction du référentiel des lignes ...")
    routes_day = day[["route_id", "route_long_name", "route_type"]].drop_duplicates(subset=["route_id"]).copy()
    routes_day["service_type"] = "jour"
    routes_day["source_id"]    = 1

    routes_de = night_de[["route_id", "route_long_name", "route_type"]].drop_duplicates(subset=["route_id"]).copy()
    routes_de["service_type"] = "nuit"
    routes_de["source_id"]    = 3

    routes_eu = night_eu[["route_id"]].drop_duplicates().copy()
    routes_eu["route_long_name"] = None
    routes_eu["route_type"]      = 2
    routes_eu["service_type"]    = "nuit"
    routes_eu["source_id"]       = 2

    routes = (pd.concat([routes_day, routes_de, routes_eu])
                .drop_duplicates(subset=["route_id"])
                .reset_index(drop=True))
    routes["route_db_id"] = range(1, len(routes) + 1)
    route_lookup = dict(zip(routes["route_id"].astype(str), routes["route_db_id"]))
    log.info(f"  → {len(routes)} lignes uniques")

    return {
        "day":          day,
        "night_eu":     night_eu,
        "night_de":     night_de,
        "stops":        stops,
        "routes":       routes,
        "stop_lookup":  stop_lookup,
        "route_lookup": route_lookup,
    }


# ════════════════════════════════════════════════════════════════════════════
# ÉTAPE 3 — CHARGEMENT POSTGRESQL
# ════════════════════════════════════════════════════════════════════════════

def truncate_tables(conn):
    """
    Vide toutes les tables en respectant les FK.
    RESTART IDENTITY remet les séquences SERIAL à 1.
    CASCADE propage aux tables dépendantes.
    """
    log.info("TRUNCATE des tables ...")
    tables = ["trips", "routes", "stops",
              "operators", "data_sources", "countries", "etl_logs"]
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f'TRUNCATE TABLE "{t}" RESTART IDENTITY CASCADE')
            log.info(f"  → {t} vidée")
    conn.commit()


def bulk_insert_pg(conn, table: str, columns: list, rows: list, label: str = "") -> int:
    """
    INSERT par batch avec psycopg2.extras.execute_batch.
    Guillemets doubles autour du nom de table pour PostgreSQL.
    """
    if not rows:
        return 0

    placeholders = ", ".join(["%s"] * len(columns))
    cols_str     = ", ".join(f'"{c}"' for c in columns)
    sql          = f'INSERT INTO "{table}" ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'

    inserted = 0
    rejected = 0
    with conn.cursor() as cur:
        for batch in chunks(rows, CHUNK_SIZE):
            try:
                psycopg2.extras.execute_batch(cur, sql, batch, page_size=CHUNK_SIZE)
                inserted += len(batch)
            except psycopg2.Error as e:
                rejected += len(batch)
                conn.rollback()
                log.warning(f"  Batch rejeté ({table}) : {e}")
    conn.commit()

    if label:
        log.info(f"  → {inserted} insérés, {rejected} rejetés  [{label}]")
    return inserted


def load_countries(conn):
    log.info("Insertion COUNTRIES ...")
    rows = [(cc, cn, tz) for cc, cn, tz in COUNTRIES]
    bulk_insert_pg(conn, "countries", ["country_code", "country_name", "timezone"], rows, "countries")


def load_data_sources(conn):
    log.info("Insertion DATA_SOURCES ...")
    rows = [(n, t, u, d, l) for n, t, u, d, l in DATA_SOURCES]
    bulk_insert_pg(conn, "data_sources",
                   ["source_name", "source_type", "source_url", "description", "license_type"],
                   rows, "data_sources")


def load_operators(conn):
    log.info("Insertion OPERATORS ...")
    rows = [(c, n, co, w, nt) for c, n, co, w, nt in OPERATORS]
    bulk_insert_pg(conn, "operators",
                   ["operator_code", "operator_name", "country_code", "website",
                    "is_night_train_operator"],
                   rows, "operators")


def load_stops(conn, stops_df):
    log.info(f"Insertion STOPS ({len(stops_df)} gares) ...")
    rows = []
    for _, r in stops_df.iterrows():
        ext  = safe(r["ext_id"])
        name = safe(r["name"])
        lat  = safe_float(r["lat"])
        lon  = safe_float(r["lon"])
        cc   = safe(r["country"])
        if not ext or lat is None or lon is None:
            continue
        rows.append((r["stop_db_id"], ext, name, None, lat, lon, cc, None, False, None))
    bulk_insert_pg(conn, "stops",
                   ["stop_id", "external_stop_id", "stop_name", "stop_name_normalized",
                    "latitude", "longitude", "country_code", "city",
                    "is_major_station", "source_id"],
                   rows, "stops")


def load_routes(conn, routes_df):
    log.info(f"Insertion ROUTES ({len(routes_df)} lignes) ...")
    rows = []
    for _, r in routes_df.iterrows():
        ext = safe(r["route_id"])
        if not ext:
            continue
        route_name = safe(r.get("route_long_name"))
        rows.append((
            r["route_db_id"], ext,
            route_name,
            safe_int(r.get("route_type", 2)) or 2,
            safe(r["service_type"]),
            None,
            safe_int(r["source_id"]),
        ))
    bulk_insert_pg(conn, "routes",
                   ["route_id", "external_route_id", "route_name", "route_type",
                    "service_type", "operator_id", "source_id"],
                   rows, "routes")


def load_trips(conn, day_df, night_eu_df, night_de_df, stop_lookup, route_lookup):
    """
    Charge tous les trajets dans la table unifiée 'trips'.
    is_night_trip = False pour day_df, True pour night_eu_df et night_de_df.
    """
    total_day = len(day_df)
    total_eu  = len(night_eu_df)
    total_de  = len(night_de_df)
    log.info(f"Insertion TRIPS ({total_day + total_eu + total_de} trajets) ...")
    started = datetime.now()

    COLS = [
        "external_trip_id", "external_service_id", "route_id",
        "origin_stop_id", "destination_stop_id", "source_id",
        "departure_minutes", "arrival_minutes", "duration_minutes",
        "departure_hour", "arrival_hour", "n_stops",
        "distance_km", "is_night_trip", "is_international",
    ]

    rows    = []
    skipped = 0

    # ── Trains de jour (eu_trips.csv) ────────────────────────────────────────
    for _, r in day_df.iterrows():
        o_sid = stop_lookup.get(str(r["origin_stop_id"]))
        d_sid = stop_lookup.get(str(r["destination_stop_id"]))
        if not o_sid or not d_sid:
            skipped += 1
            continue
        dep_m = safe_int(r.get("departure_minutes"))
        arr_m = safe_int(r.get("arrival_minutes"))
        dep_h = (dep_m // 60) % 24 if dep_m is not None else None
        arr_h = (arr_m // 60) % 24 if arr_m is not None else None
        rows.append((
            safe(r["trip_id"]),
            safe(r.get("service_id")),
            route_lookup.get(str(r["route_id"])),
            o_sid, d_sid, 1,
            dep_m, arr_m,
            safe_int(r.get("duration_minutes")),
            dep_h, arr_h,
            safe_int(r.get("n_stops", 2)) or 2,
            None, False, False,
        ))

    # ── Trains de nuit EU ────────────────────────────────────────────────────
    for _, r in night_eu_df.iterrows():
        o_ext = str(r["origin_stop_name"]) + "_" + str(r["origin_stop_lat"])
        d_ext = str(r["destination_stop_name"]) + "_" + str(r["destination_stop_lat"])
        o_sid = stop_lookup.get(o_ext)
        d_sid = stop_lookup.get(d_ext)
        if not o_sid or not d_sid:
            skipped += 1
            continue
        rows.append((
            safe(r["trip_id"]), None,
            route_lookup.get(str(r["route_id"])),
            o_sid, d_sid, 2,
            None, None,
            safe_int(r.get("duration_minutes")),
            None, None,
            safe_int(r.get("n_stops", 2)) or 2,
            None, True, False,
        ))

    # ── Trains de nuit DE ────────────────────────────────────────────────────
    for _, r in night_de_df.iterrows():
        o_sid = stop_lookup.get(str(r["origin_stop_id"]))
        d_sid = stop_lookup.get(str(r["destination_stop_id"]))
        if not o_sid or not d_sid:
            skipped += 1
            continue
        dep_m = safe_int(r.get("departure_minutes"))
        arr_m = safe_int(r.get("arrival_minutes"))
        rows.append((
            safe(r["trip_id"]),
            safe(r.get("service_id")),
            route_lookup.get(str(r["route_id"])),
            o_sid, d_sid, 3,
            dep_m, arr_m,
            safe_int(r.get("duration_minutes")),
            safe_int(r.get("departure_hh")),
            safe_int(r.get("arrival_hh")),
            safe_int(r.get("n_stops", 2)) or 2,
            None, True, False,
        ))

    inserted = bulk_insert_pg(conn, "trips", COLS, rows, "trips")

    write_etl_log(conn, "load_trips_day",      1, "jour", "SUCCESS",
                  started, total_day, min(inserted, total_day), 0, skipped)
    write_etl_log(conn, "load_trips_night_eu", 2, "nuit", "SUCCESS",
                  started, total_eu, 0, 0, 0)
    write_etl_log(conn, "load_trips_night_de", 3, "nuit", "SUCCESS",
                  started, total_de, 0, 0, 0)

    return inserted, skipped


# ════════════════════════════════════════════════════════════════════════════
# VÉRIFICATION D'ÉQUIVALENCE MySQL ↔ PostgreSQL
# ════════════════════════════════════════════════════════════════════════════

def verify_equivalence(conn) -> dict:
    """
    Calcule les métriques de vérification après import PostgreSQL.
    Ces chiffres sont à comparer aux compteurs retournés par le script MySQL.
    Voir README.md section "Vérification MySQL ↔ PostgreSQL".
    """
    log.info("Vérification d'équivalence ...")
    metrics = {}
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stops")
        metrics["total_stops"] = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM trips")
        metrics["total_trips"] = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM trips WHERE is_night_trip = TRUE")
        metrics["night_trips"] = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM trips WHERE is_night_trip = FALSE")
        metrics["day_trips"] = cur.fetchone()[0]

        cur.execute("SELECT ROUND(AVG(duration_minutes)::numeric, 2) FROM trips WHERE duration_minutes IS NOT NULL")
        metrics["avg_duration_min"] = float(cur.fetchone()[0] or 0)

        cur.execute("SELECT COUNT(*) FROM routes")
        metrics["total_routes"] = cur.fetchone()[0]

    log.info("  Métriques PostgreSQL :")
    for k, v in metrics.items():
        log.info(f"    {k:<25} : {v}")

    return metrics


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    t_start = time.time()
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║   ObRail Europe — ETL Pipeline PostgreSQL  v1.0.0       ║")
    log.info("╚══════════════════════════════════════════════════════════╝")

    conn = get_connection()

    try:
        dfs  = extract()
        data = transform(dfs)

        log.info("═" * 60)
        log.info("ÉTAPE 3 — CHARGEMENT PostgreSQL")
        log.info("═" * 60)

        truncate_tables(conn)
        load_countries(conn)
        load_data_sources(conn)
        load_operators(conn)
        load_stops(conn, data["stops"])
        load_routes(conn, data["routes"])

        t_ok, t_ko = load_trips(
            conn, data["day"], data["night_eu"], data["night_de"],
            data["stop_lookup"], data["route_lookup"])

        metrics = verify_equivalence(conn)

        elapsed = round(time.time() - t_start, 2)
        log.info("═" * 60)
        log.info("RÉSUMÉ FINAL")
        log.info("═" * 60)
        log.info(f"  Pays             : {len(COUNTRIES)}")
        log.info(f"  Sources          : {len(DATA_SOURCES)}")
        log.info(f"  Opérateurs       : {len(OPERATORS)}")
        log.info(f"  Gares (DB)       : {metrics['total_stops']}")
        log.info(f"  Routes (DB)      : {metrics['total_routes']}")
        log.info(f"  Trips total (DB) : {metrics['total_trips']}")
        log.info(f"    dont nuit      : {metrics['night_trips']}")
        log.info(f"    dont jour      : {metrics['day_trips']}")
        log.info(f"  Durée moy.       : {metrics['avg_duration_min']} min")
        log.info(f"  Ignorés (FK)     : {t_ko}")
        log.info(f"  Durée totale     : {elapsed}s")
        log.info("ETL PostgreSQL terminé avec succès.")

    except Exception as e:
        log.error(f"Erreur fatale : {e}", exc_info=True)
        conn.rollback()
        sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
