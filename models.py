"""
Modèles SQLAlchemy — schéma entrepôt ObRail (Bloc 1).

DB_TARGET=postgresql  → tables dans le schéma `entrepot` (ou ENTREPOT_SCHEMA).
DB_TARGET=mysql       → tables dans la base courante, sans préfixe de schéma.
"""

import os

from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Integer, Numeric,
    SmallInteger, String, Text, UniqueConstraint, func,
)
from sqlalchemy.orm import DeclarativeBase

DB_TARGET: str = os.getenv("DB_TARGET", "postgresql")

# Schéma cible — surchargeble par ENTREPOT_SCHEMA pour les tests.
ENTREPOT_SCHEMA: str | None = (
    os.getenv("ENTREPOT_SCHEMA", "entrepot")
    if DB_TARGET == "postgresql"
    else None
)


class Base(DeclarativeBase):
    pass


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ta(*constraints):
    """Renvoie __table_args__ avec schema, sous forme dict ou tuple+dict."""
    d = {"schema": ENTREPOT_SCHEMA}
    return (*constraints, d) if constraints else d


def _fk(table: str, col: str) -> str:
    """Référence FK qualifiée du schéma si PostgreSQL."""
    return f"{ENTREPOT_SCHEMA}.{table}.{col}" if ENTREPOT_SCHEMA else f"{table}.{col}"


# ── Référentiels ──────────────────────────────────────────────────────────────

class Country(Base):
    __tablename__  = "countries"
    __table_args__ = _ta()

    country_code = Column(String(2),   primary_key=True)
    country_name = Column(String(100), nullable=False)
    timezone     = Column(String(50),  default="Europe/Paris")
    created_at   = Column(DateTime,    server_default=func.now())
    updated_at   = Column(DateTime,    server_default=func.now())


class DataSource(Base):
    __tablename__  = "data_sources"
    __table_args__ = _ta(UniqueConstraint("source_name", name="uq_source_name"))

    source_id        = Column(Integer,     primary_key=True, autoincrement=True)
    source_name      = Column(String(100), nullable=False)
    source_type      = Column(String(50),  nullable=False)
    source_url       = Column(String(500))
    description      = Column(Text)
    license_type     = Column(String(100))
    last_import_date = Column(DateTime)
    is_active        = Column(Boolean,     default=True)
    created_at       = Column(DateTime,    server_default=func.now())
    updated_at       = Column(DateTime,    server_default=func.now())


class Operator(Base):
    __tablename__  = "operators"
    __table_args__ = _ta(UniqueConstraint("operator_code", name="uq_operator_code"))

    operator_id             = Column(Integer,     primary_key=True, autoincrement=True)
    operator_code           = Column(String(20),  nullable=False)
    operator_name           = Column(String(200), nullable=False)
    country_code            = Column(String(2),   ForeignKey(_fk("countries", "country_code")))
    website                 = Column(String(300))
    is_night_train_operator = Column(Boolean, default=False)
    is_day_train_operator   = Column(Boolean, default=True)
    created_at              = Column(DateTime, server_default=func.now())
    updated_at              = Column(DateTime, server_default=func.now())


class Stop(Base):
    __tablename__  = "stops"
    __table_args__ = _ta(
        UniqueConstraint("external_stop_id", "source_id", name="uq_stop_external"),
    )

    stop_id              = Column(Integer,        primary_key=True, autoincrement=False)
    external_stop_id     = Column(String(100),    nullable=False)
    stop_name            = Column(String(300),    nullable=False)
    stop_name_normalized = Column(String(300))
    latitude             = Column(Numeric(10, 6), nullable=False)
    longitude            = Column(Numeric(10, 6), nullable=False)
    country_code         = Column(String(2),      ForeignKey(_fk("countries", "country_code")))
    city                 = Column(String(200))
    is_major_station     = Column(Boolean,         default=False)
    source_id            = Column(Integer,         ForeignKey(_fk("data_sources", "source_id")))
    created_at           = Column(DateTime,        server_default=func.now())
    updated_at           = Column(DateTime,        server_default=func.now())


class Route(Base):
    __tablename__  = "routes"
    __table_args__ = _ta(
        UniqueConstraint("external_route_id", "source_id", name="uq_route_external"),
    )

    route_id          = Column(Integer,     primary_key=True, autoincrement=False)
    external_route_id = Column(String(200), nullable=False)
    route_short_name  = Column(String(100))
    route_long_name   = Column(String(500))
    route_type        = Column(Integer,     nullable=False, default=2)
    service_type      = Column(String(10),  nullable=False)
    operator_id       = Column(Integer,     ForeignKey(_fk("operators", "operator_id")))
    source_id         = Column(Integer,     ForeignKey(_fk("data_sources", "source_id")))
    created_at        = Column(DateTime,    server_default=func.now())
    updated_at        = Column(DateTime,    server_default=func.now())


# ── Trajets ───────────────────────────────────────────────────────────────────
# day_trips et night_trips ont les mêmes colonnes ; on les fabrique via type().

def _trip_cols():
    """Colonnes communes aux deux tables de trajets."""
    return {
        "external_trip_id":     Column(String(300), nullable=False),
        "external_service_id":  Column(String(100)),
        "route_id":             Column(Integer, ForeignKey(_fk("routes", "route_id"))),
        "route_short_name":     Column(String(100)),
        "route_long_name":      Column(String(500)),
        "route_type":           Column(Integer, default=2),
        "origin_stop_id":       Column(Integer, ForeignKey(_fk("stops", "stop_id")), nullable=False),
        "destination_stop_id":  Column(Integer, ForeignKey(_fk("stops", "stop_id")), nullable=False),
        "origin_stop_name":     Column(String(300)),
        "destination_stop_name":Column(String(300)),
        "origin_stop_lat":      Column(Numeric(10, 6)),
        "origin_stop_lon":      Column(Numeric(10, 6)),
        "destination_stop_lat": Column(Numeric(10, 6)),
        "destination_stop_lon": Column(Numeric(10, 6)),
        "country":              Column(String(2)),
        "source_id":            Column(Integer, ForeignKey(_fk("data_sources", "source_id"))),
        "departure_minutes":    Column(Integer),
        "arrival_minutes":      Column(Integer),
        "departure_hour":       Column(SmallInteger),
        "arrival_hour":         Column(SmallInteger),
        "duration_minutes":     Column(Integer),
        "n_stops":              Column(Integer, default=2),
        "distance_km":          Column(Numeric(10, 2)),
        "is_international":     Column(Boolean, default=False),
        "created_at":           Column(DateTime, server_default=func.now()),
        "updated_at":           Column(DateTime, server_default=func.now()),
    }


DayTrip = type("DayTrip", (Base,), {
    "__tablename__": "day_trips",
    "__table_args__": _ta(
        UniqueConstraint("external_trip_id", "source_id", name="uq_day_trip_external"),
    ),
    "trip_id": Column(Integer, primary_key=True, autoincrement=True),
    **_trip_cols(),
})

NightTrip = type("NightTrip", (Base,), {
    "__tablename__": "night_trips",
    "__table_args__": _ta(
        UniqueConstraint("external_trip_id", "source_id", name="uq_night_trip_external"),
    ),
    "trip_id": Column(Integer, primary_key=True, autoincrement=True),
    **_trip_cols(),
})


# ── Journal ETL ───────────────────────────────────────────────────────────────

class EtlLog(Base):
    __tablename__  = "etl_logs"
    __table_args__ = _ta()

    log_id            = Column(Integer,     primary_key=True, autoincrement=True)
    job_name          = Column(String(100), nullable=False)
    source_id         = Column(Integer,     ForeignKey(_fk("data_sources", "source_id")))
    status            = Column(String(20),  nullable=False)
    started_at        = Column(DateTime,    nullable=False)
    ended_at          = Column(DateTime)
    records_processed = Column(Integer,     default=0)
    records_inserted  = Column(Integer,     default=0)
    records_updated   = Column(Integer,     default=0)
    records_rejected  = Column(Integer,     default=0)
    error_message     = Column(Text)


# ── Sources additionnelles (C3 / C4) ─────────────────────────────────────────

class EurostatRailPassenger(Base):
    """Passagers ferroviaires trimestriels par pays — API Eurostat RAIL_PA_QUARTAL."""
    __tablename__  = "eurostat_rail_passengers"
    __table_args__ = _ta(
        UniqueConstraint("country_code", "period", name="uq_eurostat_country_period"),
    )

    id           = Column(Integer,      primary_key=True, autoincrement=True)
    country_code = Column(String(10),   nullable=False)
    period       = Column(String(10),   nullable=False)
    passengers_k = Column(Numeric(12, 1))
    source_id    = Column(Integer,      ForeignKey(_fk("data_sources", "source_id")))
    created_at   = Column(DateTime,     server_default=func.now())


class WikipediaNamedTrain(Base):
    """Liste des trains passagers nommés d'Europe — scraping Wikipedia."""
    __tablename__  = "wikipedia_named_trains"
    __table_args__ = _ta(
        UniqueConstraint("train_name", name="uq_wikipedia_train_name"),
    )

    id         = Column(Integer,     primary_key=True, autoincrement=True)
    train_name = Column(String(300), nullable=False)
    operator   = Column(String(300))
    countries  = Column(String(200))
    source_url = Column(String(500))
    source_id  = Column(Integer,     ForeignKey(_fk("data_sources", "source_id")))
    created_at = Column(DateTime,    server_default=func.now())


class SparkRouteAggregation(Base):
    """Agrégations PySpark — trajets par pays et type de route."""
    __tablename__  = "spark_route_aggregations"
    __table_args__ = _ta(
        UniqueConstraint("country", "route_type", name="uq_spark_country_route_type"),
    )

    id            = Column(Integer,      primary_key=True, autoincrement=True)
    country       = Column(String(2),    nullable=False)
    country_name  = Column(String(100))
    route_type    = Column(Integer,      nullable=False)
    nb_trajets    = Column(Integer)
    duree_moy_min = Column(Numeric(8, 1))
    arrets_moy    = Column(Numeric(8, 1))
    source_id     = Column(Integer,      ForeignKey(_fk("data_sources", "source_id")))
    created_at    = Column(DateTime,     server_default=func.now())
