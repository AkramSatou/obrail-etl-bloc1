"""
Tests d'intégration — 3 nouvelles tables (critères C3 / C4) :
  eurostat_rail_passengers, wikipedia_named_trains, spark_route_aggregations

Pour chaque table :
  1. test_comptage   : lignes insérées == lignes valides dans le CSV fixture
  2. test_idempotence: deux exécutions successives ne doublent pas les données
  3. test_fk         : la contrainte FK sur source_id rejette un id invalide

Pattern identique à tests/test_etl.py.
Prérequis : PostgreSQL accessible (Docker ou CI).
"""
import pathlib

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def _run_etl(engine, fixture_csv_paths):
    """Lance l'ETL principal pour peupler les référentiels (countries, data_sources…)."""
    from etl import run
    run(engine=engine, csv_paths=fixture_csv_paths)


# ═══════════════════════════════════════════════════════════════════════════════
#  Table 1 — eurostat_rail_passengers
# ═══════════════════════════════════════════════════════════════════════════════

class TestEurostatImport:

    def test_comptage_lignes_chargees(self, pg_engine, fixture_csv_paths):
        """
        Le nombre de lignes insérées dans eurostat_rail_passengers
        correspond exactement aux lignes valides du CSV fixture (5 lignes).
        """
        _run_etl(pg_engine, fixture_csv_paths)
        from importers.eurostat_importer import import_eurostat

        n = import_eurostat(engine=pg_engine, csv_path=FIXTURES / "eurostat_sample.csv")
        assert n > 0, "Aucune ligne insérée dans eurostat_rail_passengers"

        with pg_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM entrepot.eurostat_rail_passengers")
            ).scalar()
        assert count == n, f"COUNT(*) en base ({count}) != lignes insérées ({n})"

    def test_idempotence_double_execution(self, pg_engine, fixture_csv_paths):
        """
        Deux exécutions successives de import_eurostat produisent le même
        nombre de lignes — TRUNCATE + INSERT garantit l'idempotence.
        """
        _run_etl(pg_engine, fixture_csv_paths)
        from importers.eurostat_importer import import_eurostat

        n1 = import_eurostat(engine=pg_engine, csv_path=FIXTURES / "eurostat_sample.csv")
        n2 = import_eurostat(engine=pg_engine, csv_path=FIXTURES / "eurostat_sample.csv")
        assert n1 == n2, f"eurostat_rail_passengers a doublé : {n1} → {n2}"

        with pg_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM entrepot.eurostat_rail_passengers")
            ).scalar()
        assert count == n1, "Nombre incohérent après deux exécutions"

    def test_contrainte_fk_source_id_invalide(self, pg_engine, fixture_csv_paths):
        """
        La contrainte FK eurostat_rail_passengers.source_id → data_sources.source_id
        doit rejeter une insertion avec un source_id inexistant.
        """
        _run_etl(pg_engine, fixture_csv_paths)
        from models import EurostatRailPassenger

        session = sessionmaker(bind=pg_engine)()
        session.add(EurostatRailPassenger(
            country_code="XX",
            period="2023-Q1",
            passengers_k=1000.0,
            source_id=99999,
        ))
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()
        session.close()


# ═══════════════════════════════════════════════════════════════════════════════
#  Table 2 — wikipedia_named_trains
# ═══════════════════════════════════════════════════════════════════════════════

class TestWikipediaImport:

    def test_comptage_lignes_chargees(self, pg_engine, fixture_csv_paths):
        """
        Le nombre de lignes insérées dans wikipedia_named_trains
        correspond aux lignes valides du CSV fixture (3 trains).
        """
        _run_etl(pg_engine, fixture_csv_paths)
        from importers.wikipedia_importer import import_wikipedia

        n = import_wikipedia(engine=pg_engine, csv_path=FIXTURES / "wikipedia_sample.csv")
        assert n > 0, "Aucune ligne insérée dans wikipedia_named_trains"

        with pg_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM entrepot.wikipedia_named_trains")
            ).scalar()
        assert count == n, f"COUNT(*) en base ({count}) != lignes insérées ({n})"

    def test_idempotence_double_execution(self, pg_engine, fixture_csv_paths):
        """
        Deux exécutions successives de import_wikipedia produisent le même nombre
        de lignes — TRUNCATE + INSERT garantit l'idempotence.
        """
        _run_etl(pg_engine, fixture_csv_paths)
        from importers.wikipedia_importer import import_wikipedia

        n1 = import_wikipedia(engine=pg_engine, csv_path=FIXTURES / "wikipedia_sample.csv")
        n2 = import_wikipedia(engine=pg_engine, csv_path=FIXTURES / "wikipedia_sample.csv")
        assert n1 == n2, f"wikipedia_named_trains a doublé : {n1} → {n2}"

        with pg_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM entrepot.wikipedia_named_trains")
            ).scalar()
        assert count == n1, "Nombre incohérent après deux exécutions"

    def test_contrainte_fk_source_id_invalide(self, pg_engine, fixture_csv_paths):
        """
        La contrainte FK wikipedia_named_trains.source_id → data_sources.source_id
        doit rejeter une insertion avec un source_id inexistant.
        """
        _run_etl(pg_engine, fixture_csv_paths)
        from models import WikipediaNamedTrain

        session = sessionmaker(bind=pg_engine)()
        session.add(WikipediaNamedTrain(
            train_name="Train FK Test",
            operator="Test",
            source_id=99999,
        ))
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()
        session.close()


# ═══════════════════════════════════════════════════════════════════════════════
#  Table 3 — spark_route_aggregations
# ═══════════════════════════════════════════════════════════════════════════════

class TestSparkImport:

    def test_comptage_lignes_chargees(self, pg_engine, fixture_csv_paths):
        """
        Le nombre de lignes insérées dans spark_route_aggregations
        correspond aux lignes valides du CSV fixture (5 agrégats).
        """
        _run_etl(pg_engine, fixture_csv_paths)
        from importers.spark_importer import import_spark

        n = import_spark(engine=pg_engine, csv_path=FIXTURES / "spark_agg_sample.csv")
        assert n > 0, "Aucune ligne insérée dans spark_route_aggregations"

        with pg_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM entrepot.spark_route_aggregations")
            ).scalar()
        assert count == n, f"COUNT(*) en base ({count}) != lignes insérées ({n})"

    def test_idempotence_double_execution(self, pg_engine, fixture_csv_paths):
        """
        Deux exécutions successives de import_spark produisent le même nombre
        de lignes — TRUNCATE + INSERT garantit l'idempotence.
        """
        _run_etl(pg_engine, fixture_csv_paths)
        from importers.spark_importer import import_spark

        n1 = import_spark(engine=pg_engine, csv_path=FIXTURES / "spark_agg_sample.csv")
        n2 = import_spark(engine=pg_engine, csv_path=FIXTURES / "spark_agg_sample.csv")
        assert n1 == n2, f"spark_route_aggregations a doublé : {n1} → {n2}"

        with pg_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM entrepot.spark_route_aggregations")
            ).scalar()
        assert count == n1, "Nombre incohérent après deux exécutions"

    def test_contrainte_fk_source_id_invalide(self, pg_engine, fixture_csv_paths):
        """
        La contrainte FK spark_route_aggregations.source_id → data_sources.source_id
        doit rejeter une insertion avec un source_id inexistant.
        """
        _run_etl(pg_engine, fixture_csv_paths)
        from models import SparkRouteAggregation

        session = sessionmaker(bind=pg_engine)()
        session.add(SparkRouteAggregation(
            country="XX",
            route_type=2,
            nb_trajets=100,
            source_id=99999,
        ))
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()
        session.close()
