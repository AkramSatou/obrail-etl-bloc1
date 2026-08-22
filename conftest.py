"""
Fixtures pytest pour les tests d'intégration ETL ObRail.

Utilise le PostgreSQL Docker (localhost:5432) ou le service CI.
Chaque test reçoit un engine PostgreSQL avec le schéma `entrepot`
recréé à blanc — garantit l'isolation sans base de données dédiée.
"""

import os
import pytest
from pathlib import Path
from sqlalchemy import create_engine, text

# DB_TARGET doit être set AVANT tout import de models.py
os.environ.setdefault("DB_TARGET", "postgresql")

TEST_DB_URL: str = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+psycopg2://obrail:obrail@localhost:5432/obrail",
)

FIXTURE_DIR = Path(__file__).parent / "tests" / "fixtures"


@pytest.fixture(scope="function")
def pg_engine():
    """
    Engine PostgreSQL avec schéma `entrepot` recréé à blanc.
    Teardown : supprime le schéma entier pour ne laisser aucun résidu.
    """
    import models  # import tardif : DB_TARGET déjà positionné

    engine = create_engine(TEST_DB_URL, pool_pre_ping=True)

    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS entrepot CASCADE"))
        conn.execute(text("CREATE SCHEMA entrepot"))
        conn.commit()

    models.Base.metadata.create_all(bind=engine)

    yield engine

    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS entrepot CASCADE"))
        conn.commit()

    engine.dispose()


@pytest.fixture
def fixture_csv_paths():
    """Chemins vers les petits CSV de test (11 lignes × 3 fichiers)."""
    return {
        "day":      FIXTURE_DIR / "eu_trips_sample.csv",
        "night_eu": FIXTURE_DIR / "eu_trips_night_sample.csv",
        "night_de": FIXTURE_DIR / "de_night_sample.csv",
    }
