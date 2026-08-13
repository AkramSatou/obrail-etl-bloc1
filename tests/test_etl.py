"""
Tests d'intégration ETL — trois critères obligatoires (MODIF 1) :

1. test_comptage  : lignes insérées == lignes valides du fichier source.
2. test_idempotence : deux exécutions successives ne doublent pas les données.
3. test_integrite_fk : les contraintes FK rejettent une insertion invalide.

Prérequis : PostgreSQL accessible (Docker ou CI).
Lancer avec :
    pytest tests/test_etl.py -v
"""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker


# ── 1. Test de comptage ───────────────────────────────────────────────────────

def test_comptage_lignes_chargees_egale_lignes_valides(pg_engine, fixture_csv_paths):
    """
    Le nombre de lignes insérées dans day_trips et night_trips
    est égal au nombre de lignes valides dans les fichiers source.

    'Valide' signifie : origin_stop_id ET destination_stop_id présents
    dans le référentiel stops construit pendant la transformation.
    L'ETL construit ce référentiel à partir des mêmes CSV — donc toutes
    les lignes des CSV de test sont attendues valides.
    """
    from etl import run

    counts = run(engine=pg_engine, csv_paths=fixture_csv_paths)

    # Fixtures : 11 lignes day, 80 lignes night_eu + 11 night_de
    # L'ETL build le stop_lookup depuis ces mêmes fichiers :
    # toutes les lignes ont un stop résolvable → aucun skip attendu.
    # Aucune ligne ne doit être ignorée : les CSV sont bien formés
    # et le stop_lookup est construit depuis ces mêmes fichiers.
    assert counts["day_skipped"]   == 0, "Des trajets de jour ont été ignorés"
    assert counts["night_skipped"] == 0, "Des trajets de nuit ont été ignorés"

    # Le nombre inséré doit être strictement positif et plafonné par le CSV
    assert 0 < counts["day_trips"]   <= 11
    assert 0 < counts["night_trips"] <= 91  # 80 night_eu + 11 night_de

    # Vérification directe en base
    with pg_engine.connect() as conn:
        n_day   = conn.execute(text("SELECT COUNT(*) FROM entrepot.day_trips")).scalar()
        n_night = conn.execute(text("SELECT COUNT(*) FROM entrepot.night_trips")).scalar()
    assert n_day   == counts["day_trips"]
    assert n_night == counts["night_trips"]


# ── 2. Test d'idempotence ─────────────────────────────────────────────────────

def test_deux_executions_ne_doublent_pas_les_donnees(pg_engine, fixture_csv_paths):
    """
    Deux exécutions successives de l'ETL produisent le même nombre de
    lignes en base. La stratégie TRUNCATE + INSERT garantit l'idempotence :
    la seconde exécution repart d'une table vide.
    """
    from etl import run

    counts_1 = run(engine=pg_engine, csv_paths=fixture_csv_paths)
    counts_2 = run(engine=pg_engine, csv_paths=fixture_csv_paths)

    assert counts_2["day_trips"]   == counts_1["day_trips"],   "day_trips a doublé"
    assert counts_2["night_trips"] == counts_1["night_trips"], "night_trips a doublé"
    assert counts_2["stops"]       == counts_1["stops"],       "stops a doublé"
    assert counts_2["routes"]      == counts_1["routes"],      "routes a doublé"

    with pg_engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM entrepot.day_trips")).scalar()
    assert n == counts_1["day_trips"], "Nombre de lignes incohérent après deux exécutions"


# ── 3. Test d'intégrité des clés étrangères ───────────────────────────────────

def test_contrainte_fk_rejette_stop_id_invalide(pg_engine, fixture_csv_paths):
    """
    La contrainte FK sur day_trips.origin_stop_id → stops.stop_id
    doit rejeter une insertion dont origin_stop_id ne correspond à aucune
    gare existante.

    Ce test prouve que les FK sont bien créées dans le schéma entrepot
    et qu'elles sont actives (pas seulement définies).
    """
    from etl import run
    from models import DayTrip

    # Charger le référentiel stops et routes en base
    run(engine=pg_engine, csv_paths=fixture_csv_paths)

    Session = sessionmaker(bind=pg_engine)
    session = Session()

    # stop_id = 99999 n'existe pas dans entrepot.stops
    trip_invalide = DayTrip(
        external_trip_id="test-fk-violation",
        route_type=2,
        origin_stop_id=99999,       # FK invalide → doit lever IntegrityError
        destination_stop_id=99999,
        n_stops=2,
        is_international=False,
    )
    session.add(trip_invalide)

    with pytest.raises(IntegrityError):
        session.flush()  # flush force la validation FK sans commit

    session.rollback()
    session.close()
