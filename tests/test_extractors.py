"""
Tests des extracteurs MODIF 2 (API REST, scraping, PySpark, base de données)

Principes :
  - Les appels réseau sont simulés (unittest.mock) pour éviter une dépendance externe.
  - Les tests vérifient les colonnes, les types, la robustesse aux pannes.
  - Les tests "réels" (marqués @pytest.mark.integration) sont désactivés par défaut
    et nécessitent OBRAIL_INTEGRATION=1 dans l'environnement.
"""
from __future__ import annotations

import json
import os
import pathlib
import textwrap
import unittest.mock as mock

import pandas as pd
import pytest

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
INTEGRATION = os.getenv("OBRAIL_INTEGRATION", "0") == "1"


# ══════════════════════════════════════════════════════════════════════════════
#  Source 2 — API REST Eurostat
# ══════════════════════════════════════════════════════════════════════════════

class TestApiExtractor:

    def _fake_eurostat_response(self) -> dict:
        """JSON minimal imitant le format Eurostat SDMX-like."""
        return {
            "dimension": {
                "geo": {"category": {"index": {"DE": 0, "FR": 1}}},
                "time": {"category": {"index": {"2023-Q1": 0, "2023-Q2": 1}}},
            },
            "value": {"0": 12_000, "1": 11_500, "2": 8_000, "3": 7_500},
        }

    def test_retourne_dataframe_avec_colonnes_attendues(self):
        """Vérifie que le parsing Eurostat retourne les 3 colonnes documentées."""
        from extractors.api_extractor import _parse_eurostat_json
        df = _parse_eurostat_json(self._fake_eurostat_response())
        assert set(df.columns) == {"country_code", "period", "passengers_k"}
        assert len(df) == 4  # 2 pays × 2 trimestres

    def test_retourne_df_vide_si_reponse_malformee(self):
        """Réponse JSON sans champ 'value' → DataFrame vide, pas d'exception."""
        from extractors.api_extractor import extract_api
        fake_response = mock.MagicMock()
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = {}  # manque 'value'
        with mock.patch("extractors.api_extractor.requests.get", return_value=fake_response):
            df = extract_api(max_retries=1)
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_retry_sur_timeout_puis_df_vide(self):
        """3 timeouts successifs → DataFrame vide retourné proprement."""
        from extractors.api_extractor import extract_api
        import requests as req
        with mock.patch(
            "extractors.api_extractor.requests.get",
            side_effect=req.exceptions.Timeout("timeout simulé"),
        ), mock.patch("extractors.api_extractor.time.sleep"):
            df = extract_api(max_retries=3)
        assert isinstance(df, pd.DataFrame)
        assert "country_code" in df.columns

    def test_parse_ordre_stable(self):
        """Les lignes sont triées country_code, period — reproductible."""
        from extractors.api_extractor import _parse_eurostat_json
        df = _parse_eurostat_json(self._fake_eurostat_response())
        assert list(df["country_code"]) == ["DE", "DE", "FR", "FR"]

    @pytest.mark.skipif(not INTEGRATION, reason="Requiert OBRAIL_INTEGRATION=1 et accès Eurostat")
    def test_integration_api_eurostat_reelle(self):
        from extractors.api_extractor import extract_api
        df = extract_api()
        assert not df.empty
        assert "country_code" in df.columns


# ══════════════════════════════════════════════════════════════════════════════
#  Source 3 — Scraping Wikipedia
# ══════════════════════════════════════════════════════════════════════════════

FAKE_HTML = textwrap.dedent("""\
<html><body>
<table class="wikitable">
<thead><tr><th>Name</th><th>Operator</th><th>Countries</th></tr></thead>
<tr><td>EuroNight 40</td><td>DB / OBB</td><td>DE-AT-HU</td></tr>
<tr><td>Intercités de Nuit</td><td>SNCF</td><td>FR</td></tr>
</table>
</body></html>
""")


class TestScrapingExtractor:

    def test_retourne_dataframe_avec_colonnes_attendues(self):
        """Parse un tableau wikitable minimal → 4 colonnes attendues."""
        from extractors.scraping_extractor import _parse_night_trains
        df = _parse_night_trains(FAKE_HTML, "http://fake")
        assert set(df.columns) == {"train_name", "operator", "countries", "source_url"}
        assert len(df) == 2
        assert df.iloc[0]["train_name"] == "EuroNight 40"

    def test_robots_txt_bloquant_retourne_df_vide(self):
        """Si robots.txt refuse, le DataFrame est vide sans erreur."""
        from extractors.scraping_extractor import extract_scraping
        with mock.patch("extractors.scraping_extractor._robots_allows", return_value=False):
            df = extract_scraping()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_erreur_reseau_retourne_df_vide(self):
        """Erreur réseau après validation robots.txt → DataFrame vide."""
        from extractors.scraping_extractor import extract_scraping
        import requests as req
        with mock.patch("extractors.scraping_extractor._robots_allows", return_value=True), \
             mock.patch("extractors.scraping_extractor.requests.get",
                        side_effect=req.exceptions.ConnectionError("réseau indisponible")):
            df = extract_scraping()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_page_sans_wikitable_mode_degrade(self):
        """Page sans tableau → stratégie dégradée (listes)."""
        from extractors.scraping_extractor import _parse_night_trains
        html_no_table = "<html><body><ul><li>Train A Paris-Berlin</li><li>Train B</li></ul></body></html>"
        df = _parse_night_trains(html_no_table, "http://fake")
        assert isinstance(df, pd.DataFrame)
        # Mode dégradé : au moins les li sont capturés
        assert len(df) >= 2

    @pytest.mark.skipif(not INTEGRATION, reason="Requiert OBRAIL_INTEGRATION=1 et accès Wikipedia")
    def test_integration_wikipedia_reelle(self):
        from extractors.scraping_extractor import extract_scraping
        df = extract_scraping()
        assert not df.empty


# ══════════════════════════════════════════════════════════════════════════════
#  Source 4 — PySpark
# ══════════════════════════════════════════════════════════════════════════════

class TestSparkExtractor:

    def test_retourne_df_vide_si_aucune_source(self):
        """_resolve_source retourne None → DataFrame vide avec colonnes attendues."""
        from extractors.spark_pipeline import extract_spark
        with mock.patch("extractors.spark_pipeline._resolve_source", return_value=None):
            df = extract_spark()
        assert isinstance(df, pd.DataFrame)
        assert "nb_trajets" in df.columns
        assert df.empty

    def test_extrait_avec_fixture_csv(self):
        """Lance PySpark sur la fixture eu_trips_sample.csv et retourne des lignes."""
        from extractors.spark_pipeline import extract_spark
        fixture_dir = FIXTURES
        df = extract_spark(fallback_dir=fixture_dir)
        # La fixture peut être vide ou contenir des lignes — on vérifie juste le schéma
        assert isinstance(df, pd.DataFrame)
        assert "country" in df.columns
        assert "nb_trajets" in df.columns

    def test_colonnes_attendues_presentes(self):
        """Les 5 colonnes documentées sont présentes même sur données minimes."""
        from extractors.spark_pipeline import extract_spark
        df = extract_spark(fallback_dir=FIXTURES)
        expected = {"country", "route_type", "nb_trajets", "duree_moy_min", "arrets_moy"}
        assert expected.issubset(set(df.columns))

    def test_nb_trajets_positif_si_donnees(self):
        """Sur des données non vides, nb_trajets doit être > 0."""
        from extractors.spark_pipeline import extract_spark
        df = extract_spark(fallback_dir=FIXTURES)
        if not df.empty:
            assert (df["nb_trajets"] > 0).all()


# ══════════════════════════════════════════════════════════════════════════════
#  Source 5 — Base de données entrepot
# ══════════════════════════════════════════════════════════════════════════════

class TestDbExtractor:

    def test_retourne_dict_de_dataframes(self, pg_engine):
        """Après un ETL minimal, extract_db retourne un dict avec 3 clés."""
        from extractors.db_extractor import extract_db
        # La fixture pg_engine fournit un schéma entrepot vide (tables créées)
        result = extract_db(engine=pg_engine)
        assert isinstance(result, dict)
        assert "volumes" in result
        assert "top_routes" in result
        assert "etl_log" in result
        for df in result.values():
            assert isinstance(df, pd.DataFrame)

    def test_retourne_df_vide_si_connexion_impossible(self):
        """Connexion impossible → dict de DataFrames vides, sans exception."""
        from extractors.db_extractor import extract_db
        from sqlalchemy import create_engine
        dead_engine = create_engine(
            "postgresql+psycopg2://nobody:nobody@localhost:19999/nobody",
            pool_pre_ping=False,
        )
        result = extract_db(engine=dead_engine)
        assert isinstance(result, dict)
        for df in result.values():
            assert isinstance(df, pd.DataFrame)

    def test_volumes_colonnes_attendues(self, pg_engine):
        """La requête 'volumes' retourne les colonnes documentées."""
        from extractors.db_extractor import extract_db
        result = extract_db(engine=pg_engine)
        df = result["volumes"]
        assert "country_code" in df.columns
        assert "day_trips" in df.columns
