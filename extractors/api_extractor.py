"""
Extracteur 1 — API REST Eurostat (open, sans authentification)
Dataset : RAIL_PA_QUARTAL — passagers ferroviaires trimestriels par pays UE
Licence : CC BY 4.0  https://ec.europa.eu/eurostat/web/main/help/copyright-notice
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd
import requests

log = logging.getLogger(__name__)

EUROSTAT_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
    "/RAIL_PA_QUARTAL"
)
DEFAULT_PARAMS: dict[str, str] = {
    "format": "JSON",
    "lang": "EN",
    "sinceTimePeriod": "2020-Q1",
}
TIMEOUT_S = 15
MAX_RETRIES = 3
BACKOFF_BASE = 2  # secondes — backoff exponentiel


def extract_api(
    params: dict[str, str] | None = None,
    timeout: int = TIMEOUT_S,
    max_retries: int = MAX_RETRIES,
) -> pd.DataFrame:
    """
    Extrait les statistiques ferroviaires depuis l'API Eurostat.

    Gestion robuste :
      - délai d'attente (timeout)
      - statut HTTP non-200 (raise_for_status)
      - retry avec backoff exponentiel
      - réponse JSON vide ou malformée

    Retourne un DataFrame vide (colonnes vides) si la source est injoignable,
    sans lever d'exception, afin de ne pas interrompre l'orchestrateur.
    """
    merged = {**DEFAULT_PARAMS, **(params or {})}

    for attempt in range(1, max_retries + 1):
        try:
            log.info("[API Eurostat] Tentative %d/%d ...", attempt, max_retries)
            resp = requests.get(EUROSTAT_URL, params=merged, timeout=timeout)
            resp.raise_for_status()

            raw: dict[str, Any] = resp.json()
            df = _parse_eurostat_json(raw)
            log.info("[API Eurostat] %d lignes extraites.", len(df))
            return df

        except requests.exceptions.HTTPError as exc:
            log.warning("[API Eurostat] Statut HTTP non-200 : %s", exc)
            break  # pas de retry sur 4xx/5xx côté serveur

        except requests.exceptions.Timeout:
            log.warning("[API Eurostat] Timeout (tentative %d/%d).", attempt, max_retries)

        except requests.exceptions.ConnectionError as exc:
            log.warning("[API Eurostat] Erreur réseau : %s", exc)

        except (ValueError, KeyError) as exc:
            log.warning("[API Eurostat] Reponse malformee : %s", exc)
            break

        if attempt < max_retries:
            wait = BACKOFF_BASE ** attempt
            log.info("[API Eurostat] Attente %ds avant re-essai...", wait)
            time.sleep(wait)

    log.error(
        "[API Eurostat] Source injoignable apres %d tentatives — "
        "DataFrame vide retourne.",
        max_retries,
    )
    return pd.DataFrame(columns=["country_code", "period", "passengers_k"])


def _parse_eurostat_json(raw: dict[str, Any]) -> pd.DataFrame:
    """
    Parse le format JSON SDMX-like retourné par Eurostat.

    Structure attendue :
      raw["dimension"]["geo"]["category"]["index"]  → {code: position}
      raw["dimension"]["time"]["category"]["index"] → {period: position}
      raw["value"]                                  → {str(offset): valeur}
      offset = time_pos * n_geo + geo_pos
    """
    if not raw or "value" not in raw:
        raise ValueError("Champ 'value' absent de la reponse JSON Eurostat.")

    dims = raw.get("dimension", {})
    geo_index: dict[str, int] = dims.get("geo", {}).get("category", {}).get("index", {})
    time_index: dict[str, int] = dims.get("time", {}).get("category", {}).get("index", {})
    values: dict[str, float] = raw["value"]

    if not geo_index or not time_index:
        raise ValueError("Dimensions 'geo' ou 'time' manquantes dans la reponse.")

    n_geo = len(geo_index)
    pos_to_geo = {v: k for k, v in geo_index.items()}
    pos_to_time = {v: k for k, v in time_index.items()}

    rows: list[dict] = []
    for offset_str, value in values.items():
        offset = int(offset_str)
        time_pos = offset // n_geo
        geo_pos = offset % n_geo
        rows.append({
            "country_code": pos_to_geo.get(geo_pos, "??"),
            "period": pos_to_time.get(time_pos, "??"),
            "passengers_k": value,
        })

    return pd.DataFrame(rows).sort_values(["country_code", "period"]).reset_index(drop=True)
