"""
Extracteur 5 — Base de données OpenStreetMap (Overpass API)

Overpass API n'est pas une simple API REST : c'est, selon sa propre
documentation, "un moteur de base de données pour interroger les données
OpenStreetMap" (https://github.com/drolbr/Overpass-API), interrogé avec un
langage de requête dédié, Overpass QL, sur le modèle de SQL pour une base
relationnelle. C'est ce qui en fait une vraie 5e source pour le critère C1,
distincte d'un service web (déjà couvert par l'API Eurostat) : une base de
données externe et indépendante du projet, jamais alimentée par ce pipeline.

Zone interrogée : gares ferroviaires de Paris intra-muros uniquement, pas
toute l'Europe, pour rester léger sur un service public gratuit et partagé
(politique d'usage Overpass : https://dev.overpass-api.de/overpass-doc/en/preface/commons.html).

Filtre station=subway / station=light_rail : le tag railway=station seul
capte aussi bien les gares que les stations de métro et de tramway dans la
convention de balisage OSM. station=subway est le tag obligatoire pour
distinguer une station de métro (https://wiki.openstreetmap.org/wiki/Tag:station=subway).
Testé le 19/08/2026 sans ce filtre : 300 nœuds retournés, mélangeant de
vraies gares (Gare de Lyon, Gare Montparnasse) et des stations de métro
(Châtelet, Tuileries...). Le filtre exclut ces dernières.

Licence des données OpenStreetMap : ODbL (Open Database License).
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd
import requests

log = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
USER_AGENT = "ObRail-ETL/1.0 (EPSI RNCP37827 ; usage pedagogique non commercial)"

# bbox = (sud, ouest, nord, est) — Paris intra-muros
BBOX = (48.80, 2.25, 48.90, 2.40)

QUERY_TEMPLATE = """
[out:json][timeout:25];
node["railway"="station"]["station"!="subway"]["station"!="light_rail"]({s},{w},{n},{e});
out body;
"""

TIMEOUT_S = 30
MAX_RETRIES = 3
BACKOFF_BASE = 2  # secondes — backoff exponentiel


def extract_osm(
    bbox: tuple[float, float, float, float] = BBOX,
    timeout: int = TIMEOUT_S,
    max_retries: int = MAX_RETRIES,
) -> pd.DataFrame:
    """
    Extrait les gares ferroviaires (hors métro/tram) depuis Overpass API.

    Gestion robuste : timeout, statut HTTP non-200, retry avec backoff
    exponentiel, réponse JSON vide ou malformée. Retourne un DataFrame vide
    si la source est injoignable, sans lever d'exception, pour ne pas
    interrompre l'orchestrateur.
    """
    s, w, n, e = bbox
    query = QUERY_TEMPLATE.format(s=s, w=w, n=n, e=e)

    for attempt in range(1, max_retries + 1):
        try:
            log.info("[OSM Overpass] Tentative %d/%d ...", attempt, max_retries)
            resp = requests.post(
                OVERPASS_URL,
                data={"data": query},
                headers={"User-Agent": USER_AGENT},
                timeout=timeout,
            )
            resp.raise_for_status()

            raw: dict[str, Any] = resp.json()
            df = _parse_osm_json(raw)
            log.info("[OSM Overpass] %d gares extraites.", len(df))
            return df

        except requests.exceptions.HTTPError as exc:
            log.warning("[OSM Overpass] Statut HTTP non-200 : %s", exc)
            break  # pas de retry sur 4xx/5xx côté serveur

        except requests.exceptions.Timeout:
            log.warning("[OSM Overpass] Timeout (tentative %d/%d).", attempt, max_retries)

        except requests.exceptions.ConnectionError as exc:
            log.warning("[OSM Overpass] Erreur réseau : %s", exc)

        except (ValueError, KeyError) as exc:
            log.warning("[OSM Overpass] Réponse malformée : %s", exc)
            break

        if attempt < max_retries:
            wait = BACKOFF_BASE ** attempt
            log.info("[OSM Overpass] Attente %ds avant re-essai...", wait)
            time.sleep(wait)

    log.error(
        "[OSM Overpass] Source injoignable après %d tentatives — DataFrame vide retourné.",
        max_retries,
    )
    return pd.DataFrame(columns=["osm_node_id", "station_name", "latitude", "longitude"])


def _parse_osm_json(raw: dict[str, Any]) -> pd.DataFrame:
    """Parse la réponse JSON d'Overpass (liste de nœuds avec tags)."""
    if not raw or "elements" not in raw:
        raise ValueError("Champ 'elements' absent de la réponse Overpass.")

    rows: list[dict] = []
    for el in raw["elements"]:
        if el.get("type") != "node":
            continue
        tags = el.get("tags", {})
        rows.append({
            "osm_node_id": str(el.get("id")),
            "station_name": tags.get("name", ""),
            "latitude": el.get("lat"),
            "longitude": el.get("lon"),
        })

    df = pd.DataFrame(rows, columns=["osm_node_id", "station_name", "latitude", "longitude"])
    return df.drop_duplicates(subset=["osm_node_id"]).reset_index(drop=True)
