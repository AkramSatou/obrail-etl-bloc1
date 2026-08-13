"""
Extracteur 2 — Scraping web (BeautifulSoup)

Cible  : https://en.wikipedia.org/wiki/List_of_European_night_trains
Licence: CC BY-SA 4.0 (Wikimedia Foundation)

Vérification robots.txt :
  URL robots : https://en.wikipedia.org/robots.txt
  Règles pour User-agent * :
    - Disallow: /w/           (API MediaWiki)
    - Disallow: /wiki/Special:
    - /wiki/List_of_...  → AUTORISE
  Politique de scraping Wikimedia (https://www.mediawiki.org/wiki/API:Etiquette) :
    - Usage modéré, User-Agent identifié, pas de session authentifiée.
  Conclusion : page autorisée au scraping.
"""

from __future__ import annotations

import logging
import urllib.robotparser
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

ROBOTS_URL = "https://en.wikipedia.org/robots.txt"
TARGET_URL = "https://en.wikipedia.org/wiki/List_of_European_night_trains"
USER_AGENT = "ObRail-ETL/1.0 (EPSI formation IA ; usage pedagogique non commercial)"
TIMEOUT_S = 15


def _robots_allows(robots_url: str, page_url: str, agent: str = "*") -> bool:
    """Interroge robots.txt et retourne True si l'URL cible est autorisée."""
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
        return rp.can_fetch(agent, page_url)
    except Exception as exc:
        log.warning("[Scraping] Impossible de lire robots.txt : %s", exc)
        return False


def extract_scraping(
    url: str = TARGET_URL,
    timeout: int = TIMEOUT_S,
) -> pd.DataFrame:
    """
    Extrait la liste des trains de nuit européens depuis Wikipedia.

    Vérifie robots.txt avant toute requête.
    Gère : page inaccessible, balise absente, tableau malformé.
    Retourne DataFrame vide si la source est indisponible.
    """
    log.info("[Scraping] Verification robots.txt %s ...", ROBOTS_URL)
    if not _robots_allows(ROBOTS_URL, url):
        log.error("[Scraping] robots.txt interdit l'acces a %s — extraction abandonnee.", url)
        return pd.DataFrame(columns=["train_name", "operator", "countries", "source_url"])

    log.info("[Scraping] robots.txt autorise. Recuperation de %s ...", url)
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("[Scraping] Echec requete HTTP : %s — DataFrame vide retourne.", exc)
        return pd.DataFrame(columns=["train_name", "operator", "countries", "source_url"])

    df = _parse_night_trains(resp.text, url)
    log.info("[Scraping] %d lignes extraites.", len(df))
    return df


def _parse_night_trains(html: str, source_url: str) -> pd.DataFrame:
    """
    Parse les tableaux HTML de la page Wikipedia sur les trains de nuit.

    Stratégie dégradée : si aucun tableau structuré n'est trouvé,
    extrait les listes (ul/li) contenant un nom de train reconnaissable.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []

    # Stratégie 1 : recherche dans les wikitables (tableaux Wikipedia classiques)
    tables = soup.find_all("table", class_="wikitable")
    for table in tables:
        thead = table.find("thead") or table.find("tr")
        if not thead:
            continue
        headers = [th.get_text(strip=True).lower() for th in thead.find_all(["th", "td"])]
        # On cherche une colonne "name" ou "train" et une colonne "operator"
        name_idx = _find_col(headers, ["name", "train", "service", "denomination"])
        op_idx = _find_col(headers, ["operator", "company", "exploitant"])
        country_idx = _find_col(headers, ["country", "countries", "route", "pays"])

        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if not cells:
                continue
            rows.append({
                "train_name": cells[name_idx] if name_idx < len(cells) else cells[0],
                "operator": cells[op_idx] if op_idx is not None and op_idx < len(cells) else "",
                "countries": cells[country_idx] if country_idx is not None and country_idx < len(cells) else "",
                "source_url": source_url,
            })

    if rows:
        return pd.DataFrame(rows).drop_duplicates(subset=["train_name"])

    # Stratégie 2 dégradée : listes à puces (aucun tableau trouvé)
    log.warning("[Scraping] Aucun wikitable trouve — mode degrade (listes ul/li).")
    for li in soup.select("ul > li"):
        text = li.get_text(" ", strip=True)
        if len(text) > 5:
            rows.append({
                "train_name": text[:120],
                "operator": "",
                "countries": "",
                "source_url": source_url,
            })

    if not rows:
        log.warning("[Scraping] Aucun contenu extrait de la page.")

    return pd.DataFrame(
        rows or [],
        columns=["train_name", "operator", "countries", "source_url"],
    )


def _find_col(headers: list[str], candidates: list[str]) -> Optional[int]:
    for cand in candidates:
        for i, h in enumerate(headers):
            if cand in h:
                return i
    return None
