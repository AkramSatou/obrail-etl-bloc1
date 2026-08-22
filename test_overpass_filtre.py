"""
Confirmation finale — requete Overpass filtree (metro/tram exclus)

Reprend exactement la requete de extractors/osm_extractor.py (celle qui sera
reellement utilisee par l'ETL), avec le filtre station!=subway / station!=light_rail
qui manquait dans le premier test (test_overpass.py, 300 noeuds mais melanges
avec des stations de metro comme Chatelet ou Tuileries).

Usage :
    cd "chemin vers obrail-etl-bloc1"
    python test_overpass_filtre.py
"""
import json
import sys

import requests

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
USER_AGENT = "ObRail-ETL/1.0 (EPSI RNCP37827 ; usage pedagogique non commercial)"

# bbox = (sud, ouest, nord, est) - Paris intra-muros
BBOX = (48.80, 2.25, 48.90, 2.40)

QUERY = """
[out:json][timeout:25];
node["railway"="station"]["station"!="subway"]["station"!="light_rail"]({s},{w},{n},{e});
out body;
""".format(s=BBOX[0], w=BBOX[1], n=BBOX[2], e=BBOX[3])

# Stations de metro qui apparaissaient dans le test precedent SANS le filtre —
# si elles reapparaissent ici, le filtre ne fonctionne pas.
STATIONS_METRO_CONNUES = {"Châtelet", "Tuileries"}


def main() -> int:
    print("Envoi de la requete a Overpass API...")
    print(QUERY)

    try:
        resp = requests.post(
            OVERPASS_URL,
            data={"data": QUERY},
            headers={"User-Agent": USER_AGENT},
            timeout=30,
        )
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        print(f"ECHEC — requete impossible : {exc}")
        return 1

    raw = resp.json()
    elements = raw.get("elements", [])

    with open("test_overpass_filtre_result.json", "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)

    noms = sorted({el.get("tags", {}).get("name", "(sans nom)") for el in elements})

    print(f"\n{len(elements)} gares retournees (filtre metro/tram applique).")
    print("Resultat complet sauvegarde dans test_overpass_filtre_result.json\n")
    print("Liste des gares :")
    for nom in noms:
        print(f"  - {nom}")

    metro_trouves = STATIONS_METRO_CONNUES & set(noms)

    print()
    if metro_trouves:
        print(f"ECHEC — stations de metro encore presentes malgre le filtre : {sorted(metro_trouves)}")
        return 1
    elif len(elements) == 0:
        print("ATTENTION — 0 gare retournee, verifier la requete ou la connexion.")
        return 1
    else:
        print(f"OK — {len(elements)} gares, aucune station de metro connue (Chatelet, Tuileries) dans les resultats.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
