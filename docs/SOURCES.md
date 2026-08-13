# Registre des sources de données — ObRail ETL Bloc 1

---

## Source 1 — Fichier CSV (eu_trips.csv)

| Champ | Valeur |
|-------|--------|
| **URL / Origine** | Fichier fourni dans le cadre du MSPR ObRail (données SNCB + DB + SBB agrégées) |
| **Licence** | Usage interne projet pédagogique — données anonymisées |
| **Format** | CSV, 24 colonnes, 173 662 lignes |
| **Fréquence de mise à jour** | Statique (snapshot 2023) |
| **Justification** | Source principale du projet ; alimente le schéma `entrepot` |
| **robots.txt** | N/A (fichier local, pas de scraping) |
| **Module** | `etl.py` |

---

## Source 2 — API REST Eurostat (RAIL_PA_QUARTAL)

| Champ | Valeur |
|-------|--------|
| **URL** | `https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/RAIL_PA_QUARTAL` |
| **Documentation** | https://ec.europa.eu/eurostat/web/api-usage-guide |
| **Licence** | CC BY 4.0 — https://ec.europa.eu/eurostat/web/main/help/copyright-notice |
| **Authentification** | Aucune (API ouverte) |
| **Format** | JSON (format SDMX-like) |
| **Fréquence de mise à jour** | Trimestrielle (publication ~3 mois après fin de trimestre) |
| **Contenu** | Passagers ferroviaires en milliers par pays UE, par trimestre |
| **Justification** | Contexte macroéconomique des flux ferroviaires ; enrichit l'analyse sans données personnelles |
| **Gestion des pannes** | Timeout 15s, 3 retry avec backoff exponentiel (2^n secondes), DataFrame vide si indisponible |
| **robots.txt** | N/A (API REST, pas de scraping HTML) |
| **Module** | `extractors/api_extractor.py` |

### Paramètres utilisés

```
format=JSON
lang=EN
sinceTimePeriod=2020-Q1
```

---

## Source 3 — Scraping Wikipedia (trains de nuit européens)

| Champ | Valeur |
|-------|--------|
| **URL cible** | `https://en.wikipedia.org/wiki/List_of_named_passenger_trains_of_Europe` |
| **Licence** | CC BY-SA 4.0 — https://creativecommons.org/licenses/by-sa/4.0/ |
| **robots.txt** | `https://en.wikipedia.org/robots.txt` |
| **Fréquence de mise à jour** | Mise à jour communautaire (Wikipedia — variable) |
| **Contenu** | 338 trains nommés d'Europe (nom, opérateur, itinéraire) dont les services historiques de nuit CNL, EuroNight, NightJet, etc. |
| **Justification** | Référentiel des services ferroviaires nommés ; enrichit les données night_trips de l'entrepôt |
| **Module** | `extractors/scraping_extractor.py` |

### Vérification robots.txt (2026-08-13)

```
# URL vérifié : https://en.wikipedia.org/robots.txt
# Extrait pertinent :
#
#   User-agent: *
#   Disallow: /w/
#   Disallow: /wiki/Special:
#   Allow: /
#
# Résultat pour /wiki/List_of_named_passenger_trains_of_Europe :
#   -> AUTORISÉ pour User-agent: *
#
# Note technique : urllib.robotparser.read() envoie une requête sans User-Agent,
# ce que Wikipedia répond 403 — interprété comme 'Disallow tout'.
# Correction : on récupère robots.txt via requests (avec notre User-Agent identifié)
# puis on appelle rp.parse() manuellement. Voir scraping_extractor._robots_allows().

from extractors.scraping_extractor import _robots_allows
print(_robots_allows(
    "https://en.wikipedia.org/robots.txt",
    "https://en.wikipedia.org/wiki/List_of_named_passenger_trains_of_Europe"
))
# True
```

La vérification est exécutée automatiquement à chaque appel de `extract_scraping()`
via `_robots_allows()` dans `scraping_extractor.py`.

### Politique de scraping Wikimedia

- User-Agent identifié : `ObRail-ETL/1.0 (EPSI formation IA ; usage pedagogique non commercial)`
- Pas de session authentifiée
- Pas de requêtes parallèles (séquentiel, une seule URL)
- Usage non commercial, à faible fréquence

---

## Source 4 — Big Data PySpark (eu_trips.csv en mode local)

| Champ | Valeur |
|-------|--------|
| **Fichier source** | `eu_trips.csv` (même que Source 1) |
| **Framework** | PySpark 3.5.3, mode `local[*]` |
| **Licence** | Apache License 2.0 (PySpark) |
| **Fréquence** | À la demande (batch) |
| **Justification usage Spark** | Voir ci-dessous |
| **Module** | `extractors/spark_pipeline.py` |

### Justification de l'usage de PySpark

PySpark est utilisé en mode `local[*]` sur les 173 662 lignes de `eu_trips.csv` pour :

1. **Démontrer la compétence C1** (systèmes big data) : Spark SQL unifie SQL analytique et
   traitement distribué dans le même API, ce qui justifie son intégration même à petite échelle.

2. **Scalabilité** : le code est identique en production distribuée (YARN, Kubernetes).
   Migrer vers un cluster ne nécessite que de changer `.master("local[*]")` en
   `.master("yarn")` — le code métier reste inchangé.

3. **Agrégations** : le pipeline réalise une agrégation multi-colonnes
   (`GROUP BY country, route_type`) enrichie d'un nom de pays via `F.when()` natif
   (équivalent d'un broadcast join, 100 % JVM — évite les Python workers sur Windows).

Pandas serait suffisant à 173 000 lignes, mais Spark est justifié pédagogiquement et
architecturalement pour la montée en charge.

---

## Source 5 — Base de données entrepot (PostgreSQL)

| Champ | Valeur |
|-------|--------|
| **Connexion** | `postgresql+psycopg2://obrail:***@localhost:5432/obrail` |
| **Schéma** | `entrepot` (créé par etl.py) |
| **Licence** | N/A (données internes au projet) |
| **Fréquence** | Après chaque exécution de `etl.run()` |
| **Contenu** | Agrégats calculés sur les tables day_trips, night_trips, stops, routes, countries |
| **Justification** | Démontre la lecture en boucle (ETL → entrepôt → extraction) et la requête SQL avancée |
| **Module** | `extractors/db_extractor.py` |
