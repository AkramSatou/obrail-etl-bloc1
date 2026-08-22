# Pistes d'enrichissement des données — à explorer après la certification

Document de recherche uniquement. Rien ici n'est intégré au pipeline ni au dossier RNCP.
Aucune de ces pistes n'a été touchée : c'est une liste de sources réelles, vérifiées le
19/08/2026 par recherche web, à évaluer et intégrer après la soutenance du 8/09/2026 si
le projet continue.

## Point important à comprendre avant de commencer

`eu_trips.csv` est un fichier plat : une ligne = un trajet, avec origine, destination,
horaires, déjà prêt à l'emploi. Toutes les sources ci-dessous sont au format **GTFS**
(General Transit Feed Specification), le standard mondial des horaires de transport en
commun. Un GTFS n'est pas un seul CSV : c'est un ZIP contenant plusieurs fichiers CSV liés
entre eux (stops.txt, routes.txt, trips.txt, stop_times.txt, calendar.txt...). Les
exploiter demande donc un vrai travail de transformation (reconstituer un trajet complet
à partir de stop_times.txt trié par horaire, par exemple), pas juste un nouveau fichier à
lire. C'est plus proche de ce que fait déjà `transform()` dans etl.py que d'un simple ajout
de ligne.

---

## France — SNCF (transport.data.gouv.fr)

- **URL** : https://transport.data.gouv.fr/datasets/horaires-sncf
- **Fichier direct** : `https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip`
- **Contenu** : TGV, Intercités et TER, horaires théoriques sur ~151 jours glissants.
  Validité actuelle constatée : 2026-07-12 au 2026-12-31.
- **Formats** : GTFS, NeTEx, plus du temps réel (GTFS-RT, SIRI Lite) sur des flux séparés.
- **Licence** : Open Data (plateforme officielle de l'État, transport.data.gouv.fr).
- **Accès** : téléchargement direct, sans clé ni compte.
- **Intérêt pour ObRail** : la source la plus proche de ce que le projet utilise déjà
  (SNCF), et la plus simple d'accès de toute cette liste. Bon candidat n°1.

Existe aussi un flux séparé pour le Transilien (Île-de-France) si on veut du transport
régional en plus des grandes lignes :
`https://eu.ftp.opendatasoft.com/sncf/gtfs/transilien-gtfs.zip`

---

## Allemagne — GTFS.DE (agrégateur DELFI)

- **URL** : https://gtfs.de/
- **Contenu** : agrège les données officielles DELFI (l'organisme fédéral allemand) —
  bus, trains longue distance et régionaux (dont Deutsche Bahn), métros et trams,
  pour toute l'Allemagne dans un seul flux cohérent.
- **Formats** : GTFS statique + GTFS-RT.
- **Note** : le portail officiel de Deutsche Bahn (data.deutschebahn.com) a été
  réorganisé en mars 2024 — l'accès GTFS de DB passe maintenant par une API
  authentifiée (developer-docs.deutschebahn.com), plus une simple URL de
  téléchargement. GTFS.DE est donc plus simple d'accès et plus complet (tous
  opérateurs allemands, pas seulement DB).
- **Intérêt pour ObRail** : bon candidat pour enrichir le pays DE au delà de
  `de_night.csv`, qui ne couvre aujourd'hui que les trains de nuit.

---

## Suisse — opentransportdata.swiss

- **URL** : https://opendata.swiss/en/dataset/fahrplan-2026-gtfs2020
- **Contenu** : horaire national suisse complet (Fahrplan 2026), régénéré deux fois par
  semaine (mardi et vendredi).
- **Formats** : GTFS statique, GTFS-RT, GTFS-Flex.
- **Licence** : opendata.swiss (licence ouverte suisse).
- **Archive** : versions antérieures disponibles sur archive.opentransportdata.swiss.
- **Intérêt pour ObRail** : CH fait partie des 10 pays déjà dans `COUNTRIES` (etl.py)
  mais n'a pas de fichier CSV dédié aujourd'hui, contrairement à FR/DE. Source
  officielle directe, pas besoin de passer par un agrégateur tiers.

---

## Espagne — Renfe

- **Portail officiel** : https://data.renfe.com/
- **NAP (National Access Point)** : https://nap.transportes.gob.es/Files/Detail/929
  (Cercanías, mis à jour 22/01/2026 : gares, horaires, accessibilité, tarifs, géométrie
  des lignes)
- **Autre point d'accès** : datos.gob.es référence aussi les jeux Renfe.
- **Intérêt pour ObRail** : ES est déjà couvert par `infer_country_day()` dans etl.py
  (détection par mots-clés de gares comme Madrid, Getafe...) mais sans vraie source
  espagnole dédiée. Renfe comblerait ça avec de la vraie donnée plutôt que de
  l'inférence par mot-clé.

---

## Italie — Trenitalia

- **Flux France de Trenitalia** (transport.data.gouv.fr) :
  https://transport.data.gouv.fr/datasets/horaires-des-trains-trenitalia-france
  Valide du 2026-05-01 au 2026-10-30, GTFS + GTFS-RT.
- **Flux international** : Mobility Database référence un flux Trenitalia S.p.A. plus
  large (mdb-840).
- **Intérêt pour ObRail** : la logique IT_KW dans `infer_country_day()` (Firenze, Pisa,
  Roma, Milano...) pourrait être remplacée par de vraies données Trenitalia plutôt que
  par une liste de mots-clés.

---

## Agrégateur multi-pays — Mobility Database

- **URL** : https://mobilitydatabase.org/
- **Contenu** : plus de 6000 flux GTFS/GTFS-RT/GBFS dans 99+ pays, catalogue mondial
  maintenu par MobilityData (organisation à but non lucratif).
- **Exemples ferroviaires pertinents pour l'Europe repérés dans la recherche** :
  SNCB/NMBS (Belgique, dessert aussi DE/LU/FR), Trenord (Italie/Suisse), OVapi
  (Pays-Bas, dessert aussi BE/DE/AT).
- **Intérêt pour ObRail** : un seul point d'entrée pour chercher un flux GTFS par pays
  ou opérateur, utile pour BE, NL, PL, CZ, AT qui n'ont aujourd'hui aucune source
  dédiée dans le projet (seulement l'inférence par défaut vers "DE" dans
  `infer_country_day()` si aucun mot-clé ne correspond).

---

## Eurostat — au delà de RAIL_PA_QUARTAL (déjà utilisé en Source 2)

Deux familles de jeux de données Eurostat existent en plus de celle déjà interrogée par
`extractors/api_extractor.py` :

- **rail_go** — transport ferroviaire de marchandises (fret), pas de voyageurs.
- **Infrastructure ferroviaire** — longueur des voies et lignes, parc de matériel
  roulant, effectifs et dépenses des entreprises ferroviaires.

Ce sont des statistiques agrégées par pays/année, pas des trajets individuels — ça
n'enrichirait pas `day_trips`/`night_trips`, mais ça pourrait enrichir Source 2
(Eurostat) elle même avec plus de métriques de contexte si un jour c'est utile pour
une analyse.

---

## Recommandation, si le projet continue après la certification

Par ordre de simplicité d'intégration : SNCF (transport.data.gouv.fr, aucune clé,
téléchargement direct, déjà le même opérateur que la donnée existante), puis Suisse
(opentransportdata.swiss, source officielle directe), puis GTFS.DE pour l'Allemagne
(plus complet que l'API DB désormais authentifiée). Le vrai travail dans chaque cas sera
d'écrire un parseur GTFS → format `eu_trips.csv` (reconstituer un trajet à partir de
`stop_times.txt`), pas de trouver le fichier lui même.
