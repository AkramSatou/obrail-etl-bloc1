# Registre de traitement des données — ObRail ETL Bloc 1

**Date de rédaction :** 2026-08-13
**Responsable de traitement :** Akram (apprenant RNCP 37827 — Simplon)
**Contexte :** Projet pédagogique non commercial — certification Développeur IA

---

## 1. Finalité du traitement

Le projet ObRail agrège des données ouvertes sur les transports ferroviaires européens afin de :
- alimenter un entrepôt de données analytique (schéma `entrepot`) ;
- fournir des statistiques de fréquentation à des fins d'exploration ;
- démontrer la maîtrise des compétences C1–C4 du bloc 1 RNCP 37827.

---

## 2. Données traitées

### 2.1 Sources et nature des données

| Source | Nature des données | Données personnelles ? |
|--------|--------------------|------------------------|
| `eu_trips.csv` | Horaires et fréquences de trajets ferroviaires UE | **Non** — données de service, aucun passager individualisé |
| API Eurostat `RAIL_PA_QUARTAL` | Statistiques agrégées trimestrielles par pays | **Non** — totaux agrégés, aucun individu |
| Wikipedia (trains de nuit) | Noms de lignes, opérateurs, pays desservis | **Non** — informations publiques sur des services |
| Schéma `entrepot` PostgreSQL | Dérivé des sources ci-dessus | **Non** — agrégats et métadonnées |

### 2.2 Conclusion RGPD

> **Aucune donnée à caractère personnel n'est collectée, stockée ou traitée.**
> Le RGPD (UE 2016/679) s'applique uniquement aux personnes physiques identifiables.
> Ce projet ne déclenche donc **pas** d'obligation de registre CNIL formelle,
> mais un registre est maintenu à titre de bonne pratique conformément à l'article 30 RGPD.

---

## 3. Fondements juridiques des traitements

| Traitement | Base légale | Détail |
|------------|-------------|--------|
| Chargement `eu_trips.csv` | Intérêt légitime (art. 6.1.f) | Données ouvertes, usage éducatif |
| Requête API Eurostat | Données publiques (art. 86) | Données officielles UE, licence CC BY 4.0 |
| Scraping Wikipedia | Données publiques (art. 86) | Licence CC BY-SA 4.0, robots.txt vérifié |
| Stockage PostgreSQL local | Intérêt légitime | Usage local, non exposé, pas de transfert |

---

## 4. Transferts hors UE

**Aucun transfert.** La base PostgreSQL est hébergée localement (Docker sur poste Windows).
Les API consultées (Eurostat) sont hébergées sur des serveurs de la Commission Européenne (UE).

---

## 5. Durée de conservation

Les données sont conservées le temps du projet pédagogique (jusqu'à soutenance, septembre 2026).
Le schéma `entrepot` peut être supprimé à tout moment via :
```sql
DROP SCHEMA IF EXISTS entrepot CASCADE;
```

---

## 6. Sécurité

- Base de données accessible uniquement en local (port 5432 non exposé publiquement)
- Credentials dans `.env` (non commité, listé dans `.gitignore`)
- Aucun transfert chiffré requis (pas de données sensibles)

---

## 7. Droits des personnes

Sans objet (aucune donnée personnelle traitée).

---

## 8. Sous-traitants / tiers

| Tiers | Rôle | Localisation |
|-------|------|--------------|
| Eurostat (Commission Européenne) | Fournisseur de données | UE |
| Wikimedia Foundation | Fournisseur de données | USA — données publiques, pas de transfert de données personnelles |
| Docker Inc. | Conteneurisation locale | N/A (logiciel local) |

---

*Document conforme à l'article 30 RGPD — tenu à jour par le responsable de traitement.*
