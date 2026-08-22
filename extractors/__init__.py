"""
Extracteurs ObRail — 5 sources hétérogènes (C1)

  api_extractor   : statistiques ferroviaires Eurostat (API REST JSON)
  scraping        : liste trains de nuit Wikipedia (HTML/BeautifulSoup)
  spark_pipeline  : agrégations sur eu_trips.csv (PySpark local)
  db_extractor    : consultation du schéma entrepot PostgreSQL
  (CSV)           : déjà géré dans etl.py

Chaque module expose extract_*() → pd.DataFrame
"""
