-- ============================================================================
-- OBRAIL EUROPE - Schéma de Base de Données
-- ============================================================================
-- Projet: Entrepôt de données ferroviaires européennes
-- Version: 1.0
-- Date: 2025-02-05
-- Description: Création du schéma pour l'analyse des dessertes ferroviaires
--              européennes (trains de jour et trains de nuit)
-- ============================================================================

-- Suppression des tables existantes (pour réinitialisation)
DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS routes CASCADE;
DROP TABLE IF EXISTS stops CASCADE;
DROP TABLE IF EXISTS operators CASCADE;
DROP TABLE IF EXISTS countries CASCADE;
DROP TABLE IF EXISTS data_sources CASCADE;
DROP TABLE IF EXISTS etl_logs CASCADE;

-- ============================================================================
-- TABLE: countries (Pays)
-- ============================================================================
-- Référentiel des pays européens desservis par le réseau ferroviaire
-- ============================================================================
CREATE TABLE countries (
    country_code CHAR(2) PRIMARY KEY,           -- Code ISO 3166-1 alpha-2 (ex: FR, DE, IT)
    country_name VARCHAR(100) NOT NULL,         -- Nom complet du pays
    timezone VARCHAR(50) DEFAULT 'Europe/Paris',-- Fuseau horaire principal
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE countries IS 'Référentiel des pays européens du réseau ferroviaire';
COMMENT ON COLUMN countries.country_code IS 'Code ISO 3166-1 alpha-2 du pays';
COMMENT ON COLUMN countries.country_name IS 'Nom officiel du pays';
COMMENT ON COLUMN countries.timezone IS 'Fuseau horaire principal du pays';

-- ============================================================================
-- TABLE: data_sources (Sources de données)
-- ============================================================================
-- Traçabilité des sources de données pour garantir la fiabilité
-- ============================================================================
CREATE TABLE data_sources (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE,   -- Identifiant de la source
    source_type VARCHAR(50) NOT NULL,           -- Type: API, CSV, GTFS, scraping
    source_url VARCHAR(500),                    -- URL de la source
    description TEXT,                           -- Description détaillée
    license_type VARCHAR(100),                  -- Type de licence (Open Data, etc.)
    last_import_date TIMESTAMP,                 -- Date du dernier import
    is_active BOOLEAN DEFAULT TRUE,             -- Source active ou non
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE data_sources IS 'Référentiel des sources de données pour traçabilité';
COMMENT ON COLUMN data_sources.source_type IS 'Type de source: API, CSV, GTFS, HTML_scraping, Excel';

-- ============================================================================
-- TABLE: operators (Opérateurs ferroviaires)
-- ============================================================================
-- Opérateurs ferroviaires européens (SNCF, DB, ÖBB, Trenitalia, etc.)
-- ============================================================================
CREATE TABLE operators (
    operator_id SERIAL PRIMARY KEY,
    operator_code VARCHAR(20) NOT NULL UNIQUE,  -- Code court de l'opérateur
    operator_name VARCHAR(200) NOT NULL,        -- Nom complet
    country_code CHAR(2),                       -- Pays d'origine
    website VARCHAR(300),                       -- Site web officiel
    is_night_train_operator BOOLEAN DEFAULT FALSE, -- Propose des trains de nuit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_operator_country FOREIGN KEY (country_code) 
        REFERENCES countries(country_code) ON DELETE SET NULL
);

COMMENT ON TABLE operators IS 'Référentiel des opérateurs ferroviaires européens';
COMMENT ON COLUMN operators.is_night_train_operator IS 'Indique si l''opérateur propose des trains de nuit';

-- ============================================================================
-- TABLE: stops (Gares/Arrêts)
-- ============================================================================
-- Référentiel des gares et points d'arrêt du réseau ferroviaire européen
-- ============================================================================
CREATE TABLE stops (
    stop_id SERIAL PRIMARY KEY,
    external_stop_id VARCHAR(100) NOT NULL,     -- ID externe (source d'origine)
    stop_name VARCHAR(300) NOT NULL,            -- Nom de la gare
    stop_name_normalized VARCHAR(300),          -- Nom normalisé (sans accents, majuscules)
    latitude DECIMAL(10, 6) NOT NULL,           -- Latitude WGS84
    longitude DECIMAL(10, 6) NOT NULL,          -- Longitude WGS84
    country_code CHAR(2),                       -- Pays de la gare
    city VARCHAR(200),                          -- Ville
    is_major_station BOOLEAN DEFAULT FALSE,     -- Gare principale/centrale
    source_id INTEGER,                          -- Source de la donnée
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_stop_country FOREIGN KEY (country_code) 
        REFERENCES countries(country_code) ON DELETE SET NULL,
    CONSTRAINT fk_stop_source FOREIGN KEY (source_id) 
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    CONSTRAINT uq_stop_external UNIQUE (external_stop_id, source_id)
);

-- Index pour optimisation des recherches géographiques
CREATE INDEX idx_stops_coordinates ON stops(latitude, longitude);
CREATE INDEX idx_stops_country ON stops(country_code);
CREATE INDEX idx_stops_name ON stops(stop_name_normalized);

COMMENT ON TABLE stops IS 'Référentiel des gares et points d''arrêt ferroviaires';
COMMENT ON COLUMN stops.external_stop_id IS 'Identifiant dans le système source';
COMMENT ON COLUMN stops.stop_name_normalized IS 'Nom normalisé pour faciliter les recherches';
COMMENT ON COLUMN stops.is_major_station IS 'Indique une gare principale (Hbf, Centrale, etc.)';

-- ============================================================================
-- TABLE: routes (Lignes ferroviaires)
-- ============================================================================
-- Lignes et itinéraires ferroviaires
-- ============================================================================
CREATE TABLE routes (
    route_id SERIAL PRIMARY KEY,
    external_route_id VARCHAR(200) NOT NULL,    -- ID externe (source d'origine)
    route_name VARCHAR(500),                    -- Nom de la ligne
    route_type INTEGER NOT NULL,                -- Type GTFS (0=Tram, 1=Metro, 2=Train, 3=Bus)
    service_type VARCHAR(50),                   -- Type: jour, nuit, regional, international
    operator_id INTEGER,                        -- Opérateur de la ligne
    source_id INTEGER,                          -- Source de la donnée
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_route_operator FOREIGN KEY (operator_id) 
        REFERENCES operators(operator_id) ON DELETE SET NULL,
    CONSTRAINT fk_route_source FOREIGN KEY (source_id) 
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    CONSTRAINT uq_route_external UNIQUE (external_route_id, source_id)
);

CREATE INDEX idx_routes_type ON routes(route_type);
CREATE INDEX idx_routes_service_type ON routes(service_type);

COMMENT ON TABLE routes IS 'Référentiel des lignes ferroviaires';
COMMENT ON COLUMN routes.route_type IS 'Type GTFS: 0=Tram, 1=Metro, 2=Train, 3=Bus';
COMMENT ON COLUMN routes.service_type IS 'Classification ObRail: jour, nuit, regional, international';

-- ============================================================================
-- TABLE: trips (Trajets/Voyages)
-- ============================================================================
-- Table principale des trajets ferroviaires
-- ============================================================================
CREATE TABLE trips (
    trip_id SERIAL PRIMARY KEY,
    external_trip_id VARCHAR(300) NOT NULL,     -- ID externe (source d'origine)
    external_service_id VARCHAR(100),           -- ID du service (calendrier)
    
    -- Relation avec les autres tables
    route_id INTEGER,                           -- Ligne empruntée
    origin_stop_id INTEGER NOT NULL,            -- Gare de départ
    destination_stop_id INTEGER NOT NULL,       -- Gare d'arrivée
    source_id INTEGER,                          -- Source de la donnée
    
    -- Informations temporelles
    departure_minutes INTEGER,                  -- Minutes depuis minuit (départ)
    arrival_minutes INTEGER,                    -- Minutes depuis minuit (arrivée)
    duration_minutes INTEGER,                   -- Durée du trajet en minutes
    departure_hour INTEGER,                     -- Heure de départ (0-23)
    arrival_hour INTEGER,                       -- Heure d'arrivée (0-23)
    
    -- Caractéristiques du trajet
    n_stops INTEGER DEFAULT 2,                  -- Nombre d'arrêts
    distance_km DECIMAL(10, 2),                 -- Distance en km (calculée si possible)
    
    -- Classification ObRail
    is_night_trip BOOLEAN DEFAULT FALSE,        -- Train de nuit (21h-6h)
    is_international BOOLEAN DEFAULT FALSE,     -- Trajet transfrontalier
    
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_trip_route FOREIGN KEY (route_id) 
        REFERENCES routes(route_id) ON DELETE SET NULL,
    CONSTRAINT fk_trip_origin FOREIGN KEY (origin_stop_id) 
        REFERENCES stops(stop_id) ON DELETE CASCADE,
    CONSTRAINT fk_trip_destination FOREIGN KEY (destination_stop_id) 
        REFERENCES stops(stop_id) ON DELETE CASCADE,
    CONSTRAINT fk_trip_source FOREIGN KEY (source_id) 
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    CONSTRAINT uq_trip_external UNIQUE (external_trip_id, source_id)
);

-- Index pour optimisation des requêtes fréquentes
CREATE INDEX idx_trips_origin ON trips(origin_stop_id);
CREATE INDEX idx_trips_destination ON trips(destination_stop_id);
CREATE INDEX idx_trips_night ON trips(is_night_trip);
CREATE INDEX idx_trips_international ON trips(is_international);
CREATE INDEX idx_trips_route ON trips(route_id);
CREATE INDEX idx_trips_departure ON trips(departure_hour);
CREATE INDEX idx_trips_duration ON trips(duration_minutes);

COMMENT ON TABLE trips IS 'Table principale des trajets ferroviaires européens';
COMMENT ON COLUMN trips.departure_minutes IS 'Minutes depuis minuit pour le départ';
COMMENT ON COLUMN trips.is_night_trip IS 'Train de nuit: départ entre 21h et 6h';
COMMENT ON COLUMN trips.is_international IS 'Trajet traversant au moins une frontière';

-- ============================================================================
-- TABLE: etl_logs (Journaux ETL)
-- ============================================================================
-- Traçabilité des opérations ETL pour audit et débogage
-- ============================================================================
CREATE TABLE etl_logs (
    log_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,             -- Nom du job ETL
    source_id INTEGER,                          -- Source traitée
    status VARCHAR(20) NOT NULL,                -- SUCCESS, FAILURE, WARNING
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_rejected INTEGER DEFAULT 0,
    error_message TEXT,
    details JSONB,                              -- Détails supplémentaires en JSON
    CONSTRAINT fk_etl_source FOREIGN KEY (source_id) 
        REFERENCES data_sources(source_id) ON DELETE SET NULL
);

CREATE INDEX idx_etl_logs_job ON etl_logs(job_name);
CREATE INDEX idx_etl_logs_status ON etl_logs(status);
CREATE INDEX idx_etl_logs_date ON etl_logs(started_at);

COMMENT ON TABLE etl_logs IS 'Journal des opérations ETL pour traçabilité et audit';

-- ============================================================================
-- VUES ANALYTIQUES
-- ============================================================================

-- Vue: Statistiques par pays
CREATE OR REPLACE VIEW v_country_statistics AS
SELECT 
    c.country_code,
    c.country_name,
    COUNT(DISTINCT s.stop_id) AS total_stops,
    COUNT(DISTINCT t.trip_id) AS total_trips,
    COUNT(DISTINCT CASE WHEN t.is_night_trip THEN t.trip_id END) AS night_trips,
    COUNT(DISTINCT CASE WHEN NOT t.is_night_trip THEN t.trip_id END) AS day_trips,
    ROUND(AVG(t.duration_minutes)::numeric, 2) AS avg_duration_minutes
FROM countries c
LEFT JOIN stops s ON c.country_code = s.country_code
LEFT JOIN trips t ON t.origin_stop_id = s.stop_id OR t.destination_stop_id = s.stop_id
GROUP BY c.country_code, c.country_name;

COMMENT ON VIEW v_country_statistics IS 'Statistiques agrégées par pays';

-- Vue: Comparaison trains de jour vs trains de nuit
CREATE OR REPLACE VIEW v_day_night_comparison AS
SELECT 
    CASE WHEN is_night_trip THEN 'Nuit' ELSE 'Jour' END AS trip_type,
    COUNT(*) AS total_trips,
    ROUND(AVG(duration_minutes)::numeric, 2) AS avg_duration,
    ROUND(AVG(n_stops)::numeric, 2) AS avg_stops,
    MIN(duration_minutes) AS min_duration,
    MAX(duration_minutes) AS max_duration
FROM trips
GROUP BY is_night_trip;

COMMENT ON VIEW v_day_night_comparison IS 'Comparaison statistique entre trains de jour et de nuit';

-- Vue: Top des liaisons (origine-destination)
CREATE OR REPLACE VIEW v_top_connections AS
SELECT 
    so.stop_name AS origin_name,
    sd.stop_name AS destination_name,
    so.country_code AS origin_country,
    sd.country_code AS dest_country,
    COUNT(*) AS trip_count,
    ROUND(AVG(t.duration_minutes)::numeric, 2) AS avg_duration
FROM trips t
JOIN stops so ON t.origin_stop_id = so.stop_id
JOIN stops sd ON t.destination_stop_id = sd.stop_id
GROUP BY so.stop_name, sd.stop_name, so.country_code, sd.country_code
ORDER BY trip_count DESC;

COMMENT ON VIEW v_top_connections IS 'Classement des liaisons les plus fréquentes';

-- ============================================================================
-- INSERTION DES DONNÉES DE RÉFÉRENCE
-- ============================================================================

-- Pays européens principaux
INSERT INTO countries (country_code, country_name, timezone) VALUES
('AT', 'Autriche', 'Europe/Vienna'),
('BE', 'Belgique', 'Europe/Brussels'),
('CH', 'Suisse', 'Europe/Zurich'),
('CZ', 'République tchèque', 'Europe/Prague'),
('DE', 'Allemagne', 'Europe/Berlin'),
('DK', 'Danemark', 'Europe/Copenhagen'),
('ES', 'Espagne', 'Europe/Madrid'),
('FI', 'Finlande', 'Europe/Helsinki'),
('FR', 'France', 'Europe/Paris'),
('GB', 'Royaume-Uni', 'Europe/London'),
('HR', 'Croatie', 'Europe/Zagreb'),
('HU', 'Hongrie', 'Europe/Budapest'),
('IT', 'Italie', 'Europe/Rome'),
('LT', 'Lituanie', 'Europe/Vilnius'),
('LU', 'Luxembourg', 'Europe/Luxembourg'),
('NL', 'Pays-Bas', 'Europe/Amsterdam'),
('NO', 'Norvège', 'Europe/Oslo'),
('PL', 'Pologne', 'Europe/Warsaw'),
('PT', 'Portugal', 'Europe/Lisbon'),
('RO', 'Roumanie', 'Europe/Bucharest'),
('SE', 'Suède', 'Europe/Stockholm'),
('SI', 'Slovénie', 'Europe/Ljubljana'),
('SK', 'Slovaquie', 'Europe/Bratislava');

-- Sources de données
INSERT INTO data_sources (source_name, source_type, source_url, description, license_type) VALUES
('mobilitydatabase', 'GTFS', 'https://mobilitydatabase.org/', 'Base de données de mobilité internationale - Flux GTFS', 'Open Data'),
('transport_data_gouv_fr', 'GTFS', 'https://transport.data.gouv.fr/', 'Portail français open data transports', 'Open Data - Etalab'),
('back_on_track', 'CSV', 'https://back-on-track.eu/', 'Base de données des trains de nuit européens', 'Open Data'),
('eurostat', 'API', 'https://ec.europa.eu/eurostat/', 'Statistiques européennes sur les transports', 'Open Data'),
('db_opendata', 'GTFS', 'https://data.deutschebahn.com/', 'Open Data Deutsche Bahn', 'CC BY 4.0'),
('sncf_opendata', 'GTFS', 'https://ressources.data.sncf.com/', 'Open Data SNCF', 'Open Data');

-- Opérateurs ferroviaires principaux
INSERT INTO operators (operator_code, operator_name, country_code, is_night_train_operator) VALUES
('SNCF', 'SNCF Voyageurs', 'FR', TRUE),
('DB', 'Deutsche Bahn', 'DE', TRUE),
('OBB', 'ÖBB (Nightjet)', 'AT', TRUE),
('TRENITALIA', 'Trenitalia', 'IT', TRUE),
('RENFE', 'Renfe', 'ES', FALSE),
('SBB', 'SBB CFF FFS', 'CH', FALSE),
('EUROSTAR', 'Eurostar', 'GB', FALSE),
('THALYS', 'Thalys', 'BE', FALSE),
('NS', 'NS (Nederlandse Spoorwegen)', 'NL', FALSE),
('CD', 'České dráhy', 'CZ', FALSE),
('PKP', 'PKP Intercity', 'PL', TRUE),
('VR', 'VR (Finnish Railways)', 'FI', TRUE);

-- ============================================================================
-- FIN DU SCRIPT DE CRÉATION
-- ============================================================================
