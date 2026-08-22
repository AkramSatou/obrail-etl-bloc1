-- ============================================================================
-- OBRAIL EUROPE - Script de Création de la Base de Données
-- ============================================================================
-- SGBD : MySQL / MariaDB
-- Nom de la BDD : obrail_europe_db
-- Version: 1.0
-- Date: 2025-02-05
-- ============================================================================

-- Création de la base de données
CREATE DATABASE IF NOT EXISTS obrail_europe_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE obrail_europe_db;

-- ============================================================================
-- SUPPRESSION DES TABLES EXISTANTES (pour réinitialisation)
-- ============================================================================
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS routes;
DROP TABLE IF EXISTS stops;
DROP TABLE IF EXISTS operators;
DROP TABLE IF EXISTS countries;
DROP TABLE IF EXISTS data_sources;
DROP TABLE IF EXISTS etl_logs;
SET FOREIGN_KEY_CHECKS = 1;

-- ============================================================================
-- TABLE: countries (Pays)
-- ============================================================================
CREATE TABLE countries (
    country_code CHAR(2) PRIMARY KEY COMMENT 'Code ISO 3166-1 alpha-2',
    country_name VARCHAR(100) NOT NULL COMMENT 'Nom officiel du pays',
    timezone VARCHAR(50) DEFAULT 'Europe/Paris' COMMENT 'Fuseau horaire principal',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Référentiel des pays européens du réseau ferroviaire';

-- ============================================================================
-- TABLE: data_sources (Sources de données)
-- ============================================================================
CREATE TABLE data_sources (
    source_id INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE COMMENT 'Identifiant unique de la source',
    source_type VARCHAR(50) NOT NULL COMMENT 'Type: API, CSV, GTFS, scraping',
    source_url VARCHAR(500) COMMENT 'URL de la source',
    description TEXT COMMENT 'Description détaillée',
    license_type VARCHAR(100) COMMENT 'Type de licence',
    last_import_date TIMESTAMP NULL COMMENT 'Date du dernier import',
    is_active BOOLEAN DEFAULT TRUE COMMENT 'Source active ou non',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Référentiel des sources de données pour traçabilité RGPD';

-- ============================================================================
-- TABLE: operators (Opérateurs ferroviaires)
-- ============================================================================
CREATE TABLE operators (
    operator_id INT AUTO_INCREMENT PRIMARY KEY,
    operator_code VARCHAR(20) NOT NULL UNIQUE COMMENT 'Code court (SNCF, DB, OBB...)',
    operator_name VARCHAR(200) NOT NULL COMMENT 'Nom complet',
    country_code CHAR(2) COMMENT 'Pays origine',
    website VARCHAR(300) COMMENT 'Site web officiel',
    is_night_train_operator BOOLEAN DEFAULT FALSE COMMENT 'Propose des trains de nuit',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_operator_country FOREIGN KEY (country_code) 
        REFERENCES countries(country_code) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Référentiel des opérateurs ferroviaires européens';

-- ============================================================================
-- TABLE: stops (Gares/Arrêts)
-- ============================================================================
CREATE TABLE stops (
    stop_id INT AUTO_INCREMENT PRIMARY KEY,
    external_stop_id VARCHAR(100) NOT NULL COMMENT 'ID système source',
    stop_name VARCHAR(300) NOT NULL COMMENT 'Nom de la gare',
    stop_name_normalized VARCHAR(300) COMMENT 'Nom normalisé pour recherche',
    latitude DECIMAL(10, 6) NOT NULL COMMENT 'Latitude WGS84',
    longitude DECIMAL(10, 6) NOT NULL COMMENT 'Longitude WGS84',
    country_code CHAR(2) COMMENT 'Pays',
    city VARCHAR(200) COMMENT 'Ville',
    is_major_station BOOLEAN DEFAULT FALSE COMMENT 'Gare principale (Hbf, Centrale)',
    source_id INT COMMENT 'Source de la donnée',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_stop_country FOREIGN KEY (country_code) 
        REFERENCES countries(country_code) ON DELETE SET NULL,
    CONSTRAINT fk_stop_source FOREIGN KEY (source_id) 
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    UNIQUE KEY uq_stop_external (external_stop_id, source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Référentiel des gares et points d''arrêt ferroviaires';

-- Index pour optimisation
CREATE INDEX idx_stops_coordinates ON stops(latitude, longitude);
CREATE INDEX idx_stops_country ON stops(country_code);
CREATE INDEX idx_stops_name ON stops(stop_name_normalized);

-- ============================================================================
-- TABLE: routes (Lignes ferroviaires)
-- ============================================================================
CREATE TABLE routes (
    route_id INT AUTO_INCREMENT PRIMARY KEY,
    external_route_id VARCHAR(200) NOT NULL COMMENT 'ID système source',
    route_name VARCHAR(500) COMMENT 'Nom de la ligne',
    route_type INT NOT NULL COMMENT 'Type GTFS (0=Tram, 1=Metro, 2=Train, 3=Bus)',
    service_type VARCHAR(50) COMMENT 'Classification: jour, nuit, regional, international',
    operator_id INT COMMENT 'Opérateur',
    source_id INT COMMENT 'Source',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_route_operator FOREIGN KEY (operator_id) 
        REFERENCES operators(operator_id) ON DELETE SET NULL,
    CONSTRAINT fk_route_source FOREIGN KEY (source_id) 
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    UNIQUE KEY uq_route_external (external_route_id, source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Référentiel des lignes ferroviaires';

CREATE INDEX idx_routes_type ON routes(route_type);
CREATE INDEX idx_routes_service_type ON routes(service_type);

-- ============================================================================
-- TABLE: trips (Trajets/Voyages) - TABLE PRINCIPALE
-- ============================================================================
CREATE TABLE trips (
    trip_id INT AUTO_INCREMENT PRIMARY KEY,
    external_trip_id VARCHAR(300) NOT NULL COMMENT 'ID système source',
    external_service_id VARCHAR(100) COMMENT 'ID calendrier de service',
    route_id INT COMMENT 'Ligne empruntée',
    origin_stop_id INT NOT NULL COMMENT 'Gare de départ',
    destination_stop_id INT NOT NULL COMMENT 'Gare d''arrivée',
    source_id INT COMMENT 'Source',
    departure_minutes INT COMMENT 'Minutes depuis minuit (départ)',
    arrival_minutes INT COMMENT 'Minutes depuis minuit (arrivée)',
    duration_minutes INT COMMENT 'Durée du trajet',
    departure_hour INT COMMENT 'Heure de départ (0-23)',
    arrival_hour INT COMMENT 'Heure d''arrivée (0-23)',
    n_stops INT DEFAULT 2 COMMENT 'Nombre d''arrêts',
    distance_km DECIMAL(10, 2) COMMENT 'Distance en km',
    is_night_trip BOOLEAN DEFAULT FALSE COMMENT 'Train de nuit (21h-6h)',
    is_international BOOLEAN DEFAULT FALSE COMMENT 'Trajet transfrontalier',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_trip_route FOREIGN KEY (route_id) 
        REFERENCES routes(route_id) ON DELETE SET NULL,
    CONSTRAINT fk_trip_origin FOREIGN KEY (origin_stop_id) 
        REFERENCES stops(stop_id) ON DELETE CASCADE,
    CONSTRAINT fk_trip_destination FOREIGN KEY (destination_stop_id) 
        REFERENCES stops(stop_id) ON DELETE CASCADE,
    CONSTRAINT fk_trip_source FOREIGN KEY (source_id) 
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    UNIQUE KEY uq_trip_external (external_trip_id, source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Table principale des trajets ferroviaires européens';

-- Index pour optimisation des requêtes
CREATE INDEX idx_trips_origin ON trips(origin_stop_id);
CREATE INDEX idx_trips_destination ON trips(destination_stop_id);
CREATE INDEX idx_trips_night ON trips(is_night_trip);
CREATE INDEX idx_trips_international ON trips(is_international);
CREATE INDEX idx_trips_route ON trips(route_id);
CREATE INDEX idx_trips_departure ON trips(departure_hour);
CREATE INDEX idx_trips_duration ON trips(duration_minutes);

-- ============================================================================
-- TABLE: etl_logs (Journaux ETL)
-- ============================================================================
CREATE TABLE etl_logs (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL COMMENT 'Nom du job ETL',
    source_id INT COMMENT 'Source traitée',
    status VARCHAR(20) NOT NULL COMMENT 'SUCCESS, FAILURE, WARNING',
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NULL,
    records_processed INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    records_updated INT DEFAULT 0,
    records_rejected INT DEFAULT 0,
    error_message TEXT,
    details JSON COMMENT 'Détails supplémentaires',
    CONSTRAINT fk_etl_source FOREIGN KEY (source_id) 
        REFERENCES data_sources(source_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Journal des opérations ETL pour traçabilité et audit';

CREATE INDEX idx_etl_logs_job ON etl_logs(job_name);
CREATE INDEX idx_etl_logs_status ON etl_logs(status);
CREATE INDEX idx_etl_logs_date ON etl_logs(started_at);

-- ============================================================================
-- VUES ANALYTIQUES
-- ============================================================================

-- Vue: Comparaison trains de jour vs trains de nuit
CREATE OR REPLACE VIEW v_day_night_comparison AS
SELECT 
    CASE WHEN is_night_trip THEN 'Nuit' ELSE 'Jour' END AS trip_type,
    COUNT(*) AS total_trips,
    ROUND(AVG(duration_minutes), 2) AS avg_duration,
    ROUND(AVG(n_stops), 2) AS avg_stops,
    MIN(duration_minutes) AS min_duration,
    MAX(duration_minutes) AS max_duration
FROM trips
GROUP BY is_night_trip;

-- Vue: Statistiques par pays
CREATE OR REPLACE VIEW v_country_statistics AS
SELECT 
    c.country_code,
    c.country_name,
    COUNT(DISTINCT s.stop_id) AS total_stops,
    COUNT(DISTINCT t.trip_id) AS total_trips,
    SUM(CASE WHEN t.is_night_trip = 1 THEN 1 ELSE 0 END) AS night_trips,
    SUM(CASE WHEN t.is_night_trip = 0 THEN 1 ELSE 0 END) AS day_trips
FROM countries c
LEFT JOIN stops s ON c.country_code = s.country_code
LEFT JOIN trips t ON t.origin_stop_id = s.stop_id
GROUP BY c.country_code, c.country_name;

-- Vue: Top des liaisons
CREATE OR REPLACE VIEW v_top_connections AS
SELECT 
    so.stop_name AS origin_name,
    sd.stop_name AS destination_name,
    so.country_code AS origin_country,
    sd.country_code AS dest_country,
    COUNT(*) AS trip_count,
    ROUND(AVG(t.duration_minutes), 2) AS avg_duration
FROM trips t
JOIN stops so ON t.origin_stop_id = so.stop_id
JOIN stops sd ON t.destination_stop_id = sd.stop_id
GROUP BY so.stop_name, sd.stop_name, so.country_code, sd.country_code
ORDER BY trip_count DESC;

-- ============================================================================
-- INSERTION DES DONNÉES DE RÉFÉRENCE
-- ============================================================================

-- Pays européens
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

-- Opérateurs ferroviaires
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
-- VÉRIFICATION
-- ============================================================================
SELECT 'Base obrail_europe_db créée avec succès !' AS message;
SELECT 'Tables créées' AS info, COUNT(*) AS nombre FROM information_schema.tables WHERE table_schema = 'obrail_europe_db';
SELECT 'Pays insérés' AS info, COUNT(*) AS nombre FROM countries;
SELECT 'Sources insérées' AS info, COUNT(*) AS nombre FROM data_sources;
SELECT 'Opérateurs insérés' AS info, COUNT(*) AS nombre FROM operators;
