-- ============================================================================
-- OBRAIL EUROPE — Schéma MySQL
-- ============================================================================
-- SGBD    : MySQL / MariaDB
-- Base    : obrail_europe_db
-- Version : 1.0
-- Usage   : mysql -u root -p < 00_create_schema_mysql.sql
-- ============================================================================

CREATE DATABASE IF NOT EXISTS obrail_europe_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE obrail_europe_db;

SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS etl_logs;
DROP TABLE IF EXISTS night_trips;
DROP TABLE IF EXISTS day_trips;
DROP TABLE IF EXISTS routes;
DROP TABLE IF EXISTS stops;
DROP TABLE IF EXISTS operators;
DROP TABLE IF EXISTS data_sources;
DROP TABLE IF EXISTS countries;
SET FOREIGN_KEY_CHECKS = 1;

-- ============================================================================
-- TABLE: countries
-- ============================================================================
CREATE TABLE countries (
    country_code CHAR(2)      PRIMARY KEY COMMENT 'Code ISO 3166-1 alpha-2',
    country_name VARCHAR(100) NOT NULL COMMENT 'Nom officiel',
    timezone     VARCHAR(50)  DEFAULT 'Europe/Paris',
    created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- TABLE: data_sources
-- ============================================================================
CREATE TABLE data_sources (
    source_id        INT AUTO_INCREMENT PRIMARY KEY,
    source_name      VARCHAR(100) NOT NULL UNIQUE,
    source_type      VARCHAR(50)  NOT NULL COMMENT 'API, CSV, GTFS, scraping',
    source_url       VARCHAR(500),
    description      TEXT,
    license_type     VARCHAR(100),
    last_import_date TIMESTAMP    NULL,
    is_active        BOOLEAN      DEFAULT TRUE,
    created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- TABLE: operators
-- ============================================================================
CREATE TABLE operators (
    operator_id             INT AUTO_INCREMENT PRIMARY KEY,
    operator_code           VARCHAR(20)  NOT NULL UNIQUE,
    operator_name           VARCHAR(200) NOT NULL,
    country_code            CHAR(2),
    website                 VARCHAR(300),
    is_night_train_operator BOOLEAN DEFAULT FALSE,
    is_day_train_operator   BOOLEAN DEFAULT TRUE,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_operator_country FOREIGN KEY (country_code)
        REFERENCES countries(country_code) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- TABLE: stops
-- ============================================================================
CREATE TABLE stops (
    stop_id              INT AUTO_INCREMENT PRIMARY KEY,
    external_stop_id     VARCHAR(100) NOT NULL,
    stop_name            VARCHAR(300) NOT NULL,
    stop_name_normalized VARCHAR(300),
    latitude             DECIMAL(10,6) NOT NULL,
    longitude            DECIMAL(10,6) NOT NULL,
    country_code         CHAR(2),
    city                 VARCHAR(200),
    is_major_station     BOOLEAN DEFAULT FALSE,
    source_id            INT,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_stop_country FOREIGN KEY (country_code)
        REFERENCES countries(country_code) ON DELETE SET NULL,
    CONSTRAINT fk_stop_source FOREIGN KEY (source_id)
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    UNIQUE KEY uq_stop_external (external_stop_id, source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_stops_coordinates ON stops(latitude, longitude);
CREATE INDEX idx_stops_country     ON stops(country_code);
CREATE INDEX idx_stops_name        ON stops(stop_name_normalized);

-- ============================================================================
-- TABLE: routes
-- ============================================================================
CREATE TABLE routes (
    route_id          INT AUTO_INCREMENT PRIMARY KEY,
    external_route_id VARCHAR(200) NOT NULL,
    route_short_name  VARCHAR(100),
    route_long_name   VARCHAR(500),
    route_type        INT NOT NULL,
    service_type      VARCHAR(50),
    operator_id       INT,
    source_id         INT,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_route_operator FOREIGN KEY (operator_id)
        REFERENCES operators(operator_id) ON DELETE SET NULL,
    CONSTRAINT fk_route_source FOREIGN KEY (source_id)
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    UNIQUE KEY uq_route_external (external_route_id, source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_routes_type         ON routes(route_type);
CREATE INDEX idx_routes_service_type ON routes(service_type);

-- ============================================================================
-- TABLE: day_trips (trains de jour)
-- ============================================================================
CREATE TABLE day_trips (
    trip_id             INT AUTO_INCREMENT PRIMARY KEY,
    external_trip_id    VARCHAR(300) NOT NULL,
    external_service_id VARCHAR(100),
    route_id            INT,
    route_long_name     VARCHAR(500),
    route_type          INT,
    origin_stop_id      INT NOT NULL,
    destination_stop_id INT NOT NULL,
    origin_stop_name      VARCHAR(300),
    destination_stop_name VARCHAR(300),
    origin_stop_lat       DECIMAL(10,6),
    origin_stop_lon       DECIMAL(10,6),
    destination_stop_lat  DECIMAL(10,6),
    destination_stop_lon  DECIMAL(10,6),
    country             CHAR(2),
    source_id           INT,
    departure_minutes   INT,
    arrival_minutes     INT,
    departure_hour      INT,
    arrival_hour        INT,
    duration_minutes    INT,
    n_stops             INT DEFAULT 2,
    distance_km         DECIMAL(10,2),
    is_international    BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_day_origin  FOREIGN KEY (origin_stop_id)
        REFERENCES stops(stop_id) ON DELETE CASCADE,
    CONSTRAINT fk_day_dest    FOREIGN KEY (destination_stop_id)
        REFERENCES stops(stop_id) ON DELETE CASCADE,
    CONSTRAINT fk_day_source  FOREIGN KEY (source_id)
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    UNIQUE KEY uq_day_trip (external_trip_id, source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- TABLE: night_trips (trains de nuit)
-- ============================================================================
CREATE TABLE night_trips (
    trip_id             INT AUTO_INCREMENT PRIMARY KEY,
    external_trip_id    VARCHAR(300) NOT NULL,
    external_service_id VARCHAR(100),
    route_id            INT,
    route_long_name     VARCHAR(500),
    route_type          INT,
    origin_stop_id      INT NOT NULL,
    destination_stop_id INT NOT NULL,
    origin_stop_name      VARCHAR(300),
    destination_stop_name VARCHAR(300),
    origin_stop_lat       DECIMAL(10,6),
    origin_stop_lon       DECIMAL(10,6),
    destination_stop_lat  DECIMAL(10,6),
    destination_stop_lon  DECIMAL(10,6),
    country             CHAR(2),
    source_id           INT,
    departure_minutes   INT,
    arrival_minutes     INT,
    departure_hour      INT,
    arrival_hour        INT,
    duration_minutes    INT,
    n_stops             INT DEFAULT 2,
    distance_km         DECIMAL(10,2),
    is_international    BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_night_origin  FOREIGN KEY (origin_stop_id)
        REFERENCES stops(stop_id) ON DELETE CASCADE,
    CONSTRAINT fk_night_dest    FOREIGN KEY (destination_stop_id)
        REFERENCES stops(stop_id) ON DELETE CASCADE,
    CONSTRAINT fk_night_source  FOREIGN KEY (source_id)
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    UNIQUE KEY uq_night_trip (external_trip_id, source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- TABLE: etl_logs
-- ============================================================================
CREATE TABLE etl_logs (
    log_id            INT AUTO_INCREMENT PRIMARY KEY,
    job_name          VARCHAR(100) NOT NULL,
    source_id         INT,
    train_type        VARCHAR(20) COMMENT 'jour ou nuit',
    status            VARCHAR(20) NOT NULL COMMENT 'SUCCESS, FAILURE, WARNING',
    started_at        TIMESTAMP NOT NULL,
    ended_at          TIMESTAMP NULL,
    records_processed INT DEFAULT 0,
    records_inserted  INT DEFAULT 0,
    records_updated   INT DEFAULT 0,
    records_rejected  INT DEFAULT 0,
    error_message     TEXT,
    details           JSON,
    CONSTRAINT fk_etl_source FOREIGN KEY (source_id)
        REFERENCES data_sources(source_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_etl_logs_job    ON etl_logs(job_name);
CREATE INDEX idx_etl_logs_status ON etl_logs(status);
CREATE INDEX idx_etl_logs_date   ON etl_logs(started_at);

-- ============================================================================
-- VUES
-- ============================================================================

CREATE OR REPLACE VIEW v_day_night_comparison AS
SELECT 'Jour' AS trip_type,
       COUNT(*) AS total_trips,
       ROUND(AVG(duration_minutes), 2) AS avg_duration,
       ROUND(AVG(n_stops), 2) AS avg_stops,
       MIN(duration_minutes) AS min_duration,
       MAX(duration_minutes) AS max_duration
FROM day_trips
UNION ALL
SELECT 'Nuit',
       COUNT(*),
       ROUND(AVG(duration_minutes), 2),
       ROUND(AVG(n_stops), 2),
       MIN(duration_minutes),
       MAX(duration_minutes)
FROM night_trips;

-- ============================================================================
-- FIN
-- ============================================================================
SELECT 'Schéma MySQL obrail_europe_db créé avec succès' AS message;
