-- ============================================================================
-- OBRAIL EUROPE — Schéma PostgreSQL
-- ============================================================================
-- SGBD    : PostgreSQL 14+
-- Base    : obrail_europe_db
-- Version : 1.0
-- Usage   : psql -U postgres -d obrail_europe_db -f 01_create_schema_postgres.sql
-- ============================================================================

-- Suppression dans l'ordre inverse des FK
DROP TABLE IF EXISTS etl_logs    CASCADE;
DROP TABLE IF EXISTS trips       CASCADE;
DROP TABLE IF EXISTS routes      CASCADE;
DROP TABLE IF EXISTS stops       CASCADE;
DROP TABLE IF EXISTS operators   CASCADE;
DROP TABLE IF EXISTS data_sources CASCADE;
DROP TABLE IF EXISTS countries   CASCADE;

-- ============================================================================
-- TABLE: countries
-- ============================================================================
CREATE TABLE countries (
    country_code CHAR(2)       PRIMARY KEY,
    country_name VARCHAR(100)  NOT NULL,
    timezone     VARCHAR(50)   DEFAULT 'Europe/Paris',
    created_at   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE countries IS 'Référentiel des pays européens du réseau ferroviaire';

-- ============================================================================
-- TABLE: data_sources
-- ============================================================================
CREATE TABLE data_sources (
    source_id       SERIAL        PRIMARY KEY,
    source_name     VARCHAR(100)  NOT NULL UNIQUE,
    source_type     VARCHAR(50)   NOT NULL,   -- API, CSV, GTFS, HTML_scraping
    source_url      VARCHAR(500),
    description     TEXT,
    license_type    VARCHAR(100),
    last_import_date TIMESTAMP,
    is_active       BOOLEAN       DEFAULT TRUE,
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON COLUMN data_sources.source_type IS 'Type: API, CSV, GTFS, HTML_scraping';

-- ============================================================================
-- TABLE: operators
-- ============================================================================
CREATE TABLE operators (
    operator_id             SERIAL        PRIMARY KEY,
    operator_code           VARCHAR(20)   NOT NULL UNIQUE,
    operator_name           VARCHAR(200)  NOT NULL,
    country_code            CHAR(2),
    website                 VARCHAR(300),
    is_night_train_operator BOOLEAN       DEFAULT FALSE,
    created_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_operator_country FOREIGN KEY (country_code)
        REFERENCES countries(country_code) ON DELETE SET NULL
);

-- ============================================================================
-- TABLE: stops
-- ============================================================================
CREATE TABLE stops (
    stop_id              SERIAL         PRIMARY KEY,
    external_stop_id     VARCHAR(100)   NOT NULL,
    stop_name            VARCHAR(300)   NOT NULL,
    stop_name_normalized VARCHAR(300),
    latitude             DECIMAL(10,6)  NOT NULL,
    longitude            DECIMAL(10,6)  NOT NULL,
    country_code         CHAR(2),
    city                 VARCHAR(200),
    is_major_station     BOOLEAN        DEFAULT FALSE,
    source_id            INTEGER,
    created_at           TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_stop_country FOREIGN KEY (country_code)
        REFERENCES countries(country_code) ON DELETE SET NULL,
    CONSTRAINT fk_stop_source FOREIGN KEY (source_id)
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    CONSTRAINT uq_stop_external UNIQUE (external_stop_id, source_id)
);

CREATE INDEX idx_stops_coordinates ON stops(latitude, longitude);
CREATE INDEX idx_stops_country     ON stops(country_code);
CREATE INDEX idx_stops_name        ON stops(stop_name_normalized);

-- ============================================================================
-- TABLE: routes
-- ============================================================================
CREATE TABLE routes (
    route_id          SERIAL        PRIMARY KEY,
    external_route_id VARCHAR(200)  NOT NULL,
    route_name        VARCHAR(500),
    route_type        INTEGER       NOT NULL,
    service_type      VARCHAR(50),   -- jour, nuit, regional, international
    operator_id       INTEGER,
    source_id         INTEGER,
    created_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_route_operator FOREIGN KEY (operator_id)
        REFERENCES operators(operator_id) ON DELETE SET NULL,
    CONSTRAINT fk_route_source FOREIGN KEY (source_id)
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    CONSTRAINT uq_route_external UNIQUE (external_route_id, source_id)
);

CREATE INDEX idx_routes_type         ON routes(route_type);
CREATE INDEX idx_routes_service_type ON routes(service_type);

-- ============================================================================
-- TABLE: trips  (table unifiée jour + nuit, contrairement au schéma MySQL
--               qui utilisait day_trips / night_trips séparées)
-- ============================================================================
CREATE TABLE trips (
    trip_id              SERIAL        PRIMARY KEY,
    external_trip_id     VARCHAR(300)  NOT NULL,
    external_service_id  VARCHAR(100),
    route_id             INTEGER,
    origin_stop_id       INTEGER       NOT NULL,
    destination_stop_id  INTEGER       NOT NULL,
    source_id            INTEGER,
    departure_minutes    INTEGER,
    arrival_minutes      INTEGER,
    duration_minutes     INTEGER,
    departure_hour       INTEGER,
    arrival_hour         INTEGER,
    n_stops              INTEGER       DEFAULT 2,
    distance_km          DECIMAL(10,2),
    is_night_trip        BOOLEAN       DEFAULT FALSE,
    is_international     BOOLEAN       DEFAULT FALSE,
    created_at           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_trip_route       FOREIGN KEY (route_id)
        REFERENCES routes(route_id) ON DELETE SET NULL,
    CONSTRAINT fk_trip_origin      FOREIGN KEY (origin_stop_id)
        REFERENCES stops(stop_id) ON DELETE CASCADE,
    CONSTRAINT fk_trip_destination FOREIGN KEY (destination_stop_id)
        REFERENCES stops(stop_id) ON DELETE CASCADE,
    CONSTRAINT fk_trip_source      FOREIGN KEY (source_id)
        REFERENCES data_sources(source_id) ON DELETE SET NULL,
    CONSTRAINT uq_trip_external    UNIQUE (external_trip_id, source_id)
);

CREATE INDEX idx_trips_origin        ON trips(origin_stop_id);
CREATE INDEX idx_trips_destination   ON trips(destination_stop_id);
CREATE INDEX idx_trips_night         ON trips(is_night_trip);
CREATE INDEX idx_trips_international ON trips(is_international);
CREATE INDEX idx_trips_route         ON trips(route_id);
CREATE INDEX idx_trips_departure     ON trips(departure_hour);
CREATE INDEX idx_trips_duration      ON trips(duration_minutes);

-- ============================================================================
-- TABLE: etl_logs
-- La colonne train_type est ajoutée pour maintenir la parité avec MySQL.
-- ============================================================================
CREATE TABLE etl_logs (
    log_id            SERIAL       PRIMARY KEY,
    job_name          VARCHAR(100) NOT NULL,
    source_id         INTEGER,
    train_type        VARCHAR(20),              -- 'jour', 'nuit' (parité MySQL)
    status            VARCHAR(20)  NOT NULL,    -- SUCCESS, FAILURE, WARNING
    started_at        TIMESTAMP    NOT NULL,
    ended_at          TIMESTAMP,
    records_processed INTEGER      DEFAULT 0,
    records_inserted  INTEGER      DEFAULT 0,
    records_updated   INTEGER      DEFAULT 0,
    records_rejected  INTEGER      DEFAULT 0,
    error_message     TEXT,
    details           JSONB,
    CONSTRAINT fk_etl_source FOREIGN KEY (source_id)
        REFERENCES data_sources(source_id) ON DELETE SET NULL
);

CREATE INDEX idx_etl_logs_job    ON etl_logs(job_name);
CREATE INDEX idx_etl_logs_status ON etl_logs(status);
CREATE INDEX idx_etl_logs_date   ON etl_logs(started_at);

-- ============================================================================
-- VUES ANALYTIQUES
-- ============================================================================

CREATE OR REPLACE VIEW v_country_statistics AS
SELECT
    c.country_code,
    c.country_name,
    COUNT(DISTINCT s.stop_id)                                          AS total_stops,
    COUNT(DISTINCT t.trip_id)                                          AS total_trips,
    COUNT(DISTINCT CASE WHEN t.is_night_trip THEN t.trip_id END)       AS night_trips,
    COUNT(DISTINCT CASE WHEN NOT t.is_night_trip THEN t.trip_id END)   AS day_trips,
    ROUND(AVG(t.duration_minutes)::numeric, 2)                         AS avg_duration_minutes
FROM countries c
LEFT JOIN stops s ON c.country_code = s.country_code
LEFT JOIN trips t ON t.origin_stop_id = s.stop_id
              OR t.destination_stop_id = s.stop_id
GROUP BY c.country_code, c.country_name;

COMMENT ON VIEW v_country_statistics IS 'Statistiques agrégées par pays';

CREATE OR REPLACE VIEW v_day_night_comparison AS
SELECT
    CASE WHEN is_night_trip THEN 'Nuit' ELSE 'Jour' END AS trip_type,
    COUNT(*)                                              AS total_trips,
    ROUND(AVG(duration_minutes)::numeric, 2)              AS avg_duration,
    ROUND(AVG(n_stops)::numeric, 2)                       AS avg_stops,
    MIN(duration_minutes)                                 AS min_duration,
    MAX(duration_minutes)                                 AS max_duration
FROM trips
GROUP BY is_night_trip;

COMMENT ON VIEW v_day_night_comparison IS 'Comparaison trains de jour vs trains de nuit';

CREATE OR REPLACE VIEW v_top_connections AS
SELECT
    so.stop_name       AS origin_name,
    sd.stop_name       AS destination_name,
    so.country_code    AS origin_country,
    sd.country_code    AS dest_country,
    COUNT(*)           AS trip_count,
    ROUND(AVG(t.duration_minutes)::numeric, 2) AS avg_duration
FROM trips t
JOIN stops so ON t.origin_stop_id       = so.stop_id
JOIN stops sd ON t.destination_stop_id  = sd.stop_id
GROUP BY so.stop_name, sd.stop_name, so.country_code, sd.country_code
ORDER BY trip_count DESC;

COMMENT ON VIEW v_top_connections IS 'Classement des liaisons les plus fréquentes';

-- ============================================================================
-- FIN — Vérification
-- ============================================================================
SELECT 'Schéma PostgreSQL obrail_europe_db créé avec succès' AS message;
