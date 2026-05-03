-- AeroStream Reference Schema
-- Tables: circuits, teams, drivers, regulations
-- Loaded automatically by PostgreSQL on first container start via /docker-entrypoint-initdb.d

CREATE TABLE IF NOT EXISTS circuits (
    circuit_id       VARCHAR(50)  PRIMARY KEY,
    circuit_name     VARCHAR(100) NOT NULL,
    country          VARCHAR(50)  NOT NULL,
    city             VARCHAR(50),
    length_km        DECIMAL(6,3),
    num_laps         INT,
    drs_zones        INT,
    pit_loss_time_s  DECIMAL(5,2),
    overtaking_diff  VARCHAR(20),
    created_at       TIMESTAMP DEFAULT NOW(),
    updated_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS teams (
    team_id      VARCHAR(50)  PRIMARY KEY,
    team_name    VARCHAR(100) NOT NULL,
    power_unit   VARCHAR(50),
    base_country VARCHAR(50),
    principal    VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id          VARCHAR(50)  PRIMARY KEY,
    full_name          VARCHAR(100) NOT NULL,
    abbreviated_name   VARCHAR(3),
    team_id            VARCHAR(50)  REFERENCES teams(team_id),
    car_number         INT,
    nationality        VARCHAR(50),
    championships      INT DEFAULT 0,
    race_wins          INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS regulations (
    regulation_id              VARCHAR(50)  PRIMARY KEY,
    season                     INT,
    max_fuel_load_kg           DECIMAL(5,2),
    min_weight_kg              DECIMAL(6,2),
    drs_activation_gap_s       DECIMAL(4,2),
    mandatory_compounds        INT,
    pit_lane_speed_limit_kph   INT,
    fastest_lap_bonus_point    BOOLEAN
);
