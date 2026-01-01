-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- TERRAIN SNAPSHOTS
-- ============================================================================
CREATE TABLE terrain_snapshots (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version_name VARCHAR(100) NOT NULL,
    dem_path TEXT NOT NULL,
    dtm_path TEXT,
    ortho_path TEXT,
    resolution_m FLOAT NOT NULL,
    epsg_code INTEGER NOT NULL,
    extent GEOMETRY(POLYGON, 4326) NOT NULL,
    source VARCHAR(50) NOT NULL,  -- 'baseline', 'sentinel2', 'lidar', etc.
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_version UNIQUE (version_name)
);

CREATE INDEX idx_terrain_timestamp ON terrain_snapshots(timestamp);
CREATE INDEX idx_terrain_extent ON terrain_snapshots USING GIST(extent);

-- ============================================================================
-- WEATHER OBSERVATIONS
-- ============================================================================
CREATE TABLE weather_observations (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    location GEOMETRY(POINT, 4326) NOT NULL,
    rainfall_mm FLOAT,
    intensity_mm_hr FLOAT,
    temperature_c FLOAT,
    humidity_pct FLOAT,
    wind_speed_ms FLOAT,
    source VARCHAR(50) NOT NULL,  -- 'openweathermap', 'rain_gauge', etc.
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Convert to TimescaleDB hypertable for efficient time-series queries
SELECT create_hypertable('weather_observations', 'timestamp');

CREATE INDEX idx_weather_location ON weather_observations USING GIST(location);

-- ============================================================================
-- RAINFALL EVENTS
-- ============================================================================
CREATE TABLE rainfall_events (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    duration_minutes INTEGER,
    total_rainfall_mm FLOAT,
    max_intensity_mm_hr FLOAT,
    avg_intensity_mm_hr FLOAT,
    threshold_exceeded BOOLEAN DEFAULT FALSE,
    trigger_probability FLOAT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rainfall_start ON rainfall_events(start_time);
CREATE INDEX idx_rainfall_active ON rainfall_events(is_active);

-- ============================================================================
-- CHANGE DETECTION RESULTS
-- ============================================================================
CREATE TABLE change_detections (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    baseline_snapshot_id INTEGER REFERENCES terrain_snapshots(id),
    comparison_snapshot_id INTEGER REFERENCES terrain_snapshots(id),
    dod_raster_path TEXT NOT NULL,
    total_erosion_m3 FLOAT,
    total_deposition_m3 FLOAT,
    net_change_m3 FLOAT,
    max_erosion_m FLOAT,
    max_deposition_m FLOAT,
    change_area_m2 FLOAT,
    lod_threshold_m FLOAT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_change_timestamp ON change_detections(timestamp);

-- ============================================================================
-- SIMULATION RUNS
-- ============================================================================
CREATE TABLE simulation_runs (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    terrain_snapshot_id INTEGER REFERENCES terrain_snapshots(id),
    rainfall_event_id INTEGER REFERENCES rainfall_events(id),
    trigger_type VARCHAR(50) NOT NULL,  -- 'manual', 'threshold_exceeded', 'scheduled'
    model_name VARCHAR(50) NOT NULL,  -- 'flowr', 'ravaflow', etc.
    model_version VARCHAR(20),
    parameters JSONB,
    status VARCHAR(20) NOT NULL DEFAULT 'running',  -- 'running', 'completed', 'failed'
    output_path TEXT,
    runout_area_m2 FLOAT,
    max_runout_distance_m FLOAT,
    affected_volume_m3 FLOAT,
    max_velocity_ms FLOAT,
    computation_time_s FLOAT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_simulation_timestamp ON simulation_runs(timestamp);
CREATE INDEX idx_simulation_status ON simulation_runs(status);

-- ============================================================================
-- RISK ZONES (Spatial risk assessment results)
-- ============================================================================
CREATE TABLE risk_zones (
    id SERIAL PRIMARY KEY,
    simulation_run_id INTEGER REFERENCES simulation_runs(id),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    geometry GEOMETRY(POLYGON, 4326) NOT NULL,
    risk_level VARCHAR(20) NOT NULL,  -- 'low', 'medium', 'high', 'extreme'
    risk_value FLOAT NOT NULL CHECK (risk_value >= 0 AND risk_value <= 1),
    trigger_probability FLOAT,
    runout_probability FLOAT,
    flow_intensity FLOAT,
    affected_area_m2 FLOAT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_risk_timestamp ON risk_zones(timestamp);
CREATE INDEX idx_risk_geometry ON risk_zones USING GIST(geometry);
CREATE INDEX idx_risk_level ON risk_zones(risk_level);

-- ============================================================================
-- ALERTS
-- ============================================================================
CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_type VARCHAR(50) NOT NULL,  -- 'threshold_exceeded', 'high_risk', 'terrain_change'
    severity VARCHAR(20) NOT NULL,  -- 'info', 'warning', 'critical'
    title VARCHAR(200) NOT NULL,
    message TEXT NOT NULL,
    related_simulation_id INTEGER REFERENCES simulation_runs(id),
    related_event_id INTEGER REFERENCES rainfall_events(id),
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by VARCHAR(100),
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alert_timestamp ON alerts(timestamp);
CREATE INDEX idx_alert_severity ON alerts(severity);
CREATE INDEX idx_alert_acknowledged ON alerts(acknowledged);

-- ============================================================================
-- SOURCE AREAS (Debris flow initiation zones)
-- ============================================================================
CREATE TABLE source_areas (
    id SERIAL PRIMARY KEY,
    terrain_snapshot_id INTEGER REFERENCES terrain_snapshots(id),
    geometry GEOMETRY(POLYGON, 4326) NOT NULL,
    method VARCHAR(50) NOT NULL,  -- 'slope_threshold', 'contributing_area', 'ml_model'
    susceptibility FLOAT CHECK (susceptibility >= 0 AND susceptibility <= 1),
    slope_deg FLOAT,
    contributing_area_m2 FLOAT,
    material_availability FLOAT,  -- from change detection
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_source_geometry ON source_areas USING GIST(geometry);
CREATE INDEX idx_source_terrain ON source_areas(terrain_snapshot_id);