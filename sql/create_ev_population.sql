CREATE TABLE IF NOT EXISTS ev_population (
    vin_prefix VARCHAR(10),
    county VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(10),
    postal_code VARCHAR(20),
    model_year INT,
    make VARCHAR(100),
    model VARCHAR(100),
    ev_type VARCHAR(100),
    cafv_eligibility VARCHAR(200),
    electric_range INT,
    base_msrp NUMERIC,
    legislative_district INT,
    dol_vehicle_id BIGINT PRIMARY KEY,
    vehicle_location TEXT,
    electric_utility TEXT,
    census_tract BIGINT
);
