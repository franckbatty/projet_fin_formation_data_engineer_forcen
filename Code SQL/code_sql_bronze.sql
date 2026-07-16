-- Code de création du schema
CREATE SCHEMA macrobus;

-- Code de Creation des tables --
CREATE TABLE macrobus.vehicules (
    year INT,
    make VARCHAR(100),
    model VARCHAR(150),
    trim VARCHAR(150), 
    body VARCHAR(100),
    transmission VARCHAR(50),
    vin VARCHAR(10),
    state SMALLINT, 
    condition INTEGER,
    odometer VARCHAR(50),
    color VARCHAR(50),
    interior VARCHAR(255),
    seller NUMERIC(12,2),
    mmr NUMERIC(12,2),
    saledate DATE
);