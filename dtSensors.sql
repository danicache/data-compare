CREATE TABLE dtSensors (
    sensorid INTEGER,
    node_id INTEGER UNIQUE,
    sensorname TEXT,
    sensorlocation TEXT,
    sensornote TEXT,
    folder_id INTEGER,
    folder_template TEXT,
    folder_campaign TEXT,
    battery_replace_status TEXT,
    online INTEGER
);