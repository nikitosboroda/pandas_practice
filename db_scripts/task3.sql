CREATE SCHEMA IF NOT EXISTS third_task;

CREATE TABLE third_task.employee (
    rid VARCHAR(6) PRIMARY KEY,
    version INTEGER,
    name VARCHAR(20),
    created TIMESTAMP,
    department_1 VARCHAR,
    extra__field_1 VARCHAR,
    comment VARCHAR,
    extra__field_2 VARCHAR,
    department VARCHAR,
    dep VARCHAR,
    extra__field_3 VARCHAR
);
