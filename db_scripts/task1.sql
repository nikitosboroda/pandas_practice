CREATE SCHEMA IF NOT EXISTS first_task;

CREATE TABLE first_task.department (
    department_id INTEGER PRIMARY KEY,
    department_name VARCHAR(20)
);

CREATE TABLE first_task.employee (
    rid VARCHAR(6) PRIMARY KEY,
    version INTEGER,
    name VARCHAR(20),
    created TIMESTAMP,
    department_id INTEGER REFERENCES first_task.department
);

CREATE TABLE first_task.synonyms (
    department_synonyms TEXT,
    department_id INTEGER REFERENCES first_task.department
);