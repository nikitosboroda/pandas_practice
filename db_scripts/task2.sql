CREATE SCHEMA IF NOT EXISTS second_task;

CREATE TABLE second_task.department (
    department_id BIGINT PRIMARY KEY,
    name VARCHAR(20),
    comment VARCHAR,
    version INTEGER
);

CREATE TABLE second_task.employee (
    employee_id BIGINT PRIMARY KEY,
    version INTEGER,
    name VARCHAR(20),
    created TIMESTAMP,
    department_1 BIGINT REFERENCES second_task.department (department_id),
    department_2 BIGINT REFERENCES second_task.department (department_id),
    department_3 BIGINT REFERENCES second_task.department (department_id),
    comment VARCHAR
);
