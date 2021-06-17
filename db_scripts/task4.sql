CREATE SCHEMA IF NOT EXISTS forth_task;

CREATE TABLE forth_task.departments (
    version INTEGER,
    name VARCHAR PRIMARY KEY
);

CREATE TABLE forth_task.subdepartments (
    name VARCHAR REFERENCES forth_task.departments,
    name_sub VARCHAR,
    desc_sub VARCHAR,
    order_ INTEGER,
    PRIMARY KEY (name, name_sub)
);

--CREATE TABLE forth_task.sub_dep (
--    sub_dep_name VARCHAR,
--    department_name VARCHAR,
--    CONSTRAINT "FK_sub_dep_name" FOREIGN KEY (sub_dep_name) REFERENCES forth_task.subdepartments (name),
--    CONSTRAINT "FK_department_name" FOREIGN KEY (department_name) REFERENCES forth_task.departments (name)
--);
--
--CREATE UNIQUE INDEX "subdep_dep_name" ON forth_task.sub_dep USING btree (sub_dep_name, department_name);