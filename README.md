

# ETL Project Documentation

This repository contains an ETL (Extract, Transform, Load) project that aims to process data from three different sources and load it into a data warehouse. The project utilizes Python, SQL, and the Airflow pipeline orchestrator.

This is a prototype of a real project that uses dummy data to exemplify how to perform ETL using Airflow.

## Project Requirements

The ETL process involves the following data sources:

- API with employee data
- .PARQUET file with category data
- PostgreSQL relational table with sales data

The chosen destination for the processed data is a Star Schema data warehouse using PostgreSQL. The ETL process is scheduled to run daily. Airflow is used as the pipeline orchestrator. The implementation is primarily done in Python, with some SQL statements.

## Environment Preparation

**Operating System (OS):**

Ubuntu 22.04

**Python:**

For this project, the open-source docker image created by Israel Gonçalves de Oliveira was used as the workspace: [ysraell/my-ds](https://github.com/ysraell/my-ds). It is based on the official Python docker image: [python](https://hub.docker.com/_/python).

- Base Image Tag: 3.10-bullseye
- Jupyter Lab was used as the IDE, and the .ipynb files were converted to .py using Jupyter nbconvert.

**Airflow:**

Airflow version 2.6.1 was installed inside the Docker image using the command `pip install airflow` in the terminal.

**PostgreSQL Data Warehouse:**

The official PostgreSQL Docker image was used locally: [postgres](https://hub.docker.com/_/postgres).

**DBeaver:**

DBeaver was used as the SQL IDE.

## Data Warehouse Modeling

The data warehouse follows a star schema modeling approach. The entity-relationship diagram (ERD) for the data warehouse is as follows:

![ERD](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/8297560f-76b9-42d5-8e87-80111ea7df45/Untitled.png)

The fact table is named **vendas** (sales), and the dimensional tables are **categoria** (category), **funcionarios** (employees), and **calendario** (calendar).

The SQL code to create the tables in the **dw_projeto** schema is as follows:

```sql
CREATE SCHEMA dw_projeto;

CREATE TABLE dw_projeto.vendas (
    venda REAL,
    id_func INTEGER,
    id_cat INTEGER,
    data DATE,
    id_venda INTEGER
);

CREATE TABLE dw_projeto.funcionarios (
    funcionario VARCHAR(50),
    id_func INTEGER
);

CREATE TABLE dw_projeto.categoria (
    categoria VARCHAR(50),
    id_cat INTEGER
);

CREATE TABLE dw_projeto.calendario (
    data DATE,
    dia INT,
    mes INT,
    ano INT,
    bimestre INT
);
