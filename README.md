# Análisis de Datos Meteorológicos

> Proyecto final — Big Data · Grado en Matemáticas · UNIE Universidad

[![CI](https://github.com/alvaroinclan/proyecto_meteorologia/actions/workflows/ci.yml/badge.svg)](https://github.com/alvaroinclan/proyecto_meteorologia/actions/workflows/ci.yml)
[![Docs](https://github.com/alvaroinclan/proyecto_meteorologia/actions/workflows/docs.yml/badge.svg)](https://alvaroinclan.github.io/proyecto_meteorologia/)
[![Coverage](https://codecov.io/gh/alvaroinclan/proyecto_meteorologia/graph/badge.svg)](https://codecov.io/gh/alvaroinclan/proyecto_meteorologia)
[![Version](https://img.shields.io/github/v/release/alvaroinclan/proyecto_meteorologia)](https://github.com/alvaroinclan/proyecto_meteorologia/releases)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

---

## Descripción del Proyecto

Este repositorio contiene el proyecto final para la asignatura de Big Data. El objetivo principal (Línea 4) es la **Evaluación del potencial eólico a partir de datos de viento**, simulando un escenario de procesamiento masivo.

El proyecto se estructura en distintas fases de análisis.

### Fase 1: Lectura e Ingestión de Datos (Completada)
En esta fase nos hemos centrado en la lectura y procesamiento por lotes (*batch*) de un archivo GRIB proveniente de Copernicus Climate Data Store (ERA5):
- **Mallado de Localizaciones:** Se generan 50 localizaciones geográficas ubicadas en el Norte de España, usando el mallado estándar de ECMWF (resolución 0.1°).
- **Procesamiento de Archivos GRIB (`xarray` + `cfgrib`):** Se lee el archivo GRIB de ERA5, que almacena internamente dos conjuntos de datos separados (vientos regulares y rachas), y se realiza una interpolación iterativa para acotar los datos a las coordenadas generadas.
- **Cálculo Vectorial:** Empleando un enfoque *Big Data* de alto rendimiento con `Polars`, se extraen las componentes $u$ y $v$ del viento (a 10 y 100 metros) y se aplican operaciones vectoriales para derivar los módulos de velocidad absoluta y las direcciones en grados (0-360º).
- **Persistencia en Parquet:** Por razones de eficiencia y compresión, el marco de datos final de las 50 localizaciones para todas las horas del año se almacena en `data/staging/all_stations.parquet`.
- **Testing (`pytest`):** Se han desarrollado tests unitarios validando la integridad geométrica de la malla, el aislamiento en el parseo (`mocking`) y la veracidad física de los datos extraídos (ej. $ws \ge 0$).

## Documentation

Full documentation at **[alvaroinclan.github.io/proyecto_meteorologia](https://alvaroinclan.github.io/proyecto_meteorologia/)**

## Installation

  ```bash
  git clone https://github.com/alvaroinclan/proyecto_meteorologia.git
  cd proyecto_meteorologia
  pip install uv
  uv sync --group dev
  ```

## Data Download

Data is not included in the repository. To download:

  ```bash
  # TODO: add your data download instructions
  ```

## Usage

  ```bash
  uv run pytest                          # run tests
  uv run pytest --cov=src -v     # tests with coverage
  uv run ruff check .                    # lint
  uv run ruff format .                   # format
  uv run mkdocs serve                    # preview docs at localhost:8000
  ```

## Project Structure

  ```
  proyecto_meteorologia/
  ├── .github/workflows/   # CI/CD pipelines
  ├── data/                # Data files (not committed — see .gitignore)
  ├── docs/                # MkDocs documentation sources
  ├── notebooks/           # Exploratory notebooks
  ├── src/weather/         # Source package
  ├── tests/               # Unit and integration tests
  ├── mkdocs.yml
  ├── pyproject.toml
  └── README.md
  ```

## Author

**Álvaro Inclán** · [github.com/alvaroinclan](https://github.com/alvaroinclan)

## Professor
**Álvaro Diez** · [github.com/alvarodiez20](https://github.com/alvarodiez20)

---

*Big Data · 4º Grado en Matemáticas · UNIE Universidad · 2025–2026*