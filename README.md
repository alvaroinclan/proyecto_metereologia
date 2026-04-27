# Análisis de Datos Meteorológicos

[![CI](https://github.com/alvaroinclan/proyecto_meteorologia/actions/workflows/ci.yml/badge.svg)](https://github.com/alvaroinclan/proyecto_meteorologia/actions/workflows/ci.yml)
[![Docs](https://github.com/alvaroinclan/proyecto_meteorologia/actions/workflows/docs.yml/badge.svg)](https://alvaroinclan.github.io/proyecto_meteorologia/)
[![Coverage](https://codecov.io/gh/alvaroinclan/proyecto_meteorologia/graph/badge.svg)](https://codecov.io/gh/alvaroinclan/proyecto_meteorologia)
[![Version](https://img.shields.io/github/v/release/alvaroinclan/proyecto_meteorologia)](https://github.com/alvaroinclan/proyecto_meteorologia/releases)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

<div align="center">
  <p>
    <strong>Proyecto final — Big Data · Grado en Matemáticas · UNIE Universidad</strong>
  </p>

  <p>
    <a href="#descripción-del-proyecto">Acerca de</a> •
    <a href="https://alvaroinclan.github.io/proyecto_meteorologia/">Documentación</a> •
    <a href="#installation">Instalación</a>
  </p>
</div>

---

## Descripción del Proyecto

Evaluación del potencial eólico a partir de datos de viento en la península ibérica. El proyecto procesa datos horarios ERA5 (Copernicus Climate Data Store) de 50 localizaciones en el norte de España, aplicando técnicas de Big Data para el análisis masivo de velocidad y dirección del viento: interpolación espacial en mallado ECMWF 0.1°, cálculo vectorial de módulos, ajuste de distribuciones de Weibull y estimación de producción energética anual (AEP).

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