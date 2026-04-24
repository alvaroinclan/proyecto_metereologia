# Evaluación del Potencial Eólico a partir de Datos de Viento

> Proyecto Final — Big Data · 4º Grado en Matemáticas · UNIE Universidad

Procesamiento masivo de datos de velocidad y dirección del viento para evaluar el potencial de generación eólica en diferentes localizaciones de España. Trabajaremos con datos horarios de alta frecuencia, ajustaremos distribuciones de Weibull a las series de viento, estimaremos la producción energética teórica con curvas de potencia de aerogeneradores reales, y crearemos un ranking de localizaciones.

## Sobre Este Proyecto

En el contexto de la transición energética, evaluar el potencial eólico de forma precisa es fundamental para la planificación de parques eólicos. Este proyecto aplica técnicas de **Big Data** para procesar datos meteorológicos masivos y extraer conclusiones sobre el recurso eólico en la península ibérica.

### Objetivos Específicos

1. Descargar datos horarios de viento de al menos 50 estaciones y/o datos ERA5
2. Realizar control de calidad específico para datos de viento (calm corrections, sector consistency)
3. Ajustar distribuciones de Weibull por estación y analizar variabilidad estacional
4. Implementar el cálculo de producción energética anual (AEP) usando curvas de potencia reales
5. Generar rosas de viento interactivas y mapas de potencial eólico

### Reto Big Data

Datos horarios de alta frecuencia (alto volumen), ajuste de distribuciones por máxima verosimilitud, cálculo vectorial para dirección del viento.

### Fuentes de Datos

- **ERA5** (Copernicus Climate Data Store): Fuente principal de datos de reanálisis
- **AEMET OpenData**: Datos horarios de estaciones meteorológicas
- **Global Wind Atlas**: Validación de resultados

## Herramientas Utilizadas

- **Python 3.10+**: Lenguaje principal del proyecto
- **uv**: Gestor de paquetes ultrarrápido
- **xarray + cfgrib**: Lectura y manipulación de archivos GRIB (ERA5)
- **scipy**: Interpolación espacial de datos en mallado
- **polars**: DataFrames de alto rendimiento para procesamiento masivo
- **pyarrow / Parquet**: Almacenamiento columnar eficiente
- **numpy**: Cálculo numérico y operaciones vectoriales
- **pytest**: Testing unitario y de integración
- **ruff**: Linting y formateo de código
- **MkDocs (Material)**: Generación de documentación

## Fases del Proyecto

### Fase 1: Lectura e Ingestión de Datos ✅

En esta fase nos centramos en la lectura y procesamiento por lotes (*batch*) de un archivo GRIB proveniente de Copernicus Climate Data Store (ERA5).

#### 1. Datos de Entrada

El archivo `data/raw/data.grib` contiene datos horarios de la península ibérica para el año 2025, con las siguientes variables de viento:

| Variable | Descripción |
|----------|-------------|
| `u10`, `v10` | Componentes u/v del viento a 10 m |
| `u100`, `v100` | Componentes u/v del viento a 100 m |
| `u10n`, `v10n` | Componentes u/v del viento neutral a 10 m |
| `fg10` | Racha de viento a 10 m desde el último post-procesamiento |
| `i10fg` | Racha de viento instantánea a 10 m |

El archivo GRIB contiene internamente dos conjuntos de datos separados:

- **Dataset horario** (8760 timestamps): variables `u10`, `v10`, `u100`, `v100`, `u10n`, `v10n`
- **Dataset de rachas** (731 × 12 steps): variables `fg10`, `i10fg`

#### 2. Mallado de Localizaciones

Se generan **50 localizaciones geográficas** ubicadas en el **Norte de España**, usando el mallado estándar de ECMWF con resolución 0.1°:

- **Latitudes**: de 42.8°N a 43.2°N (5 valores, paso 0.1°)
- **Longitudes**: de 6.0°W a 5.1°W (10 valores, paso 0.1°)
- **Región cubierta**: zona de Asturias / Cantabria / León

Dado que el archivo GRIB original tiene resolución 0.25°, se aplica **interpolación lineal** (vía `scipy`) para obtener los valores en los nodos exactos del mallado de 0.1°.

#### 3. Procesamiento en Lotes (Batch)

Para simular un entorno de procesamiento Big Data y evitar desbordamiento de memoria:

- Las 50 localizaciones se dividen en **lotes de 10**
- Cada lote se interpola y convierte de forma independiente
- Los resultados parciales se concatenan al final

```
Lote 1: station_0 … station_9   → interpolación → DataFrame parcial
Lote 2: station_10 … station_19 → interpolación → DataFrame parcial
…
Lote 5: station_40 … station_49 → interpolación → DataFrame parcial
                                                    ↓
                                              pl.concat(lotes)
```

#### 4. Cálculo Vectorial

A partir de las componentes $u$ y $v$ del viento, se derivan:

- **Velocidad absoluta**: $ws = \sqrt{u^2 + v^2}$
- **Dirección del viento**: $wd = \left(\frac{180}{\pi} \cdot \arctan2(u, v) + 180\right) \mod 360$

Se calculan tanto para 10 m (`ws10`, `wd10`) como para 100 m (`ws100`, `wd100`).

#### 5. Persistencia

El DataFrame final se almacena en formato **Apache Parquet** en `data/staging/all_stations.parquet`, garantizando:

- Compresión eficiente del volumen de datos
- Lectura rápida columnar para las fases posteriores
- Tipado estricto de columnas

#### 6. Testing

Se implementan tests con `pytest` que validan:

| Test | Qué verifica |
|------|-------------|
| `test_generate_target_locations` | 50 puntos únicos, dentro de los límites geográficos del norte de España |
| `test_process_dataset_chunk` | Interpolación correcta con un dataset `xarray` sintético (mock), columnas esperadas presentes, columnas auxiliares eliminadas |
| `test_load_grib_data_in_batches` | Lectura completa del GRIB, existencia de columnas vectoriales, $ws \ge 0$, $0 \le wd \le 360$ |
| `test_generated_parquet` | Integridad del Parquet generado: 50 estaciones únicas, validaciones físicas, ausencia de columnas completamente nulas |

#### 7. Estructura de Código

```
src/weather/
├── data/
│   ├── __init__.py
│   └── load.py              # generate_target_locations(), process_dataset_chunk(), load_grib_data_in_batches()
└── pipelines/
    ├── __init__.py
    └── ingest.py             # run_ingestion() → lee GRIB y genera Parquet

tests/
└── test_load.py              # 4 tests unitarios y de integración
```

### Fase 2: Control de Calidad 🔲

*Próximamente*: Calm corrections, sector consistency, detección de outliers.

### Fase 3: Distribuciones de Weibull 🔲

*Próximamente*: Ajuste por estación, variabilidad estacional.

### Fase 4: Producción Energética (AEP) 🔲

*Próximamente*: Curvas de potencia reales, ranking de localizaciones.

### Fase 5: Visualización 🔲

*Próximamente*: Rosas de viento interactivas, mapas de potencial eólico.

## Alumno

**Álvaro Inclán** · [github.com/alvaroinclan](https://github.com/alvaroinclan)

## Profesor

**Álvaro Diez** · [github.com/alvarodiez20](https://github.com/alvarodiez20)

---

*Big Data · 4º Grado en Matemáticas · UNIE Universidad · 2025–2026*