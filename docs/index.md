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

### Fase 2: Control de Calidad ✅

En esta fase se implementa un control de calidad específico para datos de viento, siguiendo las recomendaciones de la Organización Meteorológica Mundial (OMM). Se aplican dos técnicas complementarias: **calm corrections** y **sector consistency**.

#### 1. Calm Corrections (Corrección de Calmas)

Cuando la velocidad del viento cae por debajo de un umbral configurable (por defecto 0.5 m/s, criterio OMM), la dirección del viento asociada no tiene significado físico — la veleta no se orienta con vientos tan débiles. En estos casos:

- La dirección del viento (`wd10`, `wd100`) se establece a `null`
- Se añade una columna booleana `is_calm_10` / `is_calm_100` para facilitar el filtrado posterior

| Nivel de altura | Calmas detectadas | Porcentaje |
|-----------------|-------------------|------------|
| 10 m            | 16 497            | 3.77 %     |
| 100 m           | 5 283             | 1.21 %     |

> Las calmas son más frecuentes a 10 m que a 100 m, lo cual es coherente con el perfil logarítmico del viento: a mayor altura, mayor velocidad media.

#### 2. Sector Consistency (Consistencia Sectorial)

La rosa de vientos se divide en $N$ sectores iguales (por defecto 12, de 30° cada uno). Para cada estación se calcula la frecuencia relativa de direcciones por sector y se compara con la distribución uniforme esperada ($1/N$):

- **Ratio de desviación** por sector: $r_i = f_i \cdot N$, donde $f_i$ es la frecuencia observada en el sector $i$
- **Chi-cuadrado de Pearson**: $\chi^2 = N_{\mathrm{obs}} \cdot \sum_{i=1}^{N} \frac{(f_i - 1/N)^2}{1/N}$
- **Flagged**: una estación se marca como potencialmente problemática si algún sector tiene $r_i > 3.0$

| Nivel de altura | Estaciones flagged | Estación | $\chi^2$ | Max desviación |
|-----------------|--------------------|----------|----------|----------------|
| 10 m            | 1                  | `station_25` | 6 314.1 | 3.03 |
| 100 m           | 0                  | — | — | — |

> `station_25` presenta una distribución sectorial ligeramente sesgada a 10 m (ratio máximo 3.03), lo que puede indicar efectos de orografía local. A 100 m la distribución se normaliza.

#### 3. Tratamiento de Datos Faltantes

De las 50 localizaciones originales, **20 estaciones** (station_30 a station_49) tienen datos completamente nulos — se encuentran en el borde del dominio del archivo GRIB y la interpolación lineal no puede extrapolar. El pipeline las trata correctamente:

- `is_calm` = `null` (no se puede determinar si es calma sin dato de velocidad)
- Excluidas automáticamente del análisis de consistencia sectorial

#### 4. Datos de Salida

El pipeline genera dos archivos Parquet en `data/clean/`:

| Archivo | Contenido | Columnas añadidas |
|---------|-----------|-------------------|
| `all_stations_qc.parquet` | DataFrame corregido (438 000 filas × 18 cols) | `is_calm_10`, `is_calm_100` |
| `station_qc_flags.parquet` | Flags de QC por estación (30 filas × 9 cols) | `chi2_*`, `max_sector_deviation_*`, `flagged_*` |

#### 5. Testing

Se implementan **28 tests unitarios** organizados en 4 clases:

| Clase de test | Tests | Qué verifica |
|---------------|-------|-------------|
| `TestApplyCalmCorrections` | 10 | Nullificación de dirección en calmas, preservación de no-calmas, flag `is_calm`, umbrales extremos (0 y ∞), validación de errores (threshold negativo, columnas ausentes), inmutabilidad del DataFrame original, nombres de columna personalizados |
| `TestComputeSectorFrequencies` | 6 | Distribución uniforme perfecta → freq = 1/12, rango de sectores [0, N), suma de frecuencias = 1 por estación, exclusión de direcciones nulas, wrap 360° → sector 0, sectores personalizados (4 cuadrantes) |
| `TestFlagSectorInconsistencies` | 5 | Distribución uniforme no flagged ($\chi^2 \approx 0$), distribución sesgada flagged ($\chi^2 \gg 0$), ratio máximo de desviación, tolerancia alta desactiva flag, multi-estación |
| `TestRunQualityControl` | 7 | Retorno de dos DataFrames, columnas esperadas en flags, preservación de filas (sin eliminación), todas las estaciones reciben flags, parámetros personalizados, compatibilidad con 100 m |

#### 6. Estructura de Código

```
src/weather/
├── data/
│   ├── __init__.py
│   ├── load.py              # (Fase 1) Ingestión GRIB
│   └── quality.py           # apply_calm_corrections(), compute_sector_frequencies(),
│                             # flag_sector_inconsistencies(), run_quality_control()
└── pipelines/
    ├── __init__.py
    ├── ingest.py             # (Fase 1) run_ingestion()
    └── qc.py                 # run_qc() → lee Parquet staging, aplica QC, genera salida

tests/
├── test_load.py              # (Fase 1) 4 tests
└── test_quality.py           # 28 tests de control de calidad
```

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