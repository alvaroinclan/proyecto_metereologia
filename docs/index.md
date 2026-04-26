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

#### 6. Resultados e Interpretación

El pipeline de ingestión procesa con éxito el archivo GRIB completo y genera un DataFrame con las siguientes características:

| Métrica | Valor |
|---------|-------|
| Filas totales | 438 000 |
| Columnas | 16 (time, station, u10, v10, u100, v100, u10n, v10n, fg10, i10fg, ws10, wd10, ws100, wd100) |
| Estaciones únicas | 50 |
| Cobertura temporal | 1 año completo (8 760 horas) |
| Estaciones con datos válidos | 30 (station_0 a station_29) |
| Estaciones sin datos | 20 (station_30 a station_49) |

**Interpretación:**

- El dataset resultante contiene **438 000 registros** (50 estaciones × 8 760 horas), un volumen representativo del reto Big Data propuesto.
- Las **20 estaciones sin datos** (station_30 a station_49) corresponden a puntos del mallado de 0.1° que caen fuera del dominio espacial del archivo GRIB original (resolución 0.25°). La interpolación lineal no puede extrapolar más allá del borde del dominio, por lo que estos puntos son `null`. Este comportamiento es correcto y esperable.
- La velocidad del viento a **100 m es sistemáticamente mayor** que a 10 m en todos los puntos, consistente con el perfil logarítmico del viento atmosférico: $u(z) \propto \ln(z/z_0)$, donde $z_0$ es la rugosidad superficial.
- La región cubierta (norte de Asturias/León, coordenadas 42.8°N–43.2°N, 6.0°W–5.1°W) es una zona con orografía compleja y proximidad costera, lo que genera patrones de viento interesantes para el análisis eólico.

#### 7. Testing

Se implementan tests con `pytest` que validan:

| Test | Qué verifica |
|------|-------------|
| `test_generate_target_locations` | 50 puntos únicos, dentro de los límites geográficos del norte de España |
| `test_process_dataset_chunk` | Interpolación correcta con un dataset `xarray` sintético (mock), columnas esperadas presentes, columnas auxiliares eliminadas |
| `test_load_grib_data_in_batches` | Lectura completa del GRIB, existencia de columnas vectoriales, $ws \ge 0$, $0 \le wd \le 360$ |
| `test_generated_parquet` | Integridad del Parquet generado: 50 estaciones únicas, validaciones físicas, ausencia de columnas completamente nulas |

#### 8. Estructura de Código

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

#### 5. Interpretación de Resultados

**Calmas y perfil vertical del viento:**

- La proporción de calmas a 10 m (3.77%) es **3× mayor** que a 100 m (1.21%). Esto se explica por el perfil logarítmico del viento: la superficie terrestre genera fricción que frena el viento en las capas bajas, mientras que a mayor altura el flujo es más libre. En la zona estudiada (norte de España, orografía compleja), la capa límite atmosférica genera una diferencia especialmente marcada entre ambas alturas.
- Un 3.77% de calmas a 10 m es un valor **bajo** comparado con estaciones continentales del interior, lo que sugiere que la influencia marítima y la canalización orográfica mantienen el viento en movimiento incluso a baja altura.

**Consistencia sectorial:**

- Solo **1 de 30 estaciones** (`station_25`) muestra una ligera inconsistencia sectorial a 10 m (ratio máximo 3.03, apenas por encima del umbral de 3.0), con un $\chi^2 = 6314$. Esto indica que la distribución de direcciones en esa estación no es uniforme, con un sector que acumula ~25% de las observaciones (vs. ~8.3% esperado en distribución uniforme).
- A **100 m ninguna estación** es flaggeada, lo que confirma que el sesgo direccional observado a 10 m es un efecto de superficie (canalización por valles, sombras orográficas) que se disipa con la altura.
- La baja tasa de flags (1/30 = 3.3%) indica que los datos ERA5 son de alta calidad en esta región, sin artefactos instrumentales significativos (esperable al tratarse de datos de reanálisis, no de estaciones físicas).

**Datos faltantes:**

- Las 20 estaciones sin datos no representan un problema de calidad sino una limitación geográfica del dominio GRIB. En un proyecto operacional se ajustaría el mallado para que todos los puntos queden dentro del dominio.

#### 6. Testing

Se implementan **28 tests unitarios** organizados en 4 clases:

| Clase de test | Tests | Qué verifica |
|---------------|-------|-------------|
| `TestApplyCalmCorrections` | 10 | Nullificación de dirección en calmas, preservación de no-calmas, flag `is_calm`, umbrales extremos (0 y ∞), validación de errores (threshold negativo, columnas ausentes), inmutabilidad del DataFrame original, nombres de columna personalizados |
| `TestComputeSectorFrequencies` | 6 | Distribución uniforme perfecta → freq = 1/12, rango de sectores [0, N), suma de frecuencias = 1 por estación, exclusión de direcciones nulas, wrap 360° → sector 0, sectores personalizados (4 cuadrantes) |
| `TestFlagSectorInconsistencies` | 5 | Distribución uniforme no flagged ($\chi^2 \approx 0$), distribución sesgada flagged ($\chi^2 \gg 0$), ratio máximo de desviación, tolerancia alta desactiva flag, multi-estación |
| `TestRunQualityControl` | 7 | Retorno de dos DataFrames, columnas esperadas en flags, preservación de filas (sin eliminación), todas las estaciones reciben flags, parámetros personalizados, compatibilidad con 100 m |

#### 7. Estructura de Código

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

### Fase 3: Distribuciones de Weibull y Variabilidad Estacional ✅

En esta fase se ajustan distribuciones de Weibull de dos parámetros a las series de velocidad del viento, tanto a nivel anual como estacional, y se analiza la variabilidad entre estaciones del año.

#### 1. Distribución de Weibull

La distribución de Weibull es el modelo estándar en energía eólica para caracterizar la frecuencia de velocidades del viento. Su función de densidad de probabilidad es:

$$f(v) = \frac{k}{A} \left(\frac{v}{A}\right)^{k-1} \exp\left[-\left(\frac{v}{A}\right)^k\right]$$

donde:

- $k$ (forma): describe la amplitud de la distribución. Valores típicos: 1.5–3.0
- $A$ (escala): relacionado con la velocidad media del viento (m/s)

El ajuste se realiza por **Máxima Verosimilitud (MLE)** usando `scipy.stats.weibull_min` con parámetro de localización fijado a cero (`floc=0`), como requiere la Weibull de dos parámetros.

#### 2. Ajuste Anual por Estación

Para cada una de las 50 localizaciones se ajusta una distribución Weibull a la serie completa de velocidades. Se excluyen automáticamente las observaciones nulas y las calmas ($v = 0$). Se requiere un mínimo de 10 observaciones válidas para intentar el ajuste.

El resultado incluye para cada estación:

| Columna | Descripción |
|---------|-------------|
| `weibull_k` | Parámetro de forma |
| `weibull_A` | Parámetro de escala (m/s) |
| `mean_ws` | Velocidad media observada (m/s) |
| `std_ws` | Desviación típica de la velocidad |
| `n_obs` | Número de observaciones válidas |

#### 3. Clasificación Estacional

Se asigna a cada observación una estación meteorológica según la convención estándar:

| Estación | Meses | Código |
|----------|-------|--------|
| Invierno | Diciembre, Enero, Febrero | DJF |
| Primavera | Marzo, Abril, Mayo | MAM |
| Verano | Junio, Julio, Agosto | JJA |
| Otoño | Septiembre, Octubre, Noviembre | SON |

#### 4. Ajuste Estacional por Estación

Para cada combinación estación × estación meteorológica (4 estaciones × N localizaciones) se ajusta una distribución Weibull independiente. Esto permite detectar:

- Estaciones del año con mayor recurso eólico (mayor $A$)
- Variaciones en la forma de la distribución ($k$) entre estaciones
- Localizaciones con viento estable (baja variabilidad) vs. irregular (alta variabilidad)

#### 5. Métricas de Variabilidad Estacional

Para cada localización se calculan las siguientes métricas que resumen la variabilidad del recurso eólico a lo largo del año:

| Métrica | Descripción |
|---------|-------------|
| `cv_k` | Coeficiente de Variación de $k$: $\text{std}(k) / \text{mean}(k)$ |
| `cv_A` | Coeficiente de Variación de $A$: $\text{std}(A) / \text{mean}(A)$ |
| `range_k` | Rango de $k$ entre estaciones ($\max - \min$) |
| `range_A` | Rango de $A$ entre estaciones (m/s) |
| `best_season` | Estación con mayor $A$ (viento más fuerte) |
| `worst_season` | Estación con menor $A$ (viento más débil) |
| `n_seasons_fitted` | Número de estaciones con ajuste exitoso |

> Un CV bajo de $A$ indica un recurso eólico estable a lo largo del año, lo cual es deseable para la generación eólica continua. Un CV alto sugiere marcada estacionalidad.

#### 6. Datos de Salida

El pipeline genera tres archivos Parquet por nivel de altura en `data/results/`:

| Archivo | Contenido |
|---------|-----------|
| `weibull_annual_{h}m.parquet` | Ajustes Weibull anuales por estación |
| `weibull_seasonal_{h}m.parquet` | Ajustes Weibull por estación y estación del año |
| `weibull_variability_{h}m.parquet` | Métricas de variabilidad estacional |

#### 7. Resultados del Pipeline

##### Ajuste Anual

| Métrica | 10 m | 100 m |
|---------|------|-------|
| Estaciones con ajuste exitoso | 30/50 | 30/50 |
| $k$ medio (forma) | 1.603 | 1.649 |
| $k$ rango | [1.442, 1.744] | [1.513, 1.773] |
| $A$ medio (escala) | 2.462 m/s | 4.054 m/s |
| $A$ rango | [2.228, 2.752] m/s | [3.740, 4.468] m/s |
| Velocidad media | [1.995, 2.444] m/s | [3.334, 3.970] m/s |

##### Ajuste Estacional

| Estación del año | $k$ medio (10 m) | $A$ medio (10 m) | $k$ medio (100 m) | $A$ medio (100 m) |
|------------------|-------------------|-------------------|--------------------|-----------------|
| DJF (Invierno) | 1.559 | 2.627 m/s | 1.681 | 4.618 m/s |
| MAM (Primavera) | 1.627 | 2.513 m/s | 1.686 | 4.070 m/s |
| JJA (Verano) | 1.959 | 2.085 m/s | 2.121 | 3.163 m/s |
| SON (Otoño) | 1.528 | 2.628 m/s | 1.576 | 4.390 m/s |

##### Variabilidad Estacional

| Métrica | 10 m | 100 m |
|---------|------|-------|
| CV de $A$ (media) | 0.093 | 0.138 |
| CV de $A$ (rango) | [0.069, 0.120] | [0.103, 0.171] |
| Rango $A$ medio | 0.590 m/s | 1.466 m/s |
| Mejor estación (más frecuente) | DJF (16) / SON (14) | DJF (27) / SON (3) |
| Peor estación (unánime) | JJA (30/30) | JJA (30/30) |

#### 8. Interpretación de Resultados

**Parámetro de forma $k$ y régimen de vientos:**

- Los valores de $k$ entre **1.4 y 1.8** son relativamente bajos comparados con los típicos de sitios eólicos óptimos ($k \approx 2.0$–$2.5$). Un $k$ bajo indica una distribución de velocidades con cola pesada: hay una proporción significativa tanto de calmas como de rachas fuertes. Esto es típico de zonas con **orografía compleja** como el norte de España, donde las montañas generan turbulencia y variabilidad local.
- En **verano (JJA)** el parámetro $k$ sube a **1.96–2.12**, indicando un régimen de vientos más estable y predecible, asociado a las brisas térmicas diurnas y la menor actividad ciclónica.
- A **100 m** los valores de $k$ son ligeramente superiores (1.65 vs 1.60), reflejando que el viento es más regular al alejarse de la superficie y sus perturbaciones.

**Parámetro de escala $A$ y recurso eólico:**

- La escala $A$ a 100 m (**4.05 m/s**) es un **65% mayor** que a 10 m (**2.46 m/s**), consistente con la ley de potencia del perfil vertical: $u(z_2)/u(z_1) = (z_2/z_1)^\alpha$ con $\alpha \approx 0.2$ para terreno moderadamente rugoso, lo que predice un ratio de $(100/10)^{0.2} \approx 1.58$, muy próximo al observado.
- Los valores de $A$ son **moderados-bajos** para aplicaciones eólicas. Los mejores emplazamientos europeos presentan $A > 8$–$10$ m/s a 100 m. Sin embargo, debe tenerse en cuenta que los datos ERA5 tienden a **subestimar** las velocidades en zonas de orografía compleja, ya que el modelo suaviza los picos topográficos.

**Estacionalidad:**

- **Invierno (DJF) y otoño (SON)** son las estaciones con mayor recurso eólico (mayor $A$), dominadas por los frentes atlánticos y las borrascas extratropicales que cruzan la península de oeste a este.
- **Verano (JJA)** es unánimemente la peor estación en las 30 localizaciones, con una reducción del 25% en $A$ a 10 m y del 32% a 100 m respecto al invierno. Esto coincide con el anticiclón de las Azores, que inhibe la actividad ciclónica sobre la península ibérica en verano.
- A 100 m, el invierno domina de forma más clara (**27/30** estaciones vs. 16/30 a 10 m), indicando que a mayor altura los patrones sinópticos de gran escala (borrascas invernales) predominan sobre los efectos locales (brisas, canalización).

**Variabilidad y estabilidad del recurso:**

- El CV de $A$ a 10 m es **0.093** (9.3%), un valor bajo que indica un recurso relativamente estable a lo largo del año. A 100 m el CV sube a **0.138** (13.8%), lo que puede parecer contradictorio pero se explica porque a mayor altura los contrastes estacionales sinópticos se amplifican (más viento invernal, más calma estival), mientras que a 10 m la orografía local amortigua estos contrastes.
- El rango de $A$ entre estaciones es de **0.59 m/s** a 10 m y **1.47 m/s** a 100 m, confirmando que la variabilidad estacional absoluta crece con la altura.

#### 9. Testing

Se implementan **60 tests unitarios** organizados en 7 clases:

| Clase de test | Tests | Qué verifica |
|---------------|-------|-------------|
| `TestFitWeibull` | 8 | Recuperación de parámetros conocidos, positividad, mínimo de observaciones, array vacío, distribuciones uniformes y estrechas |
| `TestFitWeibullByStation` | 12 | Una fila por estación, columnas esperadas, ajuste en estaciones válidas, `None` en estaciones nulas, parámetros razonables, media coherente, nombres de columna personalizados, orden |
| `TestAddSeasonColumn` | 7 | Columna añadida, preservación de columnas originales, mapeo correcto de meses, diciembre = DJF, solo 4 estaciones, error si falta columna |
| `TestFitWeibullByStationAndSeason` | 10 | 4 estaciones por localización, parámetros diferentes entre estaciones, invierno > verano, estaciones nulas con `None`, 100 obs por estación, multi-estación |
| `TestComputeSeasonalVariability` | 12 | CV positivo, rango positivo, mejor estación = DJF, peor = JJA, 4 estaciones ajustadas, exclusión de nulos, resultado vacío si todo falla, CV ≈ 0 para viento constante |
| `TestRunWeibullAnalysis` | 6 | Tres DataFrames de salida, conteo correcto de filas, exclusión de estaciones nulas, consistencia entre outputs, columna personalizada |
| `TestConstants` | 4 | Cobertura de meses 1–12, valores válidos, 4 estaciones únicas |

#### 10. Estructura de Código

```
src/weather/
├── data/
│   ├── __init__.py
│   ├── load.py              # (Fase 1) Ingestión GRIB
│   ├── quality.py           # (Fase 2) Control de calidad
│   └── weibull.py           # fit_weibull(), fit_weibull_by_station(),
│                             # add_season_column(), fit_weibull_by_station_and_season(),
│                             # compute_seasonal_variability(), run_weibull_analysis()
└── pipelines/
    ├── __init__.py
    ├── ingest.py             # (Fase 1) run_ingestion()
    ├── qc.py                 # (Fase 2) run_qc()
    └── weibull.py            # run_weibull_pipeline() → lee QC Parquet, ajusta Weibull, genera resultados

tests/
├── test_load.py              # (Fase 1) 4 tests
├── test_quality.py           # (Fase 2) 28 tests
└── test_weibull.py           # 60 tests de Weibull y variabilidad estacional
```
### Fase 4: Producción Energética (AEP) ✅

En esta fase se implementa el cálculo de la Producción Energética Anual (AEP, por sus siglas en inglés) para cada estación, aplicando curvas de potencia de aerogeneradores reales.

#### 1. Curva de Potencia

Se utiliza como referencia una curva de potencia estándar para un aerogenerador de **2.0 MW** (ej. equivalente a un modelo Vestas V90 típico de clase IEC baja/media), modelada numéricamente con los siguientes hitos operativos:

- **Cut-in wind speed**: 4 m/s (velocidad de arranque)
- **Rated wind speed**: ~12-13 m/s (velocidad nominal para alcanzar la máxima potencia de 2000 kW)
- **Cut-out wind speed**: 25 m/s (velocidad de parada por seguridad)

#### 2. Metodología de Cálculo

Se evalúa la energía generada mediante dos enfoques complementarios ("Reto Big Data"):

- **AEP Teórico (CDF Vectorizado)**: Calcula la energía integrando la curva de potencia sobre la distribución de Weibull paramétrica ajustada en la Fase 3. Para un alto rendimiento masivo, se vectoriza sobre todas las estaciones empleando la Función de Distribución Acumulada (CDF) de Weibull de forma analítica en tramos definidos por la curva de potencia ($Prob = CDF(v_{sup}) - CDF(v_{inf})$).
- **AEP Empírico (Time-Series Asof Join)**: Mapea la serie temporal hora a hora usando cruces de proximidad masivos (`join_asof` en Polars con estrategia `nearest`), emparejando eficientemente la velocidad horaria con el valor de potencia más cercano y promediando anualmente sobre la serie original.

Ambos resultados se agregan y se reescalan de kW a GWh considerando las 8760 horas del año estándar.

#### 3. Resultados y Ranking de Localizaciones

Se genera un ranking (`rank_locations`) para ordenar descendentemente las localizaciones en función de su AEP Teórico e identificar los puntos geográficos de la muestra con mayor potencial para albergar un parque eólico comercial. 

| Ranking | Estación | Altura | AEP Teórico | AEP Empírico | k (forma) | A (escala) |
|---------|----------|--------|-------------|--------------|-----------|------------|
| 1 | `station_7` | 100 m | 1.36 GWh | 1.38 GWh | 1.72 | 4.45 m/s |
| 2 | `station_8` | 100 m | 1.35 GWh | 1.38 GWh | 1.74 | 4.47 m/s |
| 3 | `station_6` | 100 m | 1.32 GWh | 1.34 GWh | 1.69 | 4.37 m/s |

> **Nota de Análisis:** El AEP para turbinas de 2.0 MW en estas zonas con un perfil de viento $A \approx 4.45$ m/s arroja una producción anual en torno a 1.36 GWh (Factor de capacidad ~7.7%). Estos valores, aunque físicamente correctos de acuerdo a la ecuación de potencia, sugieren recursos moderados debido a las mallas del modelo ERA5 en el interior montañoso, que suaviza extremos. Hay gran concordancia entre los cálculos empíricos directos y los teóricos usando los ajustes estadísticos MLE de la Fase 3.

#### 4. Testing

Se implementan tests unitarios específicos (`pytest`) verificando la consistencia operativa:

| Test | Qué verifica |
|------|-------------|
| `test_reference_power_curve` | Límites operativos (0 debajo cut-in y por encima cut-out, y límite max 2000 kW) |
| `test_calculate_aep_vectorized_cdf` | Propagación correcta de nulos (`np.nan`) en vectorización matemática de Numpy |
| `test_compute_theoretical_aep` | Validar AEP escalar ascendente con escalas (A) de Weibull mayores y manejo de `null` en Polars |
| `test_compute_empirical_aep` | Agregación del time series mock mediante Polars `join_asof` sin fallos y limitación frente a nulos |

#### 5. Estructura de Código

```
src/weather/
├── data/
│   ├── aep.py               # get_reference_power_curve(), compute_theoretical_aep(), 
│                             # compute_empirical_aep(), rank_locations()
└── pipelines/
    └── aep.py               # run_aep_pipeline() → Lee distribuciones y temporales, une y evalua
tests/
└── test_aep.py              # Validaciones para cálculos AEP vectorizados y asof joins
```

### Fase 5: Visualización 🔲

*Próximamente*: Rosas de viento interactivas, mapas de potencial eólico.

## Alumno

**Álvaro Inclán** · [github.com/alvaroinclan](https://github.com/alvaroinclan)

## Profesor

**Álvaro Diez** · [github.com/alvarodiez20](https://github.com/alvarodiez20)

---

*Big Data · 4º Grado en Matemáticas · UNIE Universidad · 2025–2026*