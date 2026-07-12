# Resumen técnico del proyecto

Este documento es distinto de `PROGRESO.md` (que es la bitácora cronológica,
"qué hicimos y cuándo"). Aquí el objetivo es otro: dejar organizado por tema
todo lo que hace falta para escribir el paper/informe final — el problema, las
decisiones técnicas y el *por qué* de cada una, los resultados, y los
conceptos de Big Data / Machine Learning que el trabajo usa. Está pensado para
poder copiar/parafrasear secciones enteras al momento de redactar.

---

## 1. Problema y motivación

La elección de librerías/proyectos open source suele basarse en métricas
superficiales (número de estrellas, popularidad) que no reflejan el
mantenimiento ni la actividad real detrás de un repositorio. Un proyecto con
muchas estrellas puede estar abandonado; uno con pocas puede tener
mantenimiento activo y sostenido.

**Pregunta del proyecto:** ¿se pueden agrupar repositorios de GitHub según su
comportamiento real (actividad, colaboración, sostenibilidad en el tiempo),
usando solo datos públicos de eventos, sin depender de métricas de
popularidad ni de etiquetas predefinidas?

**Enfoque:** no supervisado (clustering). No hay "respuesta correcta" — los
grupos que aparecen se interpretan después de correr el algoritmo, no se
buscan a propósito.

---

## 2. Fuente de datos y muestreo

**Fuente única:** [GH Archive](https://data.gharchive.org/), que publica un
archivo `.json.gz` por cada hora con todos los eventos públicos de GitHub de
esa hora (formato `{YYYY-MM-DD-H}.json.gz`). No se usó la API de GitHub, npm,
PyPI ni ninguna otra fuente — es una restricción deliberada del proyecto.

**Por qué muestreo y no todo el histórico:** procesar todo GH Archive
implicaría terabytes de datos y no es viable ni necesario para un proyecto de
curso. Se optó por un muestreo:

- **12 horas dispersas iniciales** (una por mes, ago. 2025 – jul. 2026): da
  variedad temporal amplia, pero cada repo aparece como mucho en una hora
  (foto puntual, no serie de tiempo).
- **Expansión a ~124 horas:** 12 originales + 57 horas dispersas nuevas (5
  por mes, distintos días/horas) + un **bloque de 4 días seguidos** (1-4 de
  junio de 2026). El bloque continuo es la pieza clave: permite que un mismo
  repo aparezca en varias horas/días *cercanos en el tiempo*, dando por
  primera vez una señal real de continuidad (no solo de volumen).

**Hallazgo de disponibilidad de datos:** en esta instancia de GH Archive, las
horas 00–09 UTC devuelven 404 sistemáticamente para cualquier fecha probada
(verificado con `curl -sI`). Solo hay datos para las horas 10–23 UTC. Todo el
muestreo se ajustó a ese rango.

**Justicia del muestreo — un error a evitar y cómo se detectó:** al comparar
actividad por mes, un mes (junio 2026, donde está el bloque continuo) tenía
~10 veces más horas muestreadas que los demás, lo que inflaba sus totales sin
que reflejara un comportamiento distinto. La corrección fue normalizar por
"eventos por hora muestreada" en vez de comparar totales — con eso el
patrón observado (ver sección 8) se sostuvo, confirmando que era real y no un
artefacto del diseño de muestreo.

---

## 3. Arquitectura del pipeline

Arquitectura por capas (medallion architecture), estándar en ingeniería de
datos:

```
data/raw/ (Bronze)  →  eventos_silver.parquet (Silver)  →  features_repos.parquet (Gold)  →  repos_clusterizados.parquet
   .json.gz crudos       eventos limpios y filtrados         1 fila por repo, con features       + columna "cluster"
```

- **Bronze**: los `.json.gz` descargados tal cual de GH Archive, sin tocar.
- **Silver**: eventos filtrados a los 5 tipos de interés, sin bots, sin
  duplicados, sin nulos en campos críticos. Unidad = 1 evento por fila.
- **Gold**: agregado a nivel de repositorio. Unidad = 1 repo por fila, con
  features numéricos y de texto.

**Motor de procesamiento:** Apache Spark (`pyspark`) en modo `local[*]`
(sin cluster real, sin Hadoop) — usa todos los núcleos de la máquina local
como si fueran varios "workers", pero todo corre en el mismo proceso. Se
usó porque el volumen de datos (millones de eventos, archivos JSON
anidados) excede lo cómodo para pandas puro, y es la herramienta estándar
del curso para Big Data.

**Formato intermedio:** Parquet (columnar, comprimido) en vez de JSON/CSV
entre capas — más rápido de leer y honra tipos de dato (incluye tipos
complejos de Spark ML como `Vector`).

---

## 4. Ingesta

Script `src/ingest.py`: descarga cada hora vía HTTP con `urllib`, guardando
en `data/raw/`. Detalles técnicos relevantes:

- GH Archive está detrás de Cloudflare y devuelve `403 Forbidden` si la
  petición no trae un header `User-Agent` de navegador — se simula uno.
- El script es idempotente: si el archivo ya existe localmente, no lo
  vuelve a descargar.
- Se verifica con `curl -sI` que una hora exista (código 200) antes de
  añadirla a la lista, porque GH Archive puede no tener el archivo de una
  hora puntual (404), y en este caso además le faltan sistemáticamente las
  horas 00-09 UTC.

---

## 5. Limpieza de datos (capa Silver)

Pasos, en este orden, con el conteo de filas después de cada uno (sobre el
dataset final de ~124 horas):

| Paso | Filas restantes |
|---|---|
| Bronce (todos los tipos de evento) | 17,909,186 |
| Filtrar a los 5 tipos de interés | 14,935,939 |
| Quitar nulos críticos (repo, actor, fecha) | 14,935,846 |
| Quitar bots | 12,048,304 |
| Quitar duplicados (por `id` de evento) | **12,047,086** |

**Criterio de bot:** login del actor termina en `-bot`, `[bot]`, o
contiene `dependabot` (heurística simple, no un servicio de detección).

**Por qué este orden:** los nulos se filtran antes que los bots para que el
filtro de bots no se confunda comparando con actores nulos.

---

## 6. Ingeniería de features (capa Gold)

**Filtro de actividad mínima:** de 3,419,971 repositorios distintos vistos,
solo se conservan los que tuvieron **al menos 3 eventos** en la muestra
(847,133 repos, ~24.8%). Los demás son "una sola foto" (1-2 eventos) y no
aportan señal de comportamiento — decisión tomada explícitamente con el
usuario, evaluando y descartando la alternativa de bajar más datos (no
resuelve el problema: la probabilidad de que el mismo repo caiga en dos
horas *dispersas* distintas es prácticamente nula, dado el tamaño de GitHub).

**Features numéricos (12 en total):**

*Conteos (9)* — cuántos eventos de cada tipo tuvo el repo, más 2 de
cobertura temporal:
`n_push, n_issues, n_pr, n_watch, n_fork, n_total, n_actores, n_horas_distintas, n_dias_distintos`

*Razones (3, agregadas en la segunda vuelta de trabajo)*:
- `ratio_issues_push = n_issues / (n_push + 1)` — discusión relativa al código.
- `ratio_pr_push = n_pr / (n_push + 1)`.
- `ratio_colaboracion = n_actores / n_total` — qué tan repartido está el
  trabajo entre personas distintas (cerca de 1) vs. concentrado en una
  sola (cerca de 0).

Por qué se agregaron las razones: en la primera vuelta de interpretación, un
cluster distintivo ("orientado a discusión") solo se detectó *mirando los
datos a mano* (notando que `n_issues ≈ n_push`). Convertir esa observación en
un feature explícito permite que el propio algoritmo la use, en vez de
depender de que alguien la note después.

**Features de texto (TF-IDF):** se combina, por repo, el texto disponible de:
mensajes de commits (`payload.commits[].message`), títulos de issues
(`payload.issue.title`) y títulos de pull requests
(`payload.pull_request.title`). Pipeline: `RegexTokenizer` → filtro de
tokens puramente numéricos → `StopWordsRemover` (inglés + basura de
URLs/HTML: `com`, `github`, `https`, `href`, `li`, `md`) → `CountVectorizer`
(vocabulario de 300 palabras, mínimo 5 repos) → `IDF`.

Nota importante de cobertura: el texto solo está disponible para una
minoría de repos (15%-38% según el cluster final) — la mayoría de los
eventos (especialmente pushes) no traen mensajes de commit en el payload de
GH Archive.

---

## 7. Modelo de clustering

**Algoritmo:** K-Means (Spark MLlib), sobre un vector final de 312
dimensiones (12 numéricos escalados + 300 de TF-IDF normalizado).

**Preparación del vector (decisiones clave):**
- Los 9 conteos se transforman con `log(1+x)` antes de escalar. Motivo: son
  variables de tipo *power-law* (la mediana de `n_push` es 3, pero el
  máximo observado es 2358) — sin esta transformación, un puñado de repos
  extremos dominan la distancia euclidiana y KMeans termina poniendo casi
  todos los repos en un solo cluster gigante.
- Los numéricos (ya en log) se escalan con `StandardScaler` (media 0,
  desviación 1) — sin esto, columnas con rango más grande pesarían más solo
  por la escala, no por relevancia real.
- El vector TF-IDF se normaliza a longitud 1 (`Normalizer`, L2). Motivo: se
  detectaron repos con texto masivamente repetido (posible *spam* de
  commits para inflar el historial) con normas de TF-IDF hasta 2.36×10⁸,
  frente a 0 en la mayoría de los repos — sin normalizar, esos pocos repos
  dominarían el clustering solo por tener mucho texto repetido.
- Los 3 ratios NO se transforman con log (ya vienen acotados entre 0 y
  valores pequeños).

**Selección de k (número de clusters):** no se fija de antemano. Se prueban
valores de k=2 a 10, comparando costo (WSSSE, método del codo) y Silhouette
score. Con el primer conjunto de features (sin los ratios), el silhouette
era coherente entre k adyacentes; al agregar los ratios, el silhouette de
una sola corrida resultó **inestable** (k=4 dio 0.2552 en una corrida). Para
no confiar en un resultado potencialmente accidental, se repitió el ajuste
de k=3, 4 y 5 con **3 semillas de inicialización distintas**:

| k | costo (3 semillas) | silhouette (3 semillas) | ¿estable? |
|---|---|---|---|
| 3 | ~7.11M / ~7.11M / ~7.11M | 0.556 / 0.556 / 0.553 | **sí** |
| 4 | ~6.66M / ~6.66M / ~6.66M | 0.255 / 0.356 / 0.385 | no |
| 5 | ~6.21M / ~6.19M / ~6.09M | 0.570 / 0.411 / 0.403 | no |

Se eligió **k=3** por ser la única opción estable ante distintas
inicializaciones — un criterio de robustez, no de ajuste a la narrativa
del proyecto (que también hablaba de 3 categorías; la coincidencia se
explica en la sección 9).

**Visualización:** proyección a 2 dimensiones con PCA (varianza explicada:
~27.5% + ~22.7% ≈ 50%) sobre una muestra de 8,000 repos, para poder
graficar los clusters (útil para la sustentación/paper, no para el análisis
en sí).

---

## 8. Resultados finales

Con k=3, sobre 847,133 repositorios:

| Cluster | Repos | % | Perfil |
|---|---|---|---|
| **Baja actividad / cola larga** | 644,806 | 76.1% | Actividad mínima (push≈4), 1.1 actores en promedio, visto en ~2 horas/1.6 días. Vocabulario genérico. Es la mayoría típica de GitHub: repos personales, de práctica o muy jóvenes. |
| **Comunidad activa** | 18,083 | 2.1% | El grupo más chico, pero con más señales de "salud" tradicional: `ratio_colaboracion`=0.81, `ratio_issues_push`=0.98, 8.9 actores, sostenido ~6.8 días. Palabras como "authored"/"signed" (co-autoría real en commits). |
| **Solitario intensivo (automatizado)** | 184,244 | 21.8% | Mucho push (30/repo) sostenido en el tiempo (~5.3 días), pero de una sola persona (`ratio_colaboracion`=0.11). Palabras "bot", "auto", "ci" en el texto. |

**Interpretación:** el cluster "Comunidad activa" es el más cercano a la
idea intuitiva de "repositorio saludable". "Solitario intensivo" sugiere
actividad automatizada (scripts, bots, CI) sin supervisión de una
comunidad — un riesgo distinto al de "abandono": hay actividad, pero
depende de un solo punto de falla. "Baja actividad" es la cola larga
habitual de cualquier plataforma con millones de usuarios.

---

## 9. Sobre el sesgo de interpretación (relevante para la discusión del paper)

El proyecto pasó por 3 vueltas de análisis, cada una con una interpretación
distinta:

1. **77,696 repos, k=3 (elegido en parte por encajar con la hipótesis
   inicial del proyecto — saludable/riesgo/abandonado).** Sesgo real: el
   número de clusters y su interpretación se influenciaron por la narrativa
   antes de mirar los datos con neutralidad.
2. **847,133 repos, k=4 (elegido por silhouette, sin mirar la narrativa).**
   Apareció un 4º cluster ("orientado a discusión") que la idea original no
   contemplaba — evidencia de que, sin el sesgo, el modelo puede mostrar
   algo distinto a lo esperado.
3. **847,133 repos, k=3 con features de razón (elegido por estabilidad
   estadística, no por narrativa ni por silhouette de una sola corrida).**
   El 4º cluster de la vuelta 2 se fusionó de nuevo con el de "comunidad",
   porque con mejores features quedó claro que colaboración real y
   discusión activa son, en los datos, la misma señal.

El resultado final vuelve a tener 3 grupos que se parecen a la idea
original — pero por un camino metodológicamente distinto y más riguroso.
Esto es un punto válido para la discusión del paper: **una conclusión que
sobrevive a revisiones hechas con criterios distintos es más sólida que una
que se alcanza al primer intento**, sobre todo cuando el primer intento
tenía un sesgo de partida conocido.

---

## 10. Limitaciones

- El texto (TF-IDF) solo está disponible para una minoría de repos (15%-38%
  según el cluster) — GH Archive no siempre incluye mensajes de commit
  completos en el payload.
- Aun con el bloque continuo, el 46% de los repos activos siguen teniendo
  señal de una sola hora (no una serie de tiempo real).
- Los nombres/interpretación de los clusters son un juicio humano posterior
  al clustering, no una etiqueta que venga en los datos — es la naturaleza
  del aprendizaje no supervisado.
- No se investigó la causa raíz del hallazgo de la sección 11 (más push,
  menos todo lo demás) — se documenta como observación, no como conclusión
  causal.
- La muestra, aunque más grande que al inicio, sigue siendo una fracción
  minúscula de la actividad total de GitHub (una plataforma con decenas de
  millones de repos activos).

---

## 11. Hallazgo adicional: el patrón temporal Push↑ / resto↓

Al comparar actividad por mes (normalizando por horas muestreadas para que
la comparación sea justa — ver sección 2), se observa un patrón consistente
entre agosto 2025 y julio 2026:

- `PushEvent` por hora **sube** de ~97,354 a ~145,487 (+~50%).
- `ForkEvent`, `WatchEvent`, `IssuesEvent`, `PullRequestEvent` por hora
  **caen** entre 97% y 99% en el mismo periodo.

Es un hallazgo colateral (no era el objetivo del proyecto), pero es fuerte y
consistente, y vale la pena mencionarlo en el paper como observación abierta
— sin proponerlo como causal, ya que los datos disponibles no permiten
distinguir entre varias hipótesis (más actividad automatizada/agéntica,
cambios en cómo GH Archive registra los eventos, un cambio real de
comportamiento de la comunidad, etc.).

---

## 12. Conceptos técnicos utilizados (para explicar en la sustentación)

**Big Data / computación distribuida:**
- Apache Spark: arquitectura driver/executor, `DataFrame` y evaluación
  perezosa (*lazy evaluation* — las transformaciones no se ejecutan hasta
  una acción como `.count()` o `.show()`), particiones y *shuffling*.
- Modo `local[*]`: simula un cluster usando los núcleos de una sola máquina.
- Formato columnar Parquet vs. formatos de fila (JSON/CSV).

**Ingeniería de datos:**
- Arquitectura por capas Bronze/Silver/Gold (*medallion architecture*).
- Esquema al leer (*schema-on-read*) — Spark infiere la estructura del JSON
  automáticamente, incluyendo campos anidados (`struct`, `array`).
- Deduplicación, filtrado de nulos, limpieza basada en heurísticas.

**Ingeniería de features:**
- Transformación logarítmica para variables con distribución sesgada
  (*power-law* / cola larga).
- Estandarización (*z-score*, media 0 / desviación 1).
- Normalización L2 (vector a longitud 1).
- Features derivados / de razón (*ratio features*).

**Procesamiento de texto (NLP):**
- Tokenización con expresiones regulares.
- Eliminación de *stopwords*.
- Bolsa de palabras (*bag-of-words*) vía `CountVectorizer`.
- TF-IDF (*Term Frequency – Inverse Document Frequency*): pesa más una
  palabra frecuente en un documento pero rara en el corpus general.

**Aprendizaje no supervisado:**
- K-Means: partición de datos en k grupos minimizando la distancia a un
  centroide.
- WSSSE / costo (*within-cluster sum of squared errors*) y método del codo.
- Silhouette score: qué tan bien separados están los clusters (-1 a 1).
- Sensibilidad a la inicialización / estabilidad ante distintas semillas
  aleatorias — un k "natural" en los datos da resultados estables sin
  importar dónde arranca el algoritmo.
- PCA (*Principal Component Analysis*): reducción de dimensionalidad para
  visualización, reteniendo las direcciones de mayor varianza.

---

## 13. Qué debería ir en el paper (guía de estructura)

Mapeo sugerido de este resumen a las secciones típicas de un paper/informe
académico:

| Sección del paper | Contenido (de este documento) |
|---|---|
| **Título + Resumen (Abstract)** | Problema (sección 1) + método (KMeans, GH Archive) + resultado principal (3 clusters, sección 8) en 150-250 palabras. |
| **1. Introducción** | Sección 1 completa (problema, motivación, pregunta de investigación). |
| **2. Datos y metodología** | Secciones 2 a 7: fuente de datos, muestreo, arquitectura del pipeline, limpieza, features, modelo. Aquí van las tablas de conteos (sección 5) y la tabla de selección de k (sección 7). |
| **3. Resultados** | Sección 8 (tabla de clusters) + gráficas: barras de tamaño de cluster, PCA 2D, barras de promedios por feature. |
| **4. Discusión** | Sección 9 (honestidad metodológica sobre el sesgo evitado) + sección 11 (hallazgo del patrón temporal, presentado como observación abierta, no conclusión). |
| **5. Limitaciones y trabajo futuro** | Sección 10, más ideas de expansión (más datos, LDA para tópicos, validación externa). |
| **6. Conclusiones** | Resumen de sección 8 + 9: los 3 perfiles de comportamiento encontrados y por qué el proceso de llegar a ellos importa tanto como el resultado. |
| **Anexos / Apéndice técnico** | Sección 12 (glosario de conceptos) si el formato del curso lo permite, o como referencia para preguntas en la sustentación. |

**Recomendación para la sustentación oral:** el jurado probablemente va a
preguntar "¿por qué k=3?" y "¿cómo evitaron que el resultado calzara
artificialmente con lo que esperaban?" — las secciones 7 y 9 responden
directamente a eso, con números concretos (la tabla de estabilidad ante
semillas). Tenerla a mano/memorizada es más valioso que memorizar cualquier
otra parte del trabajo.
