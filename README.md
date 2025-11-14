# Preprocesamiento para la ciencia de datos.

# Proyecto Final.

# Proyecto: Base de Datos Heterogénea Federada (Sector Salud): Consumo de sustancias Psicoactivas en México.

Integrantes del Equipo 5:

-Fernando Rodrigo Valenzuela García de León (fer_rodri-val@hotmail.com)

-Daniel Rojo Mata (danielrojomata@gmail.com)

Este proyecto integra tres tipos de fuentes de datos (relacional, grafo y texto) para analizar el consumo de sustancias psicoactivas en México. Utiliza los códigos de la Clasificación Internacional de Enfermedades (CIE-10), específicamente del bloque F10-F19, como el nexo de unión entre las distintas fuentes.

## Componentes del Proyecto

- **Datos Relacionales (SQL)**: Estadísticas de defunciones y atenciones de urgencia provenientes de `datos.gob.mx`.
- **Grafo de Conocimiento (NetworkX)**: Un grafo que modela la jerarquía de la CIE-10 y las relaciones de comorbilidad y policonsumo documentadas en la literatura científica.
- **Datos de Texto (NLP)**: Un análisis de texto sobre el corpus CoWeSe para mapear lenguaje natural (menciones a sustancias) a los códigos CIE-10 correspondientes y permitir búsquedas semánticas.

---

## Requisitos

Para ejecutar este proyecto, necesitas tener Python 3 y las siguientes librerías. Puedes instalarlas ejecutando:

```bash
pip install pandas sqlalchemy psycopg2-binary networkx scikit-learn
```

---

## Instalación y Configuración

1.  **Clona este repositorio**.
2.  **Añade los datos fuente**: Descarga los siguientes 6 archivos CSV y colócalos dentro de la carpeta `/Data`:
    - `defunciones_uso_sustancias.csv`
    - `urgencias_uso_sustancias.csv`
    - `cie10_f10_f19_nodes.csv`
    - `cie10_f10_f19_edges_enriched.csv`
    - `cie10_f10_f19_roots.csv`
    - `psa_tidy_long.csv`

---

## Configuración del entorno LLM local

El proyecto utiliza Ollama como motor local para ejecutar modelos de lenguaje (sin API externa).
Esto permite correr el sistema RAG/LLM completamente offline sobre CPU.

1. Instalar Ollama

   Descarga e instala Ollama desde su sitio oficial:
   https://ollama.com/download

2. Una vez instalado, verifica que funciona:

- `ollama --version`

3. Descargar el modelo Mistral.
   Usa este comando para obtener el modelo local:

- `ollama pull mistral`

Esto descargará ~4 GB del modelo Mistral 7B Instruct, optimizado para CPU.

4. Probar el modelo.
   Verifica que el modelo responde correctamente:

   -`ollama run mistral`

(Escribe algo breve, como “hola, ¿quién eres?”, y presiona Enter para salir con Ctrl+C.)

## Instrucciones de Uso

El proyecto se puede ejecutar en dos modos: un modo de demostración rápido que usa una muestra de datos de texto, y un modo completo.

### Modo de Demostración (Recomendado para empezar)

Este modo utiliza el archivo `CoWeSe_sample.txt` (incluido en el repositorio) para que puedas probar todo el flujo de trabajo rápidamente.

**Ejecuta los siguientes scripts en orden desde la terminal:**

- Paso: 1. "Limpiar los datos CSV".
  Limpia los archivos crudos de defunciones y urgencias.

  - **comandos**:

    - `python Scripts/limpiar_psa.py`

  - salida:
    - "defunciones_uso_sustancias_clean.csv"
    - "urgencias_uso_sustancias_clean.csv"

- Paso: 2. "Procesar los textos y crear el índice"
  Procesa el corpus CoWeSe y genera las frases normalizadas asociadas a CIE-10.

  - **comandos**:

    - `python Scripts/procesar_cowese_textos.py --cowese CoWeSe_sample.txt --outdir ./Textos`

  - salida:
    - "textos_cie10_frases.csv"
    - "textos_cie10_frases_x_docs.csv"

- Paso 3. "Construcción del grafo"
  Construye el grafo de comorbilidad y policonsumo asociado al bloque CIE-10 F10–F19, utilizando los archivos de nodos y aristas generados durante el preprocesamiento.

  - **comandos**:

    - `python Scripts\generar_grafo.py`

- Paso 4."Perfilado de datos".
  Se realiza el perfilado de los datos para evaluar la calidad de los archivos de hechos, grafo y texto antes de su integración a la base de datos.
  El proceso detecta inconsistencias como columnas faltantes, formatos incorrectos, valores nulos, duplicados y códigos CIE-10 fuera del rango F10–F19, además de validar la estructura y coherencia entre los distintos dominios del proyecto.

  - **comandos**:

    - `python Scripts/perfilado_csv.py`
    - `python Scripts/perfilado_grafo.py`
    - `python Scripts/perfilado_texto.py`

  - Salida: docs/perfilado/csv/

    - 01_conteos.csv — número de filas por tabla
    - 02_nulos.csv — conteo de campos nulos
    - 03_rango_anios.csv — años mínimo y máximo detectados
    - 04_fuera_rango.csv — códigos fuera de F10–F19
    - 05_duplicados.csv — filas en grupos duplicados
    - 06_distribucion_sexo.csv — distribución por sexo
    - perfilado_csv_sql_resumen.md — resumen general del perfilado

  - Salida: docs/perfilado/grafo/

    - 01_nodos.csv — resumen de nodos y descripciones vacías
    - 02_aristas.csv — resumen de aristas y nulos
    - 03_aristas_huerfanas.csv — aristas que no conectan con nodos válidos
    - perfilado_csv_grafo_resumen.md — resumen general del perfilado

  - Salida: docs/perfilado/texto/

    - 01_conteos_frases.csv — total de filas y frases vacías
    - 02_duplicados.csv — número de frases repetidas
    - 03_fuera_rango.csv — frases con códigos fuera de F10–F19
    - perfilado_csv_texto_resumen.md — resumen general del perfilado

- Paso 3: "Limpieza de Datos".
  Limpia los archivos originales de defunciones, urgencias, grafo y texto para asegurar consistencia, eliminar duplicados y normalizar nombres.

  - **comandos**:

    - `python Scripts/limpiar_csv_sql.py`
    - `python Scripts/limpiar_grafos.py`
    - `python Scripts/limpiar_textos.py`

  - archivos_generados:
    - Data/defunciones_uso_sustancias_clean.csv
    - Data/urgencias_uso_sustancias_clean.csv
    - Data/cie10_nodes_clean.csv
    - Data/cie10_edges_enriched.csv
    - Data/textos_cie10_frases.csv

- Paso: 4. "Construir la base de datos federada".
  Integra los datos limpios (SQL, grafo y texto) en una sola base SQLite.

  - **comandos**:

    - `python Scripts/build_base_final.py`

  - salida:
    - "salud_federada.db"

- Paso: 5. "Crear vista unificada".
  Construye la vista unificada v_unificado dentro de la base de datos salud_federada.db.
  Integra en una sola estructura las tres fuentes de información del proyecto.
  El resultado es una vista que consolida eventos, relaciones y texto contextual, lista para consultas federadas o semánticas.
  También se crean otras vistas auxiliares (v_eventos, v_eventos_por_codigo, v_relaciones_epidemiologicas, etc.) que permiten análisis cruzados por código, fuente o tipo de relación.

  - **comandos**:
    - `python Scripts/crear_vista_unificada.py`

- Paso: 6. "Consultar y explorar resultados de las preguntas Descriptivas".
  Resuelve las 10 preguntas DESCRIPTIVAS del proyecto usando SQL + pandas,
  y genera gráficas para su análisis.

  - **comandos**:
    - `python Scripts/consultas_descriptivas.py`

- Paso: 7. "Consultar y explorar resultados de las preguntas Predictivas".
  Ejecuta el sistema de consultas automáticas híbridas (RAG + SQL) sobre la base salud_federada.db utilizando el modelo local Mistral (vía Ollama).
  Los resultados se almacenan en: docs/llm_resultados/

  - **comandos**:
    - `python Scripts/consultas_predictivas.py`

- Paso 8 (opcional). "Consultar y explorar resultados de las preguntas Descriptivas usando LLM".
  - **comandos**:
    - `python Scripts/consultas_rag.py`

### Modo Completo

Para reproducir los resultados con el corpus de texto completo:

1.  **Descarga el corpus CoWeSe** desde su fuente oficial y colócalo en la raíz del proyecto.
2.  **Sigue los mismos pasos** que en el modo de demostración, pero en el paso 2, apunta al archivo completo:
    ```bash
    python Scripts/procesar_cowese_textos.py --cowese CoWeSe.txt --outdir ./Textos
    ```

---

## Estructura del Repositorio

```
├── Data/                 # Contiene los 6 CSV fuente que se usan.
├── Scripts/              # Contiene todos los scripts de Python del proyecto.
├── docs/                 # Contiene los reportes de las entregas.
├── Textos/               # Carpeta generada por `procesar_cowese_textos.py`.
├── .gitignore            # Ignora archivos generados y datos masivos (ignora todo el corpus).
├── README.md             # Este archivo.
└── CoWeSe_sample.txt     # Muestra del corpus para la ejecución rápida.
```
