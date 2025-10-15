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

## Instrucciones de Uso

El proyecto se puede ejecutar en dos modos: un modo de demostración rápido que usa una muestra de datos de texto, y un modo completo.

### Modo de Demostración (Recomendado para empezar)

Este modo utiliza el archivo `CoWeSe_sample.txt` (incluido en el repositorio) para que puedas probar todo el flujo de trabajo rápidamente.

**Ejecuta los siguientes scripts en orden desde la terminal:**

- Paso: 1. "Limpiar los datos CSV".
  
  Limpia los archivos crudos de defunciones y urgencias.
  
  **comandos**:
      - python Scripts/limpiar_psa.py
  
  salida:
      - "defunciones_uso_sustancias_clean.csv"
      - "urgencias_uso_sustancias_clean.csv"

- Paso: 2. "Procesar los textos y crear el índice"
  
  Procesa el corpus CoWeSe y genera las frases normalizadas asociadas a CIE-10.

  **comandos**:
      - python Scripts/procesar_cowese_textos.py --cowese CoWeSe_sample.txt --outdir ./Textos

  salida:
      - "textos_cie10_frases.csv"
      - "textos_cie10_frases_x_docs.csv"

- Paso 3: Limpieza de Datos.

  Limpia los archivos originales de defunciones, urgencias, grafo y texto para asegurar consistencia, eliminar duplicados y normalizar nombres.

  **comandos**:
      - python Scripts/limpiar_csv_sql.py
      - python Scripts/limpiar_grafos.py
      - python Scripts/limpiar_textos.py

  archivos_generados:
      - Data/defunciones_uso_sustancias_clean.csv
      - Data/urgencias_uso_sustancias_clean.csv
      - Data/cie10_nodes_clean.csv
      - Data/cie10_edges_enriched.csv
      - Data/textos_cie10_frases.csv

- Paso: 4. Construir la base de datos federada.

  Integra los datos limpios (SQL, grafo y texto) en una sola base SQLite.

  **comandos**:
  - python Scripts/build_base_final.py

  salida:
      - "salud_federada.db"

- Paso: 5. Consultar y explorar resultados.
  Permite realizar búsquedas textuales y consultas analíticas sobre la base unificada.
  ejemplos:
  - python Scripts/buscar_unificado.py 'cocaína'
  - python Scripts/buscar_unificado.py 'alcohol' --top-origen
  - python Scripts/buscar_unificado.py --codigo F14 --ejemplos-sql --limit 10
  - python Scripts/buscar_unificado.py 'cannabis' --export-all docs/Entrega4_cannabis

<!-- ### Ejemplo de Consulta Federada

Una vez creada la base de datos, puedes hacer consultas que combinen las tres fuentes de datos. Por ejemplo, para buscar información sobre "intoxicación por cocaína":

```bash
python Scripts/salud_federada_prototipo.py --data-dir ./Data --texto "intoxicación por cocaína"
```

**¿Qué hará este comando?**

1.  **Texto -> Código**: Mapeará la palabra "cocaína" al código CIE-10 **F14**.
2.  **Código -> SQL**: Consultará la base de datos para obtener el número de defunciones por F14, agrupado por año y entidad.
3.  **Código -> Grafo**: Consultará el grafo para obtener los subtipos de diagnóstico asociados a F14 (ej. F14.0, F14.1, etc.).

### Modo Completo

Para reproducir los resultados con el corpus de texto completo:

1.  **Descarga el corpus CoWeSe** desde su fuente oficial y colócalo en la raíz del proyecto.
2.  **Sigue los mismos pasos** que en el modo de demostración, pero en el paso 2, apunta al archivo completo:
    ```bash
    python Scripts/procesar_cowese_textos.py --cowese CoWeSe.txt --outdir ./Textos
    ``` -->

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
