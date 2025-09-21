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

1.  **Limpiar los datos CSV**:
    ```bash
    python Scripts/limpiar_psa.py
    ```
    Se crearán los archivos:
    -defunciones_uso_sustancias_clean.csv
    -urgencias_uso_sustancias_clean.csv
2.  **Procesar los textos y crear el índice**:
    ```bash
    python Scripts/procesar_cowese_textos.py --cowese CoWeSe_sample.txt --outdir ./Textos
    ```
3.  **Cargar todo y crear la base de datos federada**:
    (Este script creará un archivo `salud_federada.db` en la raíz del proyecto).
    ```bash
    python Scripts/salud_federada_prototipo.py --data-dir ./Data
    ```
4.  **(Opcional) Ver un análisis de ejemplo**:
    ```bash
    python Scripts/final.py
    ```

### Ejemplo de Consulta Federada

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
    ```

---

## Estructura del Repositorio

```
├── Data/                 # Contiene los 6 CSV fuente que debes añadir.
├── Scripts/              # Contiene todos los scripts de Python del proyecto.
├── Textos/               # Carpeta generada por `procesar_cowese_textos.py`.
├── .gitignore            # Ignora archivos generados y datos masivos.
├── README.md             # Este archivo.
└── CoWeSe_sample.txt     # Muestra del corpus para la ejecución rápida.
```
