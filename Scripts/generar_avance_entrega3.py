# crear_visualizaciones.py
# VERSIÓN FINAL: Incluye análisis para hombres y mujeres por separado.

import pandas as pd
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt

# --- CONFIGURACIÓN INICIAL ---
DB_URL = "sqlite:///salud_federada.db"
OUTPUT_DIR = "./" 

def conectar_db():
    return create_engine(DB_URL)

# --- FUNCIONES PARA OBTENER DATOS (sin cambios) ---

def obtener_datos_pregunta6(engine):
    sql_query = "SELECT anio, entidad_norm, SUM(COALESCE(F11, 0)) AS total_F11 FROM defunciones GROUP BY anio, entidad_norm;"
    with engine.begin() as conn:
        df_opioides = pd.read_sql(text(sql_query), conn)
    return df_opioides.groupby("entidad_norm")["total_F11"].sum().sort_values(ascending=False)

def obtener_datos_pregunta7(engine):
    sql_query = """
        SELECT sexo, edad_quinquenal, SUM(F10) as total_alcohol, SUM(F14) as total_cocaina
        FROM defunciones GROUP BY sexo, edad_quinquenal
        ORDER BY sexo, edad_quinquenal;
    """
    with engine.begin() as conn:
        return pd.read_sql(text(sql_query), conn)

def obtener_datos_pregunta9(engine):
    grupos_jovenes = ['15 a 19 años', '20 a 24 años', '25 a 29 años', '30 a 34 años']
    columnas_f = [f"F{i}" for i in range(10, 20)]
    suma_total_sql = f"SUM(" + " + ".join(columnas_f) + ")"
    sql_query = f"""
        SELECT
            CASE WHEN edad_quinquenal IN ({', '.join([f"'{g}'" for g in grupos_jovenes])}) THEN 'Jóvenes (15-34)'
            ELSE 'Otros grupos de edad' END as grupo_etario,
            {suma_total_sql} as total_defunciones
        FROM defunciones GROUP BY grupo_etario;
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(sql_query), conn)
    if not df.empty and df['total_defunciones'].sum() > 0:
        df['porcentaje'] = (df['total_defunciones'] / df['total_defunciones'].sum()) * 100
        return df
    return pd.DataFrame()

# --- FUNCIONES PARA CREAR GRÁFICAS (CON NUEVAS FUNCIONES PARA MUJERES) ---

def crear_grafica_pregunta6(ranking_entidades):
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(10, 6))
    ranking_entidades.head(10).sort_values().plot(kind='barh')
    plt.title('Top 10 Entidades por Defunciones Asociadas a Opioides (F11)', fontsize=14)
    plt.xlabel('Número Total de Defunciones', fontsize=12)
    plt.ylabel('Entidad Federativa', fontsize=12)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/grafica_opioides_por_entidad.png", dpi=300)
    print("[OK] Gráfica Pregunta 6 (Opioides) guardada.")

def crear_graficas_pregunta7(perfil_demografico):
    """Crea y guarda las 4 gráficas para el perfil demográfico (hombres/mujeres, alcohol/cocaína)."""
    # --- HOMBRES ---
    df_hombres = perfil_demografico[perfil_demografico['sexo'] == 'Masculino'].set_index('edad_quinquenal')
    
    plt.figure(figsize=(12, 7))
    df_hombres['total_alcohol'].sort_index().plot(kind='bar', color='skyblue')
    plt.title('Defunciones por Alcohol (F10) en Hombres por Grupo de Edad', fontsize=14)
    plt.xlabel('Grupo de Edad', fontsize=12)
    plt.ylabel('Número de Defunciones', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/grafica_alcohol_hombres_por_edad.png", dpi=300)
    print("[OK] Gráfica Pregunta 7 (Alcohol Hombres) guardada.")

    plt.figure(figsize=(12, 7))
    df_hombres['total_cocaina'].sort_index().plot(kind='bar', color='coral')
    plt.title('Defunciones por Cocaína (F14) en Hombres por Grupo de Edad', fontsize=14)
    plt.xlabel('Grupo de Edad', fontsize=12)
    plt.ylabel('Número de Defunciones', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/grafica_cocaina_hombres_por_edad.png", dpi=300)
    print("[OK] Gráfica Pregunta 7 (Cocaína Hombres) guardada.")

    # --- MUJERES ---
    df_mujeres = perfil_demografico[perfil_demografico['sexo'] == 'Femenino'].set_index('edad_quinquenal')

    plt.figure(figsize=(12, 7))
    df_mujeres['total_alcohol'].sort_index().plot(kind='bar', color='skyblue')
    plt.title('Defunciones por Alcohol (F10) en Mujeres por Grupo de Edad', fontsize=14)
    plt.xlabel('Grupo de Edad', fontsize=12)
    plt.ylabel('Número de Defunciones', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/grafica_alcohol_mujeres_por_edad.png", dpi=300)
    print("[OK] Gráfica Pregunta 7 (Alcohol Mujeres) guardada.")

    plt.figure(figsize=(12, 7))
    df_mujeres['total_cocaina'].sort_index().plot(kind='bar', color='coral')
    plt.title('Defunciones por Cocaína (F14) en Mujeres por Grupo de Edad', fontsize=14)
    plt.xlabel('Grupo de Edad', fontsize=12)
    plt.ylabel('Número de Defunciones', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/grafica_cocaina_mujeres_por_edad.png", dpi=300)
    print("[OK] Gráfica Pregunta 7 (Cocaína Mujeres) guardada.")


def crear_grafica_pregunta9(df_porcentaje):
    plt.figure(figsize=(7, 5))
    df_porcentaje.set_index('grupo_etario')['porcentaje'].plot(kind='bar', color=['orange', 'gray'])
    plt.title('Porcentaje de Defunciones por Sustancias en Jóvenes (15-34 años)', fontsize=14)
    plt.xlabel('Grupo Etario', fontsize=12)
    plt.ylabel('Porcentaje (%)', fontsize=12)
    plt.xticks(rotation=0)
    for index, value in enumerate(df_porcentaje['porcentaje']):
        plt.text(index, value + 0.5, f'{value:.1f}%', ha='center')
    plt.ylim(0, 100)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/grafica_porcentaje_jovenes.png", dpi=300)
    print("[OK] Gráfica Pregunta 9 (Jóvenes) guardada.")

# --- EJECUCIÓN PRINCIPAL ---

if __name__ == "__main__":
    print("Generando visualizaciones para el reporte de la Entrega 3...")
    engine = conectar_db()
    
    datos_p6 = obtener_datos_pregunta6(engine)
    crear_grafica_pregunta6(datos_p6)
    
    datos_p7 = obtener_datos_pregunta7(engine)
    crear_graficas_pregunta7(datos_p7)
    
    datos_p9 = obtener_datos_pregunta9(engine)
    crear_grafica_pregunta9(datos_p9)

    print("\n[LISTO] Todas las gráficas han sido generadas.")