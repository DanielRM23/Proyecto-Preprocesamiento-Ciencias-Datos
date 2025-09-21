import sqlite3
import pandas as pd

# Conectar a tu base SQLite
conn = sqlite3.connect("salud_federada.db")

# Ver las tablas existentes
tablas = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
print(tablas)

# Ver las primeras filas de la tabla defunciones
df_def = pd.read_sql("SELECT * FROM defunciones LIMIT 5;", conn)
print(df_def)

# # Ver las primeras filas de la tabla urgencias
# df_urg = pd.read_sql("SELECT * FROM urgencias LIMIT 5;", conn)
# print(df_urg)

conn.close()
