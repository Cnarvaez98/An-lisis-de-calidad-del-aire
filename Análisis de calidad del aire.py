import pandas as pd
import requests
from typing import Set
import sqlite3

def ej_1_cargar_datos_demograficos() -> pd.DataFrame:
    url = "https://public.opendatasoft.com/explore/dataset/us-cities-demographics/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B"
    data = pd.read_csv(url, sep=';')
    
    # Eliminar columnas no deseadas y filas duplicadas
    data = data.drop(columns=['Race', 'Count', 'Number of Veterans']).drop_duplicates()
    
    return data

def ej_2_cargar_calidad_aire(ciudades: Set[str]) -> None:
    # Crear una tabla de dimensiones para almacenar datos de calidad del aire
    calidad_aire = pd.DataFrame(columns=['CO', 'NO2', 'O3', 'SO2', 'PM2.5', 'PM10', 'overall_aqi', 'city'])

    # Obtener información de calidad del aire para cada ciudad
    for ciudad in ciudades:
        api_url = f"https://api-ninjas.com/api/airquality?city={ciudad}"
        response = requests.get(api_url)

        if response.status_code == 200:
            data = response.json()
            row = {
                'CO': data['data']['concentration']['CO'],
                'NO2': data['data']['concentration']['NO2'],
                'O3': data['data']['concentration']['O3'],
                'SO2': data['data']['concentration']['SO2'],
                'PM2.5': data['data']['concentration']['PM2.5'],
                'PM10': data['data']['concentration']['PM10'],
                'overall_aqi': data['data']['overall_aqi'],
                'city': ciudad
            }
            calidad_aire = calidad_aire.append(row, ignore_index=True)

    # Guardar los datos en un archivo CSV
    calidad_aire.to_csv("calidad_aire.csv", index=False)

    # Crear una base de datos SQLite y cargar las tablas
    conn = sqlite3.connect("datos_demograficos.db")
    ej_1_cargar_datos_demograficos().to_sql("demografia", conn, index=False, if_exists='replace')
    calidad_aire.to_sql("calidad_aire", conn, index=False, if_exists='replace')
    conn.close()

    # Realizar un JOIN y agregaciones para verificar si las ciudades más pobladas tienen la peor calidad del aire
    conn = sqlite3.connect("datos_demograficos.db")
    query = """
    SELECT d.City, d.State, d.Population, c.overall_aqi
    FROM demografia d
    JOIN calidad_aire c ON d.City = c.city
    ORDER BY d.Population DESC
    LIMIT 10;
    """
    result = pd.read_sql_query(query, conn)
    conn.close()

    print(result)

# Tests
ej_1_cargar_datos_demograficos()
ciudades = set(["Perris", "Mount Vernon", "Mobile", "Dale City", "Maple Grove", "Muncie", "San Clemente", "Providence", "Norman", "Hoover"])
ej_2_cargar_calidad_aire(ciudades)
