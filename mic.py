import requests
import pandas as pd

# URLs de la API
urls = {
    "by_product": "https://gis.mic.gov.py/api/sales/by_product/2024",
    "by_company": "https://gis.mic.gov.py/api/sales/by_month/2024",
    "by_month": "https://gis.mic.gov.py/api/sales/by_month/2024",
    "by_category": "https://gis.mic.gov.py/api/sales/by_category/2024",
    "by_estacion": "https://gis.mic.gov.py/api/sales/by_estacion",
    "by_price":"https://gis.mic.gov.py/api/sales/by_price"
}
# Diccionario para almacenar DataFrames
dataframes = {}

# Procesar cada URL
for key, url in urls.items():
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Procesar API de precios por separado
        if key == "by_price":
            if "columns" in data and "data" in data and "rows" in data:
                # Crear DataFrame con productos como índice y columnas como empresas
                precios_df = pd.DataFrame(data["data"], columns=data["columns"], index=data["rows"])
                precios_df.index.name = "Producto"  # Añadir nombre al índice
                dataframes[key] = precios_df

            # Mapear emblemas
            if "emblema" in data:
                emblemas_df = pd.DataFrame(data["emblema"].items(), columns=["Codigo", "Nombre_Empresa"])
                dataframes["emblemas"] = emblemas_df
            continue  # Saltar al siguiente API después de procesar by_price

        # Procesar las demás APIs
        if 'data' in data and data['data']:
            df = pd.DataFrame(data['data'])

            # Mapear nombres adicionales según la clave específica
            if key == "by_product" and "producto" in data:
                df["producto_descripcion"] = df["producto"].map(data["producto"])
            elif key == "by_company" and "distribuidor" in data:
                df["distribuidor_nombre"] = df["distribuidor"].map(data["distribuidor"])
            elif key == "by_month" and "categoria" in data:
                df["categoria_descripcion"] = df["categoria"].map(data["categoria"])
            elif key == "by_category":
                # Extraer distribuidores y departamentos
                if "distribuidor" in data:
                    distribuidores = pd.DataFrame(data['distribuidor'].items(), columns=["ID", "Nombre_Distribuidor"])
                    dataframes["distribuidores"] = distribuidores
                if "departamento" in data:
                    departamentos = pd.DataFrame(data['departamento'].items(), columns=["ID", "Departamento"])
                    dataframes["departamentos"] = departamentos

            # Guardar DataFrame en el diccionario
            dataframes[key] = df
            print(f"Datos de {key} procesados correctamente.")
        else:
            print(f"No se encontraron datos en {key}.")
    except Exception as e:
        print(f"Error al procesar {key}: {e}")

# Guardar todo en un único archivo Excel
if dataframes:
    with pd.ExcelWriter("datos_ventas_importaciones_completos_2024.xlsx") as writer:
        for key, df in dataframes.items():
            df.to_excel(writer, sheet_name=key, index=True if key == "by_price" else False)
    print("Datos guardados en 'datos_ventas_importaciones_completos_2024.xlsx'")
else:
    print("No hay datos para guardar.")
