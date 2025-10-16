import requests
import os
import json # Importar json para formatear la salida
from typing import Dict, List, Optional
from .monthly_data import get_cached_monthly_data

# Comentamos las definiciones a nivel de módulo que dependen de .env
# MONTHLY_SALES_API_URL = os.getenv("MONTHLY_SALES_API_URL", "http://localhost:5001")
# HEADERS = {
#     "Authorization": f"Bearer {os.getenv('BUSINESS_INVOICES_TOKEN')}"
# }
# SII_INVOICES_BASE_URL = os.getenv("SII_INVOICES_BASE_URL", "http://localhost:8001/api/v1") # URL anterior comentada

def _get_monthly_data(rut: str):
    """Función auxiliar para obtener los datos mensuales de la API.
       Lee la URL y el TOKEN de las variables de entorno aquí.
       Esta función se exporta y la usan los módulos de caché.
    """
    # Leer variables de entorno DENTRO de la función
    api_url = os.getenv("MONTHLY_SALES_API_URL", "http://localhost:5001") # Valor por defecto
    api_token = os.getenv("BUSINESS_INVOICES_TOKEN")

    if not api_token:
        print("[Compras/Ventas] Error: La variable de entorno BUSINESS_INVOICES_TOKEN no está definida.")
        return None

    headers = {
        "Authorization": f"Token {api_token}"
    }

    try:
        url = f"{api_url}/business/{rut}/monthly_sales"
        print(f"[Compras/Ventas] URL: {url}")
        print(f"[Compras/Ventas] HEADERS: {headers}")
        r = requests.get(url, headers=headers)
        r.raise_for_status() # Lanza una excepción para errores HTTP (4xx o 5xx)
        data = r.json()
        if data.get("status") == "ok" and "total_last_months" in data:
            return data["total_last_months"]
        else:
            print(f"[Compras/Ventas] Respuesta inesperada de la API: {data}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"[Compras/Ventas] Error al conectar con la API: {e}")
        return None
    except Exception as e:
        print(f"[Compras/Ventas] Error inesperado: {e}")
        return None

def get_compras_cliente(rut: str) -> Optional[List[Dict]]:
    """
    Obtiene los datos de compras mensuales para un cliente.
    
    Args:
        rut: RUT del cliente
    
    Returns:
        Lista de diccionarios con datos de compras mensuales o None si hay error
    """
    return get_cached_monthly_data(rut, 'compras')

# Código anterior comentado para referencia
# def get_compras_cliente(rut: str, periodos: int = 6):
#     try:
#         r = requests.get(f"{SII_INVOICES_BASE_URL}/compras", params={"rut": rut, "periodos": periodos})
#         r.raise_for_status()
#         return r.json()
#     except Exception as e:
#         print(f"[Compras] Error: {e}")
#         return None