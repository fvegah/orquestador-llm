import requests
import os
import json
# Importar la función auxiliar desde compras
from .compras import _get_monthly_data
from typing import Dict, List, Optional
from .monthly_data import get_cached_monthly_data

# Eliminar las variables a nivel de módulo que ya no son necesarias
# HEADERS = {
#     "Authorization": f"Bearer {os.getenv('BUSINESS_INVOICES_TOKEN')}"
# }

def get_ventas_cliente(rut: str) -> Optional[List[Dict]]:
    """
    Obtiene los datos de ventas mensuales para un cliente.
    
    Args:
        rut: RUT del cliente
    
    Returns:
        Lista de diccionarios con datos de ventas mensuales o None si hay error
    """
    return get_cached_monthly_data(rut, 'ventas')

# Código anterior comentado
# def get_ventas_cliente(rut: str, periodos: int = 6):
#     try:
#         r = requests.get(f"{SII_INVOICES_BASE_URL}/ventas", params={"rut": rut, "periodos": periodos})
#         r.raise_for_status()
#         return r.json()
#     except Exception as e:
#         print(f"[ventas] Error: {e}")
#         return None