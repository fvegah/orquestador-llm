import os
import json
from typing import Dict, Optional, List
from datetime import datetime, timedelta
import requests
from redis import Redis

# Configuración
API_URL = os.getenv("MONTHLY_SALES_API_URL", "http://localhost:5001")
API_TOKEN = os.getenv("BUSINESS_INVOICES_TOKEN")

def get_redis_client() -> Redis:
    """
    Obtiene una instancia del cliente Redis.
    """
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    
    return Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        decode_responses=True
    )

def _get_monthly_data_from_api(rut: str) -> Optional[List[Dict]]:
    """
    Obtiene los datos mensuales directamente de la API.
    
    Args:
        rut: RUT del cliente
    
    Returns:
        Lista de diccionarios con datos mensuales o None si hay error
    """
    if not API_TOKEN:
        print("[Monthly Data] Error: La variable de entorno BUSINESS_INVOICES_TOKEN no está definida.")
        return None

    headers = {
        "Authorization": f"Token {API_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        url = f"{API_URL}/business/{rut}/monthly_sales"
        print(f"[Monthly Data] URL: {url}")
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        
        if data.get("status") == "ok" and "total_last_months" in data:
            return data["total_last_months"]
        else:
            print(f"[Monthly Data] Respuesta inesperada de la API: {data}")
            return None
            
    except requests.RequestException as e:
        print(f"[Monthly Data] Error al conectar con la API: {e}")
        return None
    except Exception as e:
        print(f"[Monthly Data] Error inesperado: {e}")
        return None

def get_business_data(rut: str) -> Optional[List[Dict]]:
    """
    Obtiene los datos del negocio, primero intentando desde la caché,
    y si no están disponibles, desde la API.
    
    Args:
        rut: RUT del cliente
    
    Returns:
        Lista de diccionarios con datos mensuales o None si hay error
    """
    redis_client = get_redis_client()
    cache_key = f"business:monthly:{rut}"
    
    # Intentar obtener de caché
    cached_data = redis_client.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    # Si no está en caché, obtener de la API
    data = _get_monthly_data_from_api(rut)
    
    # Si obtuvimos datos, guardar en caché
    if data is not None:
        redis_client.setex(
            cache_key,
            timedelta(minutes=15),
            json.dumps(data)
        )
    
    return data

def get_cached_monthly_data(rut: str, data_type: str) -> Dict:
    """
    Obtiene y procesa los datos mensuales específicos (compras o ventas).
    
    Args:
        rut: RUT del cliente
        data_type: Tipo de datos ('compras' o 'ventas')
    
    Returns:
        Dict con los datos procesados del tipo especificado
    """
    # Validar tipo de datos
    if data_type not in ['compras', 'ventas']:
        raise ValueError("data_type debe ser 'compras' o 'ventas'")
    
    # Obtener datos
    monthly_data = get_business_data(rut)
    
    if monthly_data is None:
        print(f"[Monthly Data] No se pudieron obtener datos para RUT {rut}")
        return None
    
    # Procesar datos según el tipo
    result_data = []
    for month in monthly_data:
        if data_type == 'compras':
            info = {
                "period": month.get("period"),
                "total_purchases": month.get("total_purchases"),
                "total_purchases_discount_document": month.get("total_purchases_discount_document"),
                "total_purchases_exempt": month.get("total_purchases_exempt"),
                "total_purchases_iva": month.get("total_purchases_iva"),
                "total_purchases_net_with_exempt_purchases": month.get("total_purchases_net_with_exempt_purchases"),
                "total_purchases_neto": month.get("total_purchases_neto"),
                "total_purchases_tax_common_use": month.get("total_purchases_tax_common_use"),
                "total_purchases_tax_no_recoverable": month.get("total_purchases_tax_no_recoverable"),
                "total_purchases_tax_recoverable": month.get("total_purchases_tax_recoverable"),
            }
        else:  # ventas
            info = {
                "period": month.get("period"),
                "total_sales": month.get("total_sales"),
                "total_sales_discount_document": month.get("total_sales_discount_document"),
                "total_sales_exempt": month.get("total_sales_exempt"),
                "total_sales_iva": month.get("total_sales_iva"),
                "total_sales_net_with_exempt_sales": month.get("total_sales_net_with_exempt_sales"),
                "total_sales_neto": month.get("total_sales_neto"),
                "total_sales_tax_common_use": month.get("total_sales_tax_common_use"),
                "total_sales_tax_no_recoverable": month.get("total_sales_tax_no_recoverable"),
                "total_sales_tax_recoverable": month.get("total_sales_tax_recoverable"),
            }
        result_data.append(info)
    
    return result_data 