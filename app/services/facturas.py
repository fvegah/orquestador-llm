from typing import Dict, List, Optional
from datetime import datetime, timedelta
import os
import json
import httpx
from redis import Redis

from ..core.cache_updater import get_redis_client

# Configuración del servicio de facturas
FACTURAS_API_URL = os.getenv("FACTURAS_API_URL", "http://localhost:5000")
FACTURAS_API_KEY = os.getenv("FACTURAS_API_KEY", "")

async def get_facturas_cliente(
    rut: str,
    page: int = 1,
    per_page: int = 100,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> Dict:
    """
    Obtiene las facturas de un cliente con paginación y caché.
    
    Args:
        rut: RUT del cliente
        page: Número de página (1-based)
        per_page: Cantidad de documentos por página
        start_date: Fecha inicial para filtrar
        end_date: Fecha final para filtrar
    
    Returns:
        Dict con las facturas y metadata de paginación
    """
    # Construir key para caché
    cache_key = f"facturas:{rut}:p{page}:pp{per_page}"
    if start_date:
        cache_key += f":sd{start_date.strftime('%Y%m%d')}"
    if end_date:
        cache_key += f":ed{end_date.strftime('%Y%m%d')}"
    
    # Intentar obtener de caché
    redis_client = get_redis_client()
    cached_data = redis_client.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    # Preparar parámetros para la API
    params = {
        "rut": rut,
        "page": page,
        "per_page": per_page
    }
    
    if start_date:
        params["start_date"] = start_date.strftime("%Y-%m-%d")
    if end_date:
        params["end_date"] = end_date.strftime("%Y-%m-%d")
    
    # Headers para la API
    headers = {
        "X-API-Key": FACTURAS_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        # Hacer request a la API de facturas
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{FACTURAS_API_URL}/api/facturas",
                params=params,
                headers=headers
            )
            response.raise_for_status()
            data = response.json()
            
            # Guardar en caché por 5 minutos
            redis_client.setex(
                cache_key,
                timedelta(minutes=5),
                json.dumps(data)
            )
            
            return data
            
    except httpx.HTTPError as e:
        print(f"Error al obtener facturas desde la API: {e}")
        return {
            "facturas": [],
            "metadata": {
                "page": page,
                "per_page": per_page,
                "total_docs": 0,
                "total_pages": 0
            }
        }

async def get_resumen_facturas(
    rut: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> Dict:
    """
    Obtiene un resumen agregado de las facturas de un cliente.
    
    Args:
        rut: RUT del cliente
        start_date: Fecha inicial para filtrar
        end_date: Fecha final para filtrar
    
    Returns:
        Dict con estadísticas agregadas
    """
    # Construir key para caché
    cache_key = f"facturas:resumen:{rut}"
    if start_date:
        cache_key += f":sd{start_date.strftime('%Y%m%d')}"
    if end_date:
        cache_key += f":ed{end_date.strftime('%Y%m%d')}"
    
    # Intentar obtener de caché
    redis_client = get_redis_client()
    cached_data = redis_client.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    # Preparar parámetros para la API
    params = {"rut": rut}
    if start_date:
        params["start_date"] = start_date.strftime("%Y-%m-%d")
    if end_date:
        params["end_date"] = end_date.strftime("%Y-%m-%d")
    
    # Headers para la API
    headers = {
        "X-API-Key": FACTURAS_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        # Hacer request a la API de facturas
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{FACTURAS_API_URL}/api/facturas/resumen",
                params=params,
                headers=headers
            )
            response.raise_for_status()
            data = response.json()
            
            # Guardar en caché por 15 minutos
            redis_client.setex(
                cache_key,
                timedelta(minutes=15),
                json.dumps(data)
            )
            
            return data
            
    except httpx.HTTPError as e:
        print(f"Error al obtener resumen de facturas desde la API: {e}")
        return {
            "total_facturas": 0,
            "monto_total": 0,
            "promedio_monto": 0,
            "max_monto": 0,
            "min_monto": 0,
            "facturas_por_mes": []
        }

async def actualizar_cache_facturas(rut: str) -> None:
    """
    Actualiza la caché de facturas para un cliente específico.
    """
    # Limpiar todas las claves de caché relacionadas con este RUT
    redis_client = get_redis_client()
    pattern = f"facturas:{rut}:*"
    
    # Obtener todas las claves que coinciden con el patrón
    keys = redis_client.keys(pattern)
    
    # Eliminar todas las claves encontradas
    if keys:
        redis_client.delete(*keys)
    
    # Pre-cachear la primera página y el resumen
    await get_facturas_cliente(rut, page=1)
    await get_resumen_facturas(rut) 