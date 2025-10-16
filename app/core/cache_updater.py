import json
from app.services.compras import _get_monthly_data # Reutilizar la función que llama a la API
from .redis_client import set_cache, get_cache # Importar funciones de Redis

# --- ¡NECESITAMOS LA LISTA DE RUTS AQUÍ! ---
# Ejemplo: Hardcodear por ahora, reemplazar con la lógica real
LISTA_RUTS_NEGOCIOS = [
    "76111111-1",
    "76222222-2",
    "76637851-k",
    # ... añadir todos los RUTs necesarios ...
]
# --------------------------------------------

# Tiempo de expiración para la caché en segundos (ej: 1 día)
CACHE_EXPIRATION_SECONDS = 60 * 60 * 24 # 86400 segundos

# Construir la clave para Redis
def get_cache_key(rut: str) -> str:
    """Construye la clave estandarizada para Redis."""
    return f"business_data:{rut}"

def update_business_data(rut: str) -> bool:
    """Obtiene los datos mensuales para un RUT y los guarda en caché."""
    print(f"[Cache Updater] Actualizando datos para RUT: {rut}")
    monthly_data = _get_monthly_data(rut) # Llama a la API

    if monthly_data is not None:
        # Construir clave para la caché
        cache_key = get_cache_key(rut)
        # Guardar en Redis
        success = set_cache(cache_key, monthly_data, CACHE_EXPIRATION_SECONDS)
        if success:
            print(f"[Cache Updater] Datos para RUT {rut} guardados exitosamente.")
            return True
        else:
            print(f"[Cache Updater] Error al guardar datos para RUT {rut} en caché.")
            return False
    else:
        print(f"[Cache Updater] No se pudieron obtener datos de la API para RUT {rut}. No se actualizó caché.")
        return False

def get_business_data(rut: str):
    """
    Intenta obtener los datos de un negocio desde la caché.
    Si no están en caché, los obtiene de la API y los guarda en caché.
    """
    # Intentar leer de la caché primero
    cache_key = get_cache_key(rut)
    cached_data = get_cache(cache_key)
    
    if cached_data is not None:
        print(f"[Cache] Datos para RUT {rut} encontrados en caché.")
        return cached_data
    
    # Si no está en caché, obtener de la API y guardar en caché
    print(f"[Cache] Datos para RUT {rut} no encontrados en caché. Obteniendo de API...")
    monthly_data = _get_monthly_data(rut)
    
    if monthly_data is not None:
        # Guardar en caché para futuras consultas
        set_cache(cache_key, monthly_data, CACHE_EXPIRATION_SECONDS)
        print(f"[Cache] Datos para RUT {rut} obtenidos de API y guardados en caché.")
        return monthly_data
    else:
        print(f"[Cache] No se pudieron obtener datos para RUT {rut} de la API.")
        return None

def update_all_businesses() -> dict:
    """Actualiza los datos en caché para todos los negocios en la lista."""
    print("[Cache Updater] Iniciando actualización de caché para todos los negocios...")
    # Obtener la lista de RUTs (reemplazar con lógica real si es necesario)
    ruts_a_procesar = LISTA_RUTS_NEGOCIOS

    results = {
        "success": [],
        "failed": []
    }

    for rut in ruts_a_procesar:
        if update_business_data(rut):
            results["success"].append(rut)
        else:
            results["failed"].append(rut)

    print(f"[Cache Updater] Actualización completada. Éxitos: {len(results['success'])}, Fallos: {len(results['failed'])}")
    return results
