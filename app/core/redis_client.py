import redis
import os
import json

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Crear una instancia del cliente Redis que se pueda reutilizar
# decode_responses=True decodifica automáticamente las respuestas de bytes a strings
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping() # Verificar conexión al iniciar
    print(f"Conectado exitosamente a Redis en {REDIS_URL}")
except redis.exceptions.ConnectionError as e:
    print(f"Error al conectar con Redis en {REDIS_URL}: {e}")
    # Puedes decidir si lanzar una excepción aquí o manejarlo de otra forma
    # Por ahora, la aplicación podría continuar pero las operaciones de caché fallarán.
    redis_client = None

def set_cache(key: str, value: dict, expiration_seconds: int | None = None):
    """Almacena un diccionario en Redis como JSON string, con expiración opcional."""
    if redis_client:
        try:
            json_value = json.dumps(value) # Convertir diccionario a JSON string
            redis_client.set(key, json_value, ex=expiration_seconds)
            print(f"[Cache SET] Key: {key}, Expiration: {expiration_seconds}s")
            return True
        except Exception as e:
            print(f"[Cache SET Error] Key: {key}, Error: {e}")
            return False
    else:
        print("[Cache SET Error] Cliente Redis no disponible.")
        return False

def get_cache(key: str) -> dict | None:
    """Obtiene un valor de Redis y lo parsea desde JSON string a diccionario."""
    if redis_client:
        try:
            json_value = redis_client.get(key)
            if json_value:
                print(f"[Cache GET] Key: {key} - FOUND")
                return json.loads(json_value) # Convertir JSON string a diccionario
            else:
                print(f"[Cache GET] Key: {key} - NOT FOUND")
                return None
        except Exception as e:
            print(f"[Cache GET Error] Key: {key}, Error: {e}")
            return None
    else:
        print("[Cache GET Error] Cliente Redis no disponible.")
        return None
