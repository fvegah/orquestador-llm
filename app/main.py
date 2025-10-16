from fastapi import FastAPI, Request, BackgroundTasks
from dotenv import load_dotenv
import os
from pathlib import Path

# --- Cargar .env PRIMERO ---
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / '.env'
print(f"Loading .env file from: {ENV_PATH}")
loaded = load_dotenv(dotenv_path=ENV_PATH)
print(f".env file loaded: {loaded}") # Verificar si load_dotenv tuvo éxito
print(f"BUSINESS_INVOICES_TOKEN loaded: {os.getenv('BUSINESS_INVOICES_TOKEN') is not None}")
# --- Fin Carga .env ---

# --- Importar módulos de la app DESPUÉS de cargar .env ---
from app.core.ollama import consultar_llm
from app.core.openai_client import consultar_openai
from app.services.compras import get_compras_cliente
from app.services.ventas import get_ventas_cliente
from app.core.prompts import generar_prompt_iva
from app.core.kafka_consumer import start_kafka_consumer, stop_kafka_consumer
from app.core.cache_updater import update_business_data, update_all_businesses
# --- Fin Importaciones ---

app = FastAPI()

# --- Leer configuración del servicio LLM ---
LLM_SERVICE = os.getenv("LLM_SERVICE", "ollama").lower() # Default a ollama si no está definida
print(f"Using LLM service: {LLM_SERVICE}") # Debug
# --- Fin Configuración ---

# --- Eventos de inicio y parada de la aplicación ---
@app.on_event("startup")
def startup_event():
    """Se ejecuta al iniciar la aplicación FastAPI"""
    print("Iniciando la aplicación...")
    
    # Iniciar el consumidor de Kafka
    try:
        print("Iniciando consumidor de Kafka...")
        start_kafka_consumer()
        print("Consumidor de Kafka iniciado con éxito.")
    except Exception as e:
        print(f"Error al iniciar el consumidor de Kafka: {e}")

@app.on_event("shutdown")
def shutdown_event():
    """Se ejecuta al detener la aplicación FastAPI"""
    print("Deteniendo la aplicación...")
    
    # Detener el consumidor de Kafka
    try:
        print("Deteniendo consumidor de Kafka...")
        stop_kafka_consumer()
        print("Consumidor de Kafka detenido con éxito.")
    except Exception as e:
        print(f"Error al detener el consumidor de Kafka: {e}")
# --- Fin Eventos ---

@app.post("/preguntar")
async def preguntar(request: Request):
    body = await request.json()
    rut = body.get("rut")
    pregunta = body.get("pregunta")

    # Obtener datos de microservicios
    compras = get_compras_cliente(rut)
    ventas = get_ventas_cliente(rut)

    # Generar prompt
    prompt = generar_prompt_iva(rut, compras, ventas, pregunta)

    # --- Seleccionar y enviar al servicio LLM configurado ---
    respuesta = ""
    if LLM_SERVICE == "openai":
        print("Routing to OpenAI...")
        respuesta = consultar_openai(prompt)
    elif LLM_SERVICE == "ollama":
        print("Routing to Ollama...")
        respuesta = consultar_llm(prompt)
    else:
        print(f"Error: Servicio LLM desconocido: {LLM_SERVICE}")
        respuesta = f"Error: Servicio LLM '{LLM_SERVICE}' no configurado correctamente."
    # --- Fin Selección LLM ---

    return {
        "rut": rut,
        "pregunta": pregunta,
        "respuesta": respuesta
    }

@app.post("/admin/update-cache")
async def admin_update_cache(request: Request, background_tasks: BackgroundTasks):
    """Endpoint administrativo para actualizar la caché manualmente (en background)."""
    data = await request.json()
    
    if "rut" in data:
        rut = data["rut"]
        # Actualizar un solo RUT en background
        background_tasks.add_task(update_business_data, rut)
        return {"message": f"Actualizando caché para RUT {rut} en background."}
    elif data.get("all") is True:
        # Actualizar todos los RUTs en background
        background_tasks.add_task(update_all_businesses)
        return {"message": "Actualizando caché para todos los negocios en background."}
    else:
        return {"error": "Formato incorrecto. Proporciona 'rut' para actualizar un negocio o 'all': true para actualizar todos."}