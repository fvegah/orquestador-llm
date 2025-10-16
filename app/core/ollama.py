import requests
import os

# Comentar o eliminar la URL definida a nivel de módulo
# OLLAMA_URL = "http://ollama:11434/api/generate"
MODEL_NAME = os.getenv("OLLAMA_MODEL", "mistral")

def consultar_llm(prompt: str):
    # Leer el host de Ollama de las variables de entorno dentro de la función
    ollama_host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    ollama_url = f"{ollama_host}/api/generate"

    body = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False
    }

    try:
        print(f"[Ollama Request] URL: {ollama_url}, Model: {MODEL_NAME}")
        response = requests.post(ollama_url, json=body, timeout=180)
        response.raise_for_status()
        return response.json().get("response", "")
    except requests.exceptions.RequestException as e:
        print(f"[Ollama Connection Error] No se pudo conectar a {ollama_url}. Error: {e}")
        return f"Error: No se pudo conectar al servicio de Ollama en {ollama_host}. Verifica que esté corriendo y accesible."
    except Exception as e:
        print(f"[Ollama General Error] {e}")
        return "Hubo un error inesperado al comunicarse con Ollama."