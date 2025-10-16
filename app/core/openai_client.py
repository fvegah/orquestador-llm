import os
from openai import OpenAI, OpenAIError

# La inicialización del cliente puede leer la variable de entorno OPENAI_API_KEY automáticamente
# o puedes pasarla explícitamente: client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
# Dejar que la biblioteca lo maneje es más simple si la variable está definida.
client = OpenAI()

def consultar_openai(prompt: str) -> str:
    """Consulta la API de OpenAI Chat Completions."""
    openai_model = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        print("[OpenAI Error] La variable de entorno OPENAI_API_KEY no está definida.")
        return "Error: Falta la configuración de la API de OpenAI."

    print(f"[OpenAI Request] Model: {openai_model}") # Debug

    try:
        completion = client.chat.completions.create(
            model=openai_model,
            messages=[
                # Opcional: Mensaje de sistema para definir el rol del asistente
                # {"role": "system", "content": "Eres un útil asistente de contabilidad."},
                {"role": "user", "content": prompt}
            ],
            # Puedes ajustar otros parámetros como temperature, max_tokens, etc.
            # temperature=0.7,
        )
        # Acceder al contenido de la respuesta
        respuesta = completion.choices[0].message.content
        return respuesta.strip() if respuesta else ""

    except OpenAIError as e:
        # Manejar errores específicos de la API de OpenAI
        print(f"[OpenAI API Error] {e}")
        return f"Hubo un error al comunicarse con la API de OpenAI: {e}"
    except Exception as e:
        # Manejar otros errores inesperados
        print(f"[OpenAI General Error] {e}")
        return "Hubo un error inesperado al procesar la solicitud con OpenAI."
