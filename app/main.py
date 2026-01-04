"""
Orquestador LLM - Sistema de Contabilidad Inteligente
=====================================================

Aplicación principal que integra:
- Scraping del SII
- Knowledge Base con búsqueda semántica
- Cache de respuestas IA
- Bot de Slack
- API REST para consultas
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse

# --- Cargar .env PRIMERO ---
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / '.env'
load_dotenv(dotenv_path=ENV_PATH)

# --- Configurar logging ---
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Importar módulos de la app ---
from app.core.ollama import consultar_llm
from app.core.openai_client import consultar_openai
from app.core.prompts import generar_prompt_iva
from app.services.compras import get_compras_cliente
from app.services.ventas import get_ventas_cliente

# Nuevos módulos
from app.orchestrator import Orchestrator, SIIDataSource
from app.knowledge import KnowledgeBase, EmbeddingService
from app.cache import ResponseCache

# --- Configuración Global ---
LLM_SERVICE = os.getenv("LLM_SERVICE", "ollama").lower()
ENABLE_SLACK_BOT = os.getenv("SLACK_BOT_TOKEN") is not None
SCRAPING_SCHEDULE = os.getenv("SCRAPING_SCHEDULE", "0 2 * * *")

# --- Instancias Globales ---
knowledge_base: Optional[KnowledgeBase] = None
response_cache: Optional[ResponseCache] = None
orchestrator: Optional[Orchestrator] = None


async def get_llm_response(prompt: str) -> str:
    """Función helper para obtener respuesta del LLM configurado."""
    if LLM_SERVICE == "openai":
        return consultar_openai(prompt)
    else:
        return consultar_llm(prompt)


async def get_llm_response_async(prompt: str) -> str:
    """Versión async del LLM para el bot de Slack."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: get_llm_response_sync(prompt))


def get_llm_response_sync(prompt: str) -> str:
    """Versión síncrona para uso en executor."""
    if LLM_SERVICE == "openai":
        return consultar_openai(prompt)
    else:
        return consultar_llm(prompt)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestión del ciclo de vida de la aplicación."""
    global knowledge_base, response_cache, orchestrator

    logger.info("Iniciando aplicación...")
    logger.info(f"LLM Service: {LLM_SERVICE}")

    # --- Inicializar Knowledge Base ---
    try:
        embedding_service = EmbeddingService(
            provider="ollama" if LLM_SERVICE == "ollama" else "openai"
        )
        knowledge_base = KnowledgeBase(embedding_service=embedding_service)
        await knowledge_base.connect()
        await knowledge_base.initialize_schema()
        logger.info("Knowledge Base inicializada")
    except Exception as e:
        logger.warning(f"No se pudo inicializar Knowledge Base: {e}")
        knowledge_base = None

    # --- Inicializar Response Cache ---
    try:
        response_cache = ResponseCache(
            db_pool=knowledge_base._pool if knowledge_base else None
        )
        await response_cache.connect()
        logger.info("Response Cache inicializado")
    except Exception as e:
        logger.warning(f"No se pudo inicializar Response Cache: {e}")
        response_cache = None

    # --- Inicializar Orquestador ---
    try:
        orchestrator = Orchestrator(
            knowledge_base=knowledge_base,
            response_cache=response_cache
        )

        # Registrar fuente SII con schedule
        sii_source = SIIDataSource(max_documents=50)
        if knowledge_base:
            sii_source.set_knowledge_base(knowledge_base)

        orchestrator.register_source(
            source=sii_source,
            schedule_cron=SCRAPING_SCHEDULE
        )

        orchestrator.start()
        logger.info("Orquestador iniciado")
    except Exception as e:
        logger.warning(f"No se pudo inicializar Orquestador: {e}")
        orchestrator = None

    # --- Inicializar Slack Bot (en background) ---
    slack_task = None
    if ENABLE_SLACK_BOT:
        try:
            from app.integrations.slack import SlackBot
            slack_bot = SlackBot(
                knowledge_base=knowledge_base,
                response_cache=response_cache,
                llm_service=get_llm_response_async
            )
            slack_task = asyncio.create_task(slack_bot.start())
            logger.info("Slack Bot iniciado")
        except Exception as e:
            logger.warning(f"No se pudo inicializar Slack Bot: {e}")

    # --- Iniciar Kafka Consumer (código existente) ---
    try:
        from app.core.kafka_consumer import start_kafka_consumer
        start_kafka_consumer()
        logger.info("Kafka Consumer iniciado")
    except Exception as e:
        logger.warning(f"No se pudo iniciar Kafka Consumer: {e}")

    yield  # La aplicación está corriendo

    # --- Cleanup al cerrar ---
    logger.info("Cerrando aplicación...")

    if orchestrator:
        orchestrator.stop()

    if slack_task:
        slack_task.cancel()

    if response_cache:
        await response_cache.disconnect()

    if knowledge_base:
        await knowledge_base.disconnect()

    try:
        from app.core.kafka_consumer import stop_kafka_consumer
        stop_kafka_consumer()
    except Exception:
        pass

    logger.info("Aplicación cerrada")


# --- Crear aplicación FastAPI ---
app = FastAPI(
    title="Orquestador LLM - Sistema de Contabilidad",
    description="API para consultas de contabilidad y tributación chilena con IA",
    version="2.0.0",
    lifespan=lifespan
)


# ============================================
# ENDPOINTS DE SALUD
# ============================================

@app.get("/health")
async def health_check():
    """Endpoint de health check."""
    return {
        "status": "healthy",
        "llm_service": LLM_SERVICE,
        "knowledge_base": knowledge_base is not None,
        "response_cache": response_cache is not None,
        "orchestrator": orchestrator is not None
    }


@app.get("/status")
async def get_status():
    """Obtiene el estado completo del sistema."""
    status = {
        "llm_service": LLM_SERVICE,
        "slack_enabled": ENABLE_SLACK_BOT,
        "scraping_schedule": SCRAPING_SCHEDULE
    }

    if orchestrator:
        status["orchestrator"] = orchestrator.get_status()

    if response_cache:
        status["cache"] = await response_cache.get_stats()

    if knowledge_base:
        status["knowledge_base"] = await knowledge_base.get_document_stats()

    return status


# ============================================
# ENDPOINTS DE CONSULTA
# ============================================

@app.post("/preguntar")
async def preguntar(request: Request):
    """
    Endpoint principal para preguntas de contabilidad.

    Flujo:
    1. Verificar cache de respuestas
    2. Buscar contexto en knowledge base
    3. Generar respuesta con LLM
    4. Cachear respuesta
    """
    body = await request.json()
    rut = body.get("rut")
    pregunta = body.get("pregunta")

    if not pregunta:
        raise HTTPException(status_code=400, detail="El campo 'pregunta' es requerido")

    # 1. Verificar cache
    if response_cache:
        cached = await response_cache.get(question=pregunta, rut=rut)
        if cached:
            logger.info(f"Cache HIT para pregunta: {pregunta[:50]}...")
            return {
                "rut": rut,
                "pregunta": pregunta,
                "respuesta": cached["response"],
                "from_cache": True,
                "source_documents": cached.get("source_documents", [])
            }

    # 2. Buscar contexto en knowledge base
    context_docs = []
    if knowledge_base:
        context_docs = await knowledge_base.search_semantic(
            query=pregunta,
            limit=3,
            source="sii"
        )

    # 3. Generar prompt con contexto
    compras = get_compras_cliente(rut) if rut else []
    ventas = get_ventas_cliente(rut) if rut else []

    # Agregar contexto de knowledge base al prompt
    kb_context = ""
    if context_docs:
        kb_context = "\n\nInformación relevante del SII:\n"
        for doc in context_docs:
            kb_context += f"- {doc['title']}: {doc['content'][:300]}...\n"

    prompt = generar_prompt_iva(rut, compras, ventas, pregunta)
    prompt = prompt.replace("*Pregunta:*", f"{kb_context}\n\n*Pregunta:*")

    # 4. Obtener respuesta del LLM
    respuesta = await get_llm_response(prompt)

    # 5. Cachear respuesta
    if response_cache:
        await response_cache.set(
            question=pregunta,
            response=respuesta,
            rut=rut,
            source_documents=context_docs
        )

    # 6. Guardar conversación
    if knowledge_base:
        await knowledge_base.save_conversation(
            question=pregunta,
            response=respuesta,
            source_documents=context_docs
        )

    return {
        "rut": rut,
        "pregunta": pregunta,
        "respuesta": respuesta,
        "from_cache": False,
        "source_documents": [{"title": d["title"], "url": d.get("url")} for d in context_docs]
    }


@app.post("/buscar")
async def buscar_documentos(request: Request):
    """Busca documentos en la knowledge base."""
    body = await request.json()
    query = body.get("query")
    doc_type = body.get("doc_type")
    limit = body.get("limit", 5)

    if not query:
        raise HTTPException(status_code=400, detail="El campo 'query' es requerido")

    if not knowledge_base:
        raise HTTPException(status_code=503, detail="Knowledge Base no disponible")

    # Búsqueda semántica
    results = await knowledge_base.search_semantic(
        query=query,
        limit=limit,
        doc_type=doc_type
    )

    return {
        "query": query,
        "results": results,
        "total": len(results)
    }


# ============================================
# ENDPOINTS DE ADMINISTRACIÓN
# ============================================

@app.post("/admin/scrape")
async def admin_trigger_scrape(request: Request, background_tasks: BackgroundTasks):
    """Dispara scraping manualmente."""
    body = await request.json()
    source = body.get("source", "sii")

    if not orchestrator:
        raise HTTPException(status_code=503, detail="Orquestador no disponible")

    async def run_scrape():
        result = await orchestrator.run_source_now(source)
        logger.info(f"Scraping completado: {result.documents_count} documentos")

    background_tasks.add_task(run_scrape)

    return {
        "message": f"Scraping de '{source}' iniciado en background",
        "status": "running"
    }


@app.post("/admin/update-cache")
async def admin_update_cache(request: Request, background_tasks: BackgroundTasks):
    """Endpoint administrativo para actualizar la caché manualmente."""
    from app.core.cache_updater import update_business_data, update_all_businesses

    data = await request.json()

    if "rut" in data:
        rut = data["rut"]
        background_tasks.add_task(update_business_data, rut)
        return {"message": f"Actualizando caché para RUT {rut} en background."}
    elif data.get("all") is True:
        background_tasks.add_task(update_all_businesses)
        return {"message": "Actualizando caché para todos los negocios en background."}
    else:
        raise HTTPException(
            status_code=400,
            detail="Proporciona 'rut' para actualizar un negocio o 'all': true para todos"
        )


@app.post("/admin/cache/invalidate")
async def admin_invalidate_cache(request: Request):
    """Invalida entradas del cache."""
    body = await request.json()
    pattern = body.get("pattern", "response_cache:*")

    if not response_cache:
        raise HTTPException(status_code=503, detail="Response Cache no disponible")

    count = await response_cache.invalidate(pattern=pattern)

    return {
        "message": f"Cache invalidado",
        "entries_removed": count
    }


@app.get("/admin/jobs")
async def admin_list_jobs():
    """Lista los jobs programados."""
    if not orchestrator:
        raise HTTPException(status_code=503, detail="Orquestador no disponible")

    return {
        "sources": orchestrator.list_sources(),
        "jobs": orchestrator.scheduler.get_jobs()
    }


# ============================================
# MANEJO DE ERRORES
# ============================================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Manejador global de excepciones."""
    logger.error(f"Error no manejado: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Error interno del servidor",
            "detail": str(exc) if os.getenv("DEBUG") else "Contacte al administrador"
        }
    )
