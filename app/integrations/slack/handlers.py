"""Handlers para mensajes de Slack."""
import logging
import re
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class MessageHandler:
    """
    Handler para procesar mensajes de Slack.

    Funcionalidades:
    - Detectar intención de la pregunta
    - Extraer RUT si se menciona
    - Formatear respuestas para Slack
    - Manejar comandos especiales
    """

    # Patrones para detectar RUT
    RUT_PATTERN = r'\b(\d{1,2}\.?\d{3}\.?\d{3}[-]?[0-9kK])\b'

    # Comandos disponibles
    COMMANDS = {
        "ayuda": "Muestra la ayuda del bot",
        "normativa": "Busca normativas del SII",
        "utm": "Consulta valor de UTM actual",
        "uf": "Consulta valor de UF actual",
        "iva": "Consultas sobre IVA",
        "stats": "Muestra estadísticas del sistema",
    }

    def __init__(
        self,
        knowledge_base=None,
        response_cache=None,
        llm_service=None
    ):
        """
        Inicializa el handler.

        Args:
            knowledge_base: Instancia de KnowledgeBase
            response_cache: Instancia de ResponseCache
            llm_service: Función para consultar LLM
        """
        self.knowledge_base = knowledge_base
        self.response_cache = response_cache
        self.llm_service = llm_service

    def extract_rut(self, text: str) -> Optional[str]:
        """Extrae RUT de un texto si existe."""
        match = re.search(self.RUT_PATTERN, text)
        if match:
            rut = match.group(1)
            # Normalizar formato
            rut = rut.replace(".", "").replace("-", "")
            rut = f"{rut[:-1]}-{rut[-1].upper()}"
            return rut
        return None

    def detect_command(self, text: str) -> Optional[str]:
        """Detecta si el mensaje es un comando."""
        text_lower = text.lower().strip()

        for cmd in self.COMMANDS:
            if text_lower.startswith(cmd) or text_lower.startswith(f"/{cmd}"):
                return cmd

        return None

    def detect_intent(self, text: str) -> Dict[str, Any]:
        """
        Detecta la intención del mensaje.

        Returns:
            Dict con tipo de intención y parámetros detectados
        """
        text_lower = text.lower()

        intent = {
            "type": "general",
            "rut": self.extract_rut(text),
            "keywords": [],
            "is_question": "?" in text or text_lower.startswith(("qué", "cómo", "cuánto", "cuál", "dónde", "por qué"))
        }

        # Detectar tipo de consulta
        if any(word in text_lower for word in ["utm", "valor utm"]):
            intent["type"] = "valor_utm"
            intent["keywords"].append("utm")

        elif any(word in text_lower for word in ["uf", "valor uf", "unidad de fomento"]):
            intent["type"] = "valor_uf"
            intent["keywords"].append("uf")

        elif any(word in text_lower for word in ["iva", "impuesto", "débito", "crédito", "factura"]):
            intent["type"] = "consulta_iva"
            intent["keywords"].extend(["iva", "impuesto"])

        elif any(word in text_lower for word in ["circular", "resolución", "normativa", "ley"]):
            intent["type"] = "normativa"
            intent["keywords"].extend(["normativa", "circular"])

        elif any(word in text_lower for word in ["renta", "impuesto a la renta", "declaración"]):
            intent["type"] = "renta"
            intent["keywords"].append("renta")

        elif any(word in text_lower for word in ["boleta", "factura electrónica", "dte"]):
            intent["type"] = "documentos_tributarios"
            intent["keywords"].extend(["dte", "factura"])

        return intent

    async def process_message(
        self,
        text: str,
        user_id: str,
        channel_id: str,
        thread_ts: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Procesa un mensaje y genera respuesta.

        Args:
            text: Texto del mensaje
            user_id: ID del usuario de Slack
            channel_id: ID del canal
            thread_ts: Timestamp del thread (si aplica)

        Returns:
            Dict con respuesta y metadata
        """
        start_time = time.time()

        # Detectar comando
        command = self.detect_command(text)
        if command:
            return await self._handle_command(command, text)

        # Detectar intención
        intent = self.detect_intent(text)

        # Verificar cache
        if self.response_cache:
            cached = await self.response_cache.get(
                question=text,
                rut=intent.get("rut")
            )
            if cached:
                return {
                    "response": cached["response"],
                    "from_cache": True,
                    "source_documents": cached.get("source_documents", []),
                    "response_time_ms": int((time.time() - start_time) * 1000)
                }

        # Buscar contexto en knowledge base
        context_docs = []
        if self.knowledge_base:
            context_docs = await self.knowledge_base.search_semantic(
                query=text,
                limit=3
            )

        # Generar respuesta con LLM
        response = await self._generate_response(text, intent, context_docs)

        # Cachear respuesta
        if self.response_cache and response:
            await self.response_cache.set(
                question=text,
                response=response,
                rut=intent.get("rut"),
                source_documents=context_docs
            )

        # Guardar conversación
        if self.knowledge_base:
            await self.knowledge_base.save_conversation(
                question=text,
                response=response,
                slack_user_id=user_id,
                slack_channel_id=channel_id,
                slack_thread_ts=thread_ts,
                source_documents=context_docs,
                response_time_ms=int((time.time() - start_time) * 1000),
                from_cache=False
            )

        return {
            "response": response,
            "from_cache": False,
            "intent": intent,
            "source_documents": context_docs,
            "response_time_ms": int((time.time() - start_time) * 1000)
        }

    async def _handle_command(self, command: str, text: str) -> Dict[str, Any]:
        """Maneja comandos especiales."""
        if command == "ayuda":
            help_text = "*Comandos disponibles:*\n\n"
            for cmd, desc in self.COMMANDS.items():
                help_text += f"• `{cmd}` - {desc}\n"
            help_text += "\n_También puedes hacerme preguntas directamente sobre contabilidad y tributación._"
            return {"response": help_text, "from_cache": False}

        elif command == "stats":
            stats = await self._get_stats()
            return {"response": stats, "from_cache": False}

        elif command in ["utm", "uf"]:
            # Buscar valor actual
            if self.knowledge_base:
                docs = await self.knowledge_base.search_keyword(
                    keyword=command.upper(),
                    doc_type="tabla_valores",
                    limit=1
                )
                if docs:
                    return {
                        "response": f"*Valor {command.upper()}:*\n{docs[0].get('content_preview', 'No disponible')}",
                        "from_cache": False
                    }

            return {
                "response": f"No tengo información actualizada sobre {command.upper()}. Puedes consultar en https://www.sii.cl",
                "from_cache": False
            }

        return {"response": "Comando no reconocido. Usa `ayuda` para ver los comandos disponibles.", "from_cache": False}

    async def _generate_response(
        self,
        question: str,
        intent: Dict[str, Any],
        context_docs: List[Dict]
    ) -> str:
        """Genera respuesta usando LLM con contexto."""
        if not self.llm_service:
            return "Lo siento, el servicio de IA no está disponible en este momento."

        # Construir prompt con contexto
        prompt = self._build_prompt(question, intent, context_docs)

        try:
            response = await self.llm_service(prompt)
            return response
        except Exception as e:
            logger.error(f"Error generando respuesta: {e}")
            return "Lo siento, hubo un error procesando tu consulta. Por favor intenta nuevamente."

    def _build_prompt(
        self,
        question: str,
        intent: Dict[str, Any],
        context_docs: List[Dict]
    ) -> str:
        """Construye el prompt para el LLM."""
        system_prompt = """Eres un asistente experto en contabilidad y tributación chilena.
Tu rol es responder preguntas sobre:
- Impuestos (IVA, Renta, etc.)
- Normativas del SII
- Cálculos tributarios
- Documentos tributarios electrónicos

Responde de forma clara, concisa y profesional.
Si no tienes información suficiente, indícalo claramente.
Siempre menciona las fuentes cuando sea relevante."""

        context = ""
        if context_docs:
            context = "\n\n*Información relevante de la base de conocimientos:*\n"
            for i, doc in enumerate(context_docs, 1):
                context += f"\n[{i}] {doc.get('title', 'Sin título')}\n"
                context += f"Tipo: {doc.get('doc_type', 'N/A')}\n"
                content = doc.get('content', '')[:500]
                context += f"Contenido: {content}...\n"

        rut_info = ""
        if intent.get("rut"):
            rut_info = f"\n*El usuario está consultando sobre RUT: {intent['rut']}*\n"

        full_prompt = f"""{system_prompt}
{context}
{rut_info}
*Pregunta del usuario:*
{question}

*Respuesta:*"""

        return full_prompt

    async def _get_stats(self) -> str:
        """Obtiene estadísticas formateadas para Slack."""
        stats_text = "*Estadísticas del Sistema*\n\n"

        if self.knowledge_base:
            kb_stats = await self.knowledge_base.get_document_stats()
            stats_text += f"*Base de conocimientos:*\n"
            stats_text += f"• Total documentos: {kb_stats.get('total_documents', 0)}\n"

        if self.response_cache:
            cache_stats = await self.response_cache.get_stats()
            stats_text += f"\n*Cache de respuestas:*\n"
            stats_text += f"• Respuestas cacheadas: {cache_stats.get('total_cached_responses', 0)}\n"
            stats_text += f"• Tasa de aciertos: {cache_stats.get('hit_rate', 0)}%\n"

        return stats_text

    def format_response_for_slack(self, response: str, source_docs: List[Dict] = None) -> Dict[str, Any]:
        """
        Formatea respuesta para Slack con bloques.

        Args:
            response: Texto de respuesta
            source_docs: Documentos fuente

        Returns:
            Dict con bloques de Slack
        """
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": response
                }
            }
        ]

        # Agregar fuentes si existen
        if source_docs:
            blocks.append({"type": "divider"})
            sources_text = "*Fuentes:*\n"
            for doc in source_docs[:3]:  # Máximo 3 fuentes
                title = doc.get('title', 'Documento')[:50]
                url = doc.get('url', '')
                if url:
                    sources_text += f"• <{url}|{title}>\n"
                else:
                    sources_text += f"• {title}\n"

            blocks.append({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": sources_text
                    }
                ]
            })

        return {"blocks": blocks}
