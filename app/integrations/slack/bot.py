"""Bot de Slack para consultas de contabilidad."""
import logging
import os
import re
from typing import Optional

from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

from .handlers import MessageHandler

logger = logging.getLogger(__name__)


class SlackBot:
    """
    Bot de Slack para responder consultas de contabilidad.

    Funcionalidades:
    - Responde a menciones directas
    - Comandos slash (/consulta, /normativa, etc.)
    - Mensajes en canales específicos
    - Threads para conversaciones largas
    """

    def __init__(
        self,
        bot_token: Optional[str] = None,
        app_token: Optional[str] = None,
        signing_secret: Optional[str] = None,
        knowledge_base=None,
        response_cache=None,
        llm_service=None
    ):
        """
        Inicializa el bot de Slack.

        Args:
            bot_token: Token del bot (xoxb-...)
            app_token: Token de la app para Socket Mode (xapp-...)
            signing_secret: Signing secret de Slack
            knowledge_base: Instancia de KnowledgeBase
            response_cache: Instancia de ResponseCache
            llm_service: Función async para consultar LLM
        """
        self.bot_token = bot_token or os.getenv("SLACK_BOT_TOKEN")
        self.app_token = app_token or os.getenv("SLACK_APP_TOKEN")
        self.signing_secret = signing_secret or os.getenv("SLACK_SIGNING_SECRET")

        if not self.bot_token:
            raise ValueError("SLACK_BOT_TOKEN es requerido")

        # Inicializar Slack Bolt App
        self.app = AsyncApp(
            token=self.bot_token,
            signing_secret=self.signing_secret
        )

        # Inicializar handler de mensajes
        self.handler = MessageHandler(
            knowledge_base=knowledge_base,
            response_cache=response_cache,
            llm_service=llm_service
        )

        # Registrar handlers
        self._register_handlers()

        self._socket_handler: Optional[AsyncSocketModeHandler] = None
        self._bot_user_id: Optional[str] = None

    def _register_handlers(self):
        """Registra todos los handlers de eventos."""

        # Handler para menciones al bot
        @self.app.event("app_mention")
        async def handle_mention(event, say, client):
            await self._handle_message(event, say, client)

        # Handler para mensajes directos
        @self.app.event("message")
        async def handle_message(event, say, client):
            # Ignorar mensajes del bot mismo
            if event.get("bot_id") or event.get("subtype"):
                return

            # Solo responder en DMs o si fue mencionado
            channel_type = event.get("channel_type", "")
            if channel_type == "im":
                await self._handle_message(event, say, client)

        # Comando /consulta
        @self.app.command("/consulta")
        async def handle_consulta_command(ack, command, respond):
            await ack()
            text = command.get("text", "").strip()

            if not text:
                await respond("Por favor, incluye tu consulta. Ejemplo: `/consulta ¿Cómo calculo el IVA?`")
                return

            result = await self.handler.process_message(
                text=text,
                user_id=command["user_id"],
                channel_id=command["channel_id"]
            )

            formatted = self.handler.format_response_for_slack(
                result["response"],
                result.get("source_documents")
            )

            await respond(**formatted)

        # Comando /normativa
        @self.app.command("/normativa")
        async def handle_normativa_command(ack, command, respond):
            await ack()
            query = command.get("text", "").strip()

            if not query:
                await respond("Por favor, indica qué normativa buscas. Ejemplo: `/normativa circular IVA`")
                return

            result = await self.handler.process_message(
                text=f"Buscar normativa: {query}",
                user_id=command["user_id"],
                channel_id=command["channel_id"]
            )

            formatted = self.handler.format_response_for_slack(
                result["response"],
                result.get("source_documents")
            )

            await respond(**formatted)

        # Comando /iva
        @self.app.command("/iva")
        async def handle_iva_command(ack, command, respond):
            await ack()
            query = command.get("text", "").strip()

            if not query:
                await respond("¿Qué necesitas saber sobre IVA? Ejemplo: `/iva ¿cómo calcular débito fiscal?`")
                return

            result = await self.handler.process_message(
                text=f"Consulta IVA: {query}",
                user_id=command["user_id"],
                channel_id=command["channel_id"]
            )

            formatted = self.handler.format_response_for_slack(
                result["response"],
                result.get("source_documents")
            )

            await respond(**formatted)

        # Comando /ayuda
        @self.app.command("/ayuda")
        async def handle_help_command(ack, respond):
            await ack()
            help_message = {
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "Asistente de Contabilidad"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Soy tu asistente para consultas de contabilidad y tributación chilena."
                        }
                    },
                    {
                        "type": "divider"
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Comandos disponibles:*"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": "`/consulta [pregunta]`\nRealiza cualquier consulta contable"},
                            {"type": "mrkdwn", "text": "`/normativa [búsqueda]`\nBusca normativas del SII"},
                            {"type": "mrkdwn", "text": "`/iva [pregunta]`\nConsultas específicas de IVA"},
                            {"type": "mrkdwn", "text": "`/ayuda`\nMuestra esta ayuda"}
                        ]
                    },
                    {
                        "type": "divider"
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": "También puedes mencionarme (@bot) o enviarme un mensaje directo."
                            }
                        ]
                    }
                ]
            }
            await respond(**help_message)

        # Handler para acciones de botones (si se agregan en el futuro)
        @self.app.action(re.compile(r"^action_.*"))
        async def handle_action(ack, body, respond):
            await ack()
            action_id = body["actions"][0]["action_id"]
            logger.info(f"Acción recibida: {action_id}")

    async def _handle_message(self, event, say, client):
        """Procesa un mensaje y responde."""
        text = event.get("text", "")
        user_id = event.get("user")
        channel_id = event.get("channel")
        thread_ts = event.get("thread_ts") or event.get("ts")

        # Remover mención del bot del texto
        if self._bot_user_id:
            text = re.sub(f"<@{self._bot_user_id}>", "", text).strip()

        if not text:
            return

        # Mostrar indicador de escritura
        try:
            await client.reactions_add(
                channel=channel_id,
                timestamp=event.get("ts"),
                name="hourglass_flowing_sand"
            )
        except Exception:
            pass

        # Procesar mensaje
        result = await self.handler.process_message(
            text=text,
            user_id=user_id,
            channel_id=channel_id,
            thread_ts=thread_ts
        )

        # Formatear respuesta
        formatted = self.handler.format_response_for_slack(
            result["response"],
            result.get("source_documents")
        )

        # Enviar respuesta en thread
        await say(
            **formatted,
            thread_ts=thread_ts
        )

        # Quitar indicador de escritura
        try:
            await client.reactions_remove(
                channel=channel_id,
                timestamp=event.get("ts"),
                name="hourglass_flowing_sand"
            )
            # Agregar checkmark si fue desde cache
            if result.get("from_cache"):
                await client.reactions_add(
                    channel=channel_id,
                    timestamp=event.get("ts"),
                    name="zap"  # Rayo para indicar respuesta rápida del cache
                )
        except Exception:
            pass

    async def start(self):
        """Inicia el bot usando Socket Mode."""
        if not self.app_token:
            raise ValueError("SLACK_APP_TOKEN es requerido para Socket Mode")

        # Obtener ID del bot
        try:
            auth_response = await self.app.client.auth_test()
            self._bot_user_id = auth_response["user_id"]
            logger.info(f"Bot conectado como: {auth_response['user']}")
        except Exception as e:
            logger.error(f"Error en auth_test: {e}")

        # Iniciar Socket Mode handler
        self._socket_handler = AsyncSocketModeHandler(self.app, self.app_token)
        logger.info("Iniciando Slack Bot en Socket Mode...")
        await self._socket_handler.start_async()

    async def stop(self):
        """Detiene el bot."""
        if self._socket_handler:
            await self._socket_handler.close_async()
            logger.info("Slack Bot detenido")


# Función helper para crear y ejecutar el bot
async def run_slack_bot(
    knowledge_base=None,
    response_cache=None,
    llm_service=None
):
    """
    Función helper para ejecutar el bot de Slack.

    Args:
        knowledge_base: Instancia de KnowledgeBase
        response_cache: Instancia de ResponseCache
        llm_service: Función async para consultar LLM
    """
    bot = SlackBot(
        knowledge_base=knowledge_base,
        response_cache=response_cache,
        llm_service=llm_service
    )
    await bot.start()
