"""Orquestador principal del sistema."""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

from .base import BaseDataSource, DataSourceRegistry, Document, FetchResult
from .scheduler import TaskScheduler

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Orquestador principal que coordina:
    - Scraping de múltiples fuentes
    - Almacenamiento en knowledge base
    - Scheduling de tareas
    - Procesamiento de datos
    """

    def __init__(
        self,
        knowledge_base=None,
        response_cache=None
    ):
        """
        Inicializa el orquestador.

        Args:
            knowledge_base: Instancia de KnowledgeBase para almacenamiento
            response_cache: Instancia de ResponseCache para caching de respuestas
        """
        self.scheduler = TaskScheduler()
        self.knowledge_base = knowledge_base
        self.response_cache = response_cache
        self._active_sources: Dict[str, BaseDataSource] = {}
        self._last_runs: Dict[str, datetime] = {}

    def register_source(
        self,
        source: BaseDataSource,
        schedule_cron: Optional[str] = None,
        schedule_interval_hours: Optional[int] = None
    ) -> bool:
        """
        Registra y activa una fuente de datos.

        Args:
            source: Instancia de la fuente de datos
            schedule_cron: Expresión cron para scraping automático
            schedule_interval_hours: Intervalo en horas para scraping

        Returns:
            True si se registró exitosamente
        """
        try:
            self._active_sources[source.name] = source

            # Configurar scheduling si se especificó
            if schedule_cron:
                self.scheduler.add_cron_job(
                    job_id=f"scrape_{source.name}",
                    func=self._run_source,
                    cron_expression=schedule_cron,
                    kwargs={"source_name": source.name},
                    description=f"Scraping automático de {source.name}"
                )
            elif schedule_interval_hours:
                self.scheduler.add_interval_job(
                    job_id=f"scrape_{source.name}",
                    func=self._run_source,
                    hours=schedule_interval_hours,
                    kwargs={"source_name": source.name},
                    description=f"Scraping cada {schedule_interval_hours}h de {source.name}"
                )

            logger.info(f"Fuente registrada: {source.name}")
            return True

        except Exception as e:
            logger.error(f"Error registrando fuente {source.name}: {e}")
            return False

    async def _run_source(self, source_name: str) -> FetchResult:
        """Ejecuta el scraping de una fuente específica."""
        source = self._active_sources.get(source_name)
        if not source:
            return FetchResult(
                success=False,
                errors=[f"Fuente no encontrada: {source_name}"]
            )

        logger.info(f"Iniciando scraping de: {source_name}")

        try:
            # Fetch datos
            result = await source.fetch()

            if result.success and result.documents:
                # Procesar y almacenar
                await source.process(result.documents)

                # Almacenar en knowledge base si está disponible
                if self.knowledge_base:
                    await self._store_documents(result.documents)

            self._last_runs[source_name] = datetime.now()

            logger.info(
                f"Scraping completado: {source_name} - "
                f"{len(result.documents)} documentos, "
                f"{len(result.errors)} errores"
            )

            return result

        except Exception as e:
            logger.error(f"Error en scraping de {source_name}: {e}")
            return FetchResult(
                success=False,
                errors=[str(e)]
            )

    async def _store_documents(self, documents: List[Document]) -> int:
        """Almacena documentos en la knowledge base."""
        if not self.knowledge_base:
            return 0

        stored = 0
        for doc in documents:
            try:
                await self.knowledge_base.store_document(doc)
                stored += 1
            except Exception as e:
                logger.error(f"Error almacenando documento: {e}")

        return stored

    async def run_source_now(self, source_name: str) -> FetchResult:
        """Ejecuta el scraping de una fuente inmediatamente."""
        return await self._run_source(source_name)

    async def run_all_sources(self) -> Dict[str, FetchResult]:
        """Ejecuta el scraping de todas las fuentes registradas."""
        results = {}
        for source_name in self._active_sources:
            results[source_name] = await self._run_source(source_name)
        return results

    def get_status(self) -> Dict[str, Any]:
        """Obtiene el estado del orquestador."""
        return {
            "active_sources": list(self._active_sources.keys()),
            "scheduled_jobs": self.scheduler.get_jobs(),
            "last_runs": {
                name: dt.isoformat() for name, dt in self._last_runs.items()
            },
            "knowledge_base_connected": self.knowledge_base is not None,
            "response_cache_connected": self.response_cache is not None
        }

    def start(self):
        """Inicia el orquestador y el scheduler."""
        self.scheduler.start()
        logger.info("Orquestador iniciado")

    def stop(self):
        """Detiene el orquestador y el scheduler."""
        self.scheduler.stop()
        logger.info("Orquestador detenido")

    def list_sources(self) -> List[Dict[str, Any]]:
        """Lista todas las fuentes de datos activas."""
        return [
            {
                "name": name,
                "description": source.description,
                "last_run": self._last_runs.get(name, "Never").isoformat()
                    if isinstance(self._last_runs.get(name), datetime) else "Never"
            }
            for name, source in self._active_sources.items()
        ]


# Implementación de SII como fuente de datos
from app.scrapers.sii import SIIScraper, SIIDocument


@DataSourceRegistry.register
class SIIDataSource(BaseDataSource):
    """Fuente de datos del SII (Servicio de Impuestos Internos)."""

    name = "sii"
    description = "Servicio de Impuestos Internos de Chile - Normativas, circulares y valores"

    def __init__(
        self,
        sections: Optional[List[str]] = None,
        max_documents: int = 50
    ):
        self.scraper = SIIScraper()
        self.sections = sections
        self.max_documents = max_documents
        self._knowledge_base = None

    def set_knowledge_base(self, kb):
        """Configura la knowledge base para almacenamiento."""
        self._knowledge_base = kb

    async def fetch(self, **kwargs) -> FetchResult:
        """Obtiene documentos del SII."""
        sections = kwargs.get("sections", self.sections)
        max_docs = kwargs.get("max_documents", self.max_documents)

        results = await self.scraper.scrape_all(sections, max_docs)

        documents = []
        errors = []

        for section, result in results.items():
            for sii_doc in result.documents:
                doc = Document(
                    source="sii",
                    doc_type=sii_doc.doc_type.value,
                    title=sii_doc.title,
                    content=sii_doc.content,
                    url=sii_doc.url,
                    metadata={
                        "doc_number": sii_doc.doc_number,
                        "published_date": sii_doc.published_date.isoformat()
                            if sii_doc.published_date else None,
                        "section": section,
                        **sii_doc.metadata
                    }
                )
                documents.append(doc)
            errors.extend(result.errors)

        return FetchResult(
            success=len(documents) > 0,
            documents=documents,
            errors=errors,
            metadata={"sections_scraped": list(results.keys())}
        )

    async def process(self, documents: List[Document]) -> None:
        """Procesa y almacena documentos en la knowledge base."""
        if self._knowledge_base:
            for doc in documents:
                await self._knowledge_base.store_document(doc)
