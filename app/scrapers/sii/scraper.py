"""Scraper principal para el SII (Servicio de Impuestos Internos de Chile)."""
import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin

import httpx

from .models import SIIDocument, DocumentType, ScrapingResult
from .parser import SIIParser

logger = logging.getLogger(__name__)


class SIIScraper:
    """
    Scraper para extraer información del sitio web del SII.

    Extrae:
    - Circulares
    - Resoluciones
    - Normativas tributarias
    - Valores UTM/UF
    - Formularios e instructivos
    """

    BASE_URL = "https://www.sii.cl"

    # URLs de secciones importantes del SII
    SECTIONS = {
        "normativa": "/normativa_legislacion/index.html",
        "circulares": "/normativa_legislacion/circulares.html",
        "resoluciones": "/normativa_legislacion/resoluciones.html",
        "valores_utm": "/valores_y_fechas/utm/utm2024.htm",
        "valores_uf": "/valores_y_fechas/uf/uf2024.htm",
        "formularios": "/formularios/index.html",
        "iva": "/portales/iva/index.html",
        "renta": "/portales/renta/index.html",
    }

    def __init__(
        self,
        timeout: int = 30,
        max_concurrent: int = 5,
        delay_between_requests: float = 1.0
    ):
        """
        Inicializa el scraper.

        Args:
            timeout: Timeout en segundos para requests HTTP
            max_concurrent: Máximo de requests concurrentes
            delay_between_requests: Delay entre requests para no sobrecargar el servidor
        """
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self.delay = delay_between_requests
        self.parser = SIIParser()
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def _fetch_page(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        """Obtiene el contenido HTML de una página."""
        async with self._semaphore:
            try:
                response = await client.get(url, timeout=self.timeout)
                response.raise_for_status()
                await asyncio.sleep(self.delay)  # Rate limiting
                return response.text
            except httpx.HTTPError as e:
                logger.error(f"Error fetching {url}: {e}")
                return None

    async def scrape_section(
        self,
        section: str,
        max_documents: int = 100
    ) -> ScrapingResult:
        """
        Scrapea una sección específica del SII.

        Args:
            section: Nombre de la sección (normativa, circulares, etc.)
            max_documents: Máximo de documentos a extraer

        Returns:
            ScrapingResult con los documentos extraídos
        """
        start_time = datetime.now()
        documents: List[SIIDocument] = []
        errors: List[str] = []

        section_path = self.SECTIONS.get(section)
        if not section_path:
            return ScrapingResult(
                success=False,
                documents_count=0,
                errors=[f"Sección desconocida: {section}"]
            )

        url = urljoin(self.BASE_URL, section_path)

        async with httpx.AsyncClient(
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; ContabilidadBot/1.0)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "es-CL,es;q=0.9"
            },
            follow_redirects=True
        ) as client:
            # Obtener página principal de la sección
            html = await self._fetch_page(client, url)
            if not html:
                return ScrapingResult(
                    success=False,
                    documents_count=0,
                    errors=[f"No se pudo obtener la página: {url}"]
                )

            # Determinar tipo de contenido y parsear
            if section in ["valores_utm", "valores_uf"]:
                docs = self.parser.parse_valores_utm_uf(html, url)
                documents.extend(docs)
            else:
                # Extraer lista de documentos
                doc_list = self.parser.parse_normativa_list(html, self.BASE_URL)

                # Limitar cantidad
                doc_list = doc_list[:max_documents]

                # Obtener cada documento
                for title, doc_url, doc_type_str in doc_list:
                    try:
                        doc_html = await self._fetch_page(client, doc_url)
                        if doc_html:
                            doc_type = DocumentType(doc_type_str)
                            doc = self.parser.parse_document_page(doc_html, doc_url, doc_type)
                            if doc:
                                documents.append(doc)
                    except Exception as e:
                        errors.append(f"Error procesando {doc_url}: {str(e)}")

        duration = (datetime.now() - start_time).total_seconds()

        return ScrapingResult(
            success=len(documents) > 0,
            documents_count=len(documents),
            documents=documents,
            errors=errors,
            duration_seconds=duration
        )

    async def scrape_all(
        self,
        sections: Optional[List[str]] = None,
        max_documents_per_section: int = 50
    ) -> Dict[str, ScrapingResult]:
        """
        Scrapea múltiples secciones del SII.

        Args:
            sections: Lista de secciones a scrapear (None = todas)
            max_documents_per_section: Máximo de documentos por sección

        Returns:
            Dict con resultados por sección
        """
        if sections is None:
            sections = list(self.SECTIONS.keys())

        results = {}
        for section in sections:
            logger.info(f"Scraping sección: {section}")
            result = await self.scrape_section(section, max_documents_per_section)
            results[section] = result
            logger.info(
                f"Sección {section}: {result.documents_count} documentos, "
                f"{len(result.errors)} errores, {result.duration_seconds:.2f}s"
            )

        return results

    async def scrape_utm_uf_values(self, year: int = 2024) -> ScrapingResult:
        """
        Scrapea valores de UTM y UF para un año específico.

        Args:
            year: Año para obtener valores

        Returns:
            ScrapingResult con tablas de valores
        """
        documents = []
        errors = []
        start_time = datetime.now()

        urls = [
            f"{self.BASE_URL}/valores_y_fechas/utm/utm{year}.htm",
            f"{self.BASE_URL}/valores_y_fechas/uf/uf{year}.htm",
        ]

        async with httpx.AsyncClient(
            headers={"User-Agent": "Mozilla/5.0 (compatible; ContabilidadBot/1.0)"},
            follow_redirects=True
        ) as client:
            for url in urls:
                html = await self._fetch_page(client, url)
                if html:
                    docs = self.parser.parse_valores_utm_uf(html, url)
                    documents.extend(docs)
                else:
                    errors.append(f"No se pudo obtener: {url}")

        duration = (datetime.now() - start_time).total_seconds()

        return ScrapingResult(
            success=len(documents) > 0,
            documents_count=len(documents),
            documents=documents,
            errors=errors,
            duration_seconds=duration
        )


# Función helper para ejecutar scraping desde código síncrono
def run_scraping(
    sections: Optional[List[str]] = None,
    max_documents: int = 50
) -> Dict[str, ScrapingResult]:
    """Ejecuta scraping de forma síncrona."""
    scraper = SIIScraper()
    return asyncio.run(scraper.scrape_all(sections, max_documents))
