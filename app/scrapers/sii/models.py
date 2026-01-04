"""Modelos de datos para documentos del SII."""
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Optional, List


class DocumentType(Enum):
    """Tipos de documentos del SII."""
    CIRCULAR = "circular"
    RESOLUCION = "resolucion"
    NORMATIVA = "normativa"
    FORMULARIO = "formulario"
    INSTRUCTIVO = "instructivo"
    LEY = "ley"
    DECRETO = "decreto"
    TABLA_VALORES = "tabla_valores"


@dataclass
class SIIDocument:
    """Representa un documento extraído del SII."""
    doc_type: DocumentType
    title: str
    content: str
    url: str
    doc_number: Optional[str] = None
    published_date: Optional[date] = None
    scraped_at: datetime = field(default_factory=datetime.now)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convierte el documento a diccionario."""
        return {
            "doc_type": self.doc_type.value,
            "doc_number": self.doc_number,
            "title": self.title,
            "content": self.content,
            "url": self.url,
            "published_date": self.published_date.isoformat() if self.published_date else None,
            "scraped_at": self.scraped_at.isoformat(),
            "metadata": self.metadata
        }


@dataclass
class ScrapingResult:
    """Resultado de una operación de scraping."""
    success: bool
    documents_count: int
    documents: List[SIIDocument] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0
