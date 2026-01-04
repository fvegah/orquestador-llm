"""Clases base para fuentes de datos extensibles."""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Type


@dataclass
class Document:
    """Documento genérico extraído de cualquier fuente."""
    source: str
    doc_type: str
    title: str
    content: str
    url: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "doc_type": self.doc_type,
            "title": self.title,
            "content": self.content,
            "url": self.url,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat()
        }


@dataclass
class FetchResult:
    """Resultado de una operación de fetch."""
    success: bool
    documents: List[Document] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseDataSource(ABC):
    """
    Clase base abstracta para fuentes de datos.

    Para agregar una nueva fuente de datos:
    1. Crear una clase que herede de BaseDataSource
    2. Implementar los métodos abstractos
    3. Registrar la fuente con DataSourceRegistry
    """

    name: str = "base"
    description: str = "Base data source"

    @abstractmethod
    async def fetch(self, **kwargs) -> FetchResult:
        """
        Obtiene datos de la fuente.

        Returns:
            FetchResult con los documentos obtenidos
        """
        pass

    @abstractmethod
    async def process(self, documents: List[Document]) -> None:
        """
        Procesa y almacena los documentos obtenidos.

        Args:
            documents: Lista de documentos a procesar
        """
        pass

    async def validate(self) -> bool:
        """
        Valida que la fuente de datos esté disponible.

        Returns:
            True si la fuente está disponible
        """
        return True

    def get_config(self) -> Dict[str, Any]:
        """Retorna la configuración de la fuente."""
        return {
            "name": self.name,
            "description": self.description
        }


class DataSourceRegistry:
    """Registro de fuentes de datos disponibles."""

    _sources: Dict[str, Type[BaseDataSource]] = {}

    @classmethod
    def register(cls, source_class: Type[BaseDataSource]) -> Type[BaseDataSource]:
        """
        Registra una nueva fuente de datos.

        Puede usarse como decorador:
        @DataSourceRegistry.register
        class MiFuente(BaseDataSource):
            ...
        """
        cls._sources[source_class.name] = source_class
        return source_class

    @classmethod
    def get(cls, name: str) -> Optional[Type[BaseDataSource]]:
        """Obtiene una fuente de datos por nombre."""
        return cls._sources.get(name)

    @classmethod
    def list_sources(cls) -> List[Dict[str, str]]:
        """Lista todas las fuentes de datos registradas."""
        return [
            {"name": source.name, "description": source.description}
            for source in cls._sources.values()
        ]

    @classmethod
    def create_instance(cls, name: str, **kwargs) -> Optional[BaseDataSource]:
        """Crea una instancia de una fuente de datos."""
        source_class = cls.get(name)
        if source_class:
            return source_class(**kwargs)
        return None
