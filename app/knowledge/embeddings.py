"""Servicio de embeddings para búsqueda semántica."""
import hashlib
import logging
import os
from typing import List, Optional

import httpx

logger = logging.getLogger(__name__)


class EmbeddingService:
    """
    Servicio para generar embeddings de texto.

    Soporta múltiples backends:
    - OpenAI (text-embedding-ada-002, text-embedding-3-small)
    - Ollama (nomic-embed-text, mxbai-embed-large)
    """

    EMBEDDING_DIMENSIONS = {
        "text-embedding-ada-002": 1536,
        "text-embedding-3-small": 1536,
        "text-embedding-3-large": 3072,
        "nomic-embed-text": 768,
        "mxbai-embed-large": 1024,
    }

    def __init__(
        self,
        provider: str = "openai",
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        ollama_host: Optional[str] = None
    ):
        """
        Inicializa el servicio de embeddings.

        Args:
            provider: 'openai' u 'ollama'
            model: Modelo de embeddings a usar
            api_key: API key de OpenAI (solo para provider='openai')
            ollama_host: URL de Ollama (solo para provider='ollama')
        """
        self.provider = provider.lower()

        if self.provider == "openai":
            self.model = model or os.getenv("EMBEDDING_MODEL", "text-embedding-ada-002")
            self.api_key = api_key or os.getenv("OPENAI_API_KEY")
            if not self.api_key:
                logger.warning("OpenAI API key no configurada para embeddings")
        else:  # ollama
            self.model = model or os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text")
            self.ollama_host = ollama_host or os.getenv("OLLAMA_HOST", "http://localhost:11434")

        self.dimension = self.EMBEDDING_DIMENSIONS.get(self.model, 1536)

    async def generate_embedding(self, text: str) -> Optional[List[float]]:
        """
        Genera embedding para un texto.

        Args:
            text: Texto a convertir en embedding

        Returns:
            Lista de floats representando el embedding, o None si hay error
        """
        if not text or not text.strip():
            return None

        # Truncar texto si es muy largo
        text = text[:8000]  # Límite seguro para la mayoría de modelos

        try:
            if self.provider == "openai":
                return await self._openai_embedding(text)
            else:
                return await self._ollama_embedding(text)
        except Exception as e:
            logger.error(f"Error generando embedding: {e}")
            return None

    async def _openai_embedding(self, text: str) -> Optional[List[float]]:
        """Genera embedding usando OpenAI."""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.openai.com/v1/embeddings",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.model,
                    "input": text
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            return data["data"][0]["embedding"]

    async def _ollama_embedding(self, text: str) -> Optional[List[float]]:
        """Genera embedding usando Ollama."""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.ollama_host}/api/embeddings",
                json={
                    "model": self.model,
                    "prompt": text
                },
                timeout=60
            )
            response.raise_for_status()
            data = response.json()
            return data.get("embedding")

    async def generate_embeddings_batch(
        self,
        texts: List[str],
        batch_size: int = 100
    ) -> List[Optional[List[float]]]:
        """
        Genera embeddings para múltiples textos.

        Args:
            texts: Lista de textos
            batch_size: Tamaño de batch para procesamiento

        Returns:
            Lista de embeddings (algunos pueden ser None si hay error)
        """
        embeddings = []

        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]

            if self.provider == "openai":
                # OpenAI soporta batch nativo
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.post(
                            "https://api.openai.com/v1/embeddings",
                            headers={
                                "Authorization": f"Bearer {self.api_key}",
                                "Content-Type": "application/json"
                            },
                            json={
                                "model": self.model,
                                "input": batch
                            },
                            timeout=60
                        )
                        response.raise_for_status()
                        data = response.json()

                        batch_embeddings = [None] * len(batch)
                        for item in data["data"]:
                            batch_embeddings[item["index"]] = item["embedding"]
                        embeddings.extend(batch_embeddings)

                except Exception as e:
                    logger.error(f"Error en batch de embeddings: {e}")
                    embeddings.extend([None] * len(batch))
            else:
                # Ollama no soporta batch, procesar uno a uno
                for text in batch:
                    embedding = await self.generate_embedding(text)
                    embeddings.append(embedding)

        return embeddings

    def text_hash(self, text: str) -> str:
        """Genera un hash único para un texto."""
        return hashlib.sha256(text.encode()).hexdigest()[:16]
