"""Knowledge Base con PostgreSQL y búsqueda vectorial."""
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from app.orchestrator.base import Document
from .embeddings import EmbeddingService

logger = logging.getLogger(__name__)


class KnowledgeBase:
    """
    Base de conocimientos con PostgreSQL + pgvector.

    Funcionalidades:
    - Almacenamiento de documentos con embeddings
    - Búsqueda semántica por similitud
    - Búsqueda por filtros (tipo, fuente, fecha)
    - Historial de conversaciones
    """

    def __init__(
        self,
        database_url: Optional[str] = None,
        embedding_service: Optional[EmbeddingService] = None
    ):
        """
        Inicializa la knowledge base.

        Args:
            database_url: URL de conexión a PostgreSQL
            embedding_service: Servicio de embeddings a usar
        """
        self.database_url = database_url or os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:postgres@localhost:5432/contabilidad_db"
        )
        self.embedding_service = embedding_service or EmbeddingService()
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        """Establece conexión con la base de datos."""
        try:
            self._pool = await asyncpg.create_pool(
                self.database_url,
                min_size=2,
                max_size=10
            )
            logger.info("Conectado a PostgreSQL")
        except Exception as e:
            logger.error(f"Error conectando a PostgreSQL: {e}")
            raise

    async def disconnect(self) -> None:
        """Cierra la conexión con la base de datos."""
        if self._pool:
            await self._pool.close()
            logger.info("Desconectado de PostgreSQL")

    async def initialize_schema(self) -> None:
        """Crea las tablas necesarias si no existen."""
        schema_sql = """
        -- Habilitar extensión pgvector
        CREATE EXTENSION IF NOT EXISTS vector;

        -- Tabla de documentos
        CREATE TABLE IF NOT EXISTS sii_documents (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            doc_type VARCHAR(50) NOT NULL,
            doc_number VARCHAR(100),
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            url VARCHAR(500),
            published_date DATE,
            scraped_at TIMESTAMP DEFAULT NOW(),
            metadata JSONB DEFAULT '{}',
            content_hash VARCHAR(64) UNIQUE,
            embedding vector(1536)
        );

        -- Índices para búsqueda
        CREATE INDEX IF NOT EXISTS idx_documents_source ON sii_documents(source);
        CREATE INDEX IF NOT EXISTS idx_documents_doc_type ON sii_documents(doc_type);
        CREATE INDEX IF NOT EXISTS idx_documents_published_date ON sii_documents(published_date);

        -- Índice vectorial para búsqueda semántica (IVFFlat para mejor rendimiento)
        CREATE INDEX IF NOT EXISTS idx_documents_embedding
        ON sii_documents USING ivfflat (embedding vector_cosine_ops)
        WITH (lists = 100);

        -- Tabla de caché de respuestas
        CREATE TABLE IF NOT EXISTS response_cache (
            id SERIAL PRIMARY KEY,
            question_hash VARCHAR(64) NOT NULL,
            context_hash VARCHAR(64),
            question TEXT NOT NULL,
            response TEXT NOT NULL,
            source_documents JSONB DEFAULT '[]',
            created_at TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP,
            hit_count INTEGER DEFAULT 0,
            UNIQUE(question_hash, context_hash)
        );

        CREATE INDEX IF NOT EXISTS idx_response_cache_hash
        ON response_cache(question_hash, context_hash);

        CREATE INDEX IF NOT EXISTS idx_response_cache_expires
        ON response_cache(expires_at);

        -- Tabla de historial de conversaciones
        CREATE TABLE IF NOT EXISTS conversations (
            id SERIAL PRIMARY KEY,
            slack_user_id VARCHAR(50),
            slack_channel_id VARCHAR(50),
            slack_thread_ts VARCHAR(50),
            question TEXT NOT NULL,
            response TEXT NOT NULL,
            source_documents JSONB DEFAULT '[]',
            response_time_ms INTEGER,
            from_cache BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_conversations_user
        ON conversations(slack_user_id);

        CREATE INDEX IF NOT EXISTS idx_conversations_channel
        ON conversations(slack_channel_id);

        CREATE INDEX IF NOT EXISTS idx_conversations_created
        ON conversations(created_at);
        """

        async with self._pool.acquire() as conn:
            await conn.execute(schema_sql)
            logger.info("Schema de base de datos inicializado")

    async def store_document(self, document: Document) -> Optional[int]:
        """
        Almacena un documento en la base de datos.

        Args:
            document: Documento a almacenar

        Returns:
            ID del documento insertado, o None si ya existe
        """
        # Generar hash del contenido para evitar duplicados
        content_hash = self.embedding_service.text_hash(document.content)

        # Generar embedding
        embedding = await self.embedding_service.generate_embedding(
            f"{document.title}\n\n{document.content[:4000]}"
        )

        try:
            async with self._pool.acquire() as conn:
                # Intentar insertar, ignorar si ya existe
                result = await conn.fetchval("""
                    INSERT INTO sii_documents
                    (source, doc_type, doc_number, title, content, url,
                     published_date, metadata, content_hash, embedding)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (content_hash) DO NOTHING
                    RETURNING id
                """,
                    document.source,
                    document.doc_type,
                    document.metadata.get("doc_number"),
                    document.title,
                    document.content,
                    document.url,
                    document.metadata.get("published_date"),
                    json.dumps(document.metadata),
                    content_hash,
                    embedding
                )

                if result:
                    logger.debug(f"Documento almacenado: {document.title[:50]}...")
                return result

        except Exception as e:
            logger.error(f"Error almacenando documento: {e}")
            return None

    async def search_semantic(
        self,
        query: str,
        limit: int = 5,
        source: Optional[str] = None,
        doc_type: Optional[str] = None,
        threshold: float = 0.7
    ) -> List[Dict[str, Any]]:
        """
        Búsqueda semántica de documentos.

        Args:
            query: Consulta de búsqueda
            limit: Máximo de resultados
            source: Filtrar por fuente (ej: 'sii')
            doc_type: Filtrar por tipo de documento
            threshold: Umbral mínimo de similitud (0-1)

        Returns:
            Lista de documentos ordenados por relevancia
        """
        # Generar embedding de la consulta
        query_embedding = await self.embedding_service.generate_embedding(query)
        if not query_embedding:
            return []

        # Construir query con filtros opcionales
        filters = []
        params = [query_embedding, limit]
        param_idx = 3

        if source:
            filters.append(f"source = ${param_idx}")
            params.append(source)
            param_idx += 1

        if doc_type:
            filters.append(f"doc_type = ${param_idx}")
            params.append(doc_type)
            param_idx += 1

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        sql = f"""
            SELECT
                id, source, doc_type, doc_number, title,
                content, url, published_date, metadata,
                1 - (embedding <=> $1) as similarity
            FROM sii_documents
            {where_clause}
            ORDER BY embedding <=> $1
            LIMIT $2
        """

        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)

                results = []
                for row in rows:
                    similarity = float(row['similarity'])
                    if similarity >= threshold:
                        results.append({
                            "id": row['id'],
                            "source": row['source'],
                            "doc_type": row['doc_type'],
                            "doc_number": row['doc_number'],
                            "title": row['title'],
                            "content": row['content'][:1000] + "..." if len(row['content']) > 1000 else row['content'],
                            "url": row['url'],
                            "published_date": row['published_date'].isoformat() if row['published_date'] else None,
                            "similarity": round(similarity, 4)
                        })

                return results

        except Exception as e:
            logger.error(f"Error en búsqueda semántica: {e}")
            return []

    async def search_keyword(
        self,
        keyword: str,
        limit: int = 10,
        source: Optional[str] = None,
        doc_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Búsqueda por palabra clave en título y contenido.

        Args:
            keyword: Palabra clave a buscar
            limit: Máximo de resultados
            source: Filtrar por fuente
            doc_type: Filtrar por tipo

        Returns:
            Lista de documentos que coinciden
        """
        filters = ["(title ILIKE $1 OR content ILIKE $1)"]
        params = [f"%{keyword}%", limit]
        param_idx = 3

        if source:
            filters.append(f"source = ${param_idx}")
            params.append(source)
            param_idx += 1

        if doc_type:
            filters.append(f"doc_type = ${param_idx}")
            params.append(doc_type)
            param_idx += 1

        where_clause = "WHERE " + " AND ".join(filters)

        sql = f"""
            SELECT id, source, doc_type, doc_number, title,
                   LEFT(content, 500) as content_preview, url, published_date
            FROM sii_documents
            {where_clause}
            ORDER BY scraped_at DESC
            LIMIT $2
        """

        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)

                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"Error en búsqueda por keyword: {e}")
            return []

    async def get_document_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de los documentos almacenados."""
        sql = """
            SELECT
                source,
                doc_type,
                COUNT(*) as count,
                MIN(scraped_at) as oldest,
                MAX(scraped_at) as newest
            FROM sii_documents
            GROUP BY source, doc_type
            ORDER BY source, doc_type
        """

        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(sql)

                total = await conn.fetchval("SELECT COUNT(*) FROM sii_documents")

                stats = {
                    "total_documents": total,
                    "by_source_and_type": [
                        {
                            "source": row['source'],
                            "doc_type": row['doc_type'],
                            "count": row['count'],
                            "oldest": row['oldest'].isoformat() if row['oldest'] else None,
                            "newest": row['newest'].isoformat() if row['newest'] else None
                        }
                        for row in rows
                    ]
                }

                return stats

        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {"error": str(e)}

    async def save_conversation(
        self,
        question: str,
        response: str,
        slack_user_id: Optional[str] = None,
        slack_channel_id: Optional[str] = None,
        slack_thread_ts: Optional[str] = None,
        source_documents: Optional[List[Dict]] = None,
        response_time_ms: Optional[int] = None,
        from_cache: bool = False
    ) -> int:
        """Guarda una conversación en el historial."""
        try:
            async with self._pool.acquire() as conn:
                return await conn.fetchval("""
                    INSERT INTO conversations
                    (slack_user_id, slack_channel_id, slack_thread_ts,
                     question, response, source_documents, response_time_ms, from_cache)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    RETURNING id
                """,
                    slack_user_id,
                    slack_channel_id,
                    slack_thread_ts,
                    question,
                    response,
                    json.dumps(source_documents or []),
                    response_time_ms,
                    from_cache
                )
        except Exception as e:
            logger.error(f"Error guardando conversación: {e}")
            return -1
