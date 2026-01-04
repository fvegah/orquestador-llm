"""Sistema de caché de respuestas IA."""
import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as redis

logger = logging.getLogger(__name__)


class ResponseCache:
    """
    Cache de respuestas para reducir llamadas a LLM.

    Estrategias:
    - Cache en Redis para respuestas rápidas
    - Persistencia en PostgreSQL para respuestas frecuentes
    - TTL configurable por tipo de pregunta
    - Invalidación automática de caché antigua
    """

    # TTL por defecto en segundos (24 horas)
    DEFAULT_TTL = 86400

    # TTL por categoría de pregunta
    CATEGORY_TTL = {
        "valores_utm_uf": 3600,      # 1 hora (valores cambian mensualmente)
        "normativa": 604800,          # 7 días (normativas cambian poco)
        "consulta_general": 86400,    # 24 horas
        "calculo_iva": 43200,         # 12 horas
    }

    def __init__(
        self,
        redis_url: Optional[str] = None,
        default_ttl: Optional[int] = None,
        db_pool=None  # Pool de asyncpg para persistencia
    ):
        """
        Inicializa el cache de respuestas.

        Args:
            redis_url: URL de conexión a Redis
            default_ttl: TTL por defecto en segundos
            db_pool: Pool de conexiones PostgreSQL para persistencia
        """
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.default_ttl = default_ttl or self.DEFAULT_TTL
        self.db_pool = db_pool
        self._redis: Optional[redis.Redis] = None

    async def connect(self) -> None:
        """Conecta a Redis."""
        try:
            self._redis = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            await self._redis.ping()
            logger.info("Conectado a Redis para cache de respuestas")
        except Exception as e:
            logger.error(f"Error conectando a Redis: {e}")
            raise

    async def disconnect(self) -> None:
        """Desconecta de Redis."""
        if self._redis:
            await self._redis.close()
            logger.info("Desconectado de Redis")

    def _generate_key(
        self,
        question: str,
        context_hash: Optional[str] = None,
        rut: Optional[str] = None
    ) -> str:
        """
        Genera una clave única para la pregunta.

        La clave incluye:
        - Hash de la pregunta normalizada
        - Hash del contexto (documentos usados)
        - RUT si la pregunta es específica de un contribuyente
        """
        # Normalizar pregunta
        normalized = question.lower().strip()
        normalized = " ".join(normalized.split())  # Remover espacios extra

        question_hash = hashlib.sha256(normalized.encode()).hexdigest()[:16]

        key_parts = ["response_cache", question_hash]

        if context_hash:
            key_parts.append(context_hash[:8])

        if rut:
            rut_hash = hashlib.md5(rut.encode()).hexdigest()[:8]
            key_parts.append(rut_hash)

        return ":".join(key_parts)

    def _detect_category(self, question: str) -> str:
        """Detecta la categoría de una pregunta para determinar TTL."""
        question_lower = question.lower()

        if any(word in question_lower for word in ["utm", "uf", "valor", "dolar"]):
            return "valores_utm_uf"
        elif any(word in question_lower for word in ["circular", "resolucion", "normativa", "ley"]):
            return "normativa"
        elif any(word in question_lower for word in ["iva", "calculo", "calcular", "debito", "credito"]):
            return "calculo_iva"
        else:
            return "consulta_general"

    async def get(
        self,
        question: str,
        context_hash: Optional[str] = None,
        rut: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Obtiene respuesta cacheada si existe.

        Args:
            question: Pregunta del usuario
            context_hash: Hash del contexto usado
            rut: RUT del contribuyente

        Returns:
            Diccionario con respuesta y metadata, o None si no hay cache
        """
        if not self._redis:
            return None

        key = self._generate_key(question, context_hash, rut)

        try:
            cached = await self._redis.get(key)

            if cached:
                data = json.loads(cached)

                # Incrementar contador de hits
                await self._redis.hincrby(f"cache_stats:{key}", "hits", 1)

                # Actualizar hit count en PostgreSQL si está disponible
                if self.db_pool:
                    await self._update_hit_count(key)

                logger.debug(f"Cache HIT: {key[:30]}...")
                return data

            logger.debug(f"Cache MISS: {key[:30]}...")
            return None

        except Exception as e:
            logger.error(f"Error leyendo cache: {e}")
            return None

    async def set(
        self,
        question: str,
        response: str,
        context_hash: Optional[str] = None,
        rut: Optional[str] = None,
        source_documents: Optional[List[Dict]] = None,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Guarda respuesta en cache.

        Args:
            question: Pregunta original
            response: Respuesta del LLM
            context_hash: Hash del contexto
            rut: RUT del contribuyente
            source_documents: Documentos fuente usados
            ttl: Tiempo de vida en segundos

        Returns:
            True si se guardó exitosamente
        """
        if not self._redis:
            return False

        key = self._generate_key(question, context_hash, rut)

        # Determinar TTL basado en categoría si no se especificó
        if ttl is None:
            category = self._detect_category(question)
            ttl = self.CATEGORY_TTL.get(category, self.default_ttl)

        data = {
            "question": question,
            "response": response,
            "context_hash": context_hash,
            "source_documents": source_documents or [],
            "cached_at": datetime.now().isoformat(),
            "expires_at": (datetime.now() + timedelta(seconds=ttl)).isoformat(),
            "category": self._detect_category(question)
        }

        try:
            await self._redis.setex(
                key,
                ttl,
                json.dumps(data, ensure_ascii=False)
            )

            # Guardar en PostgreSQL para persistencia a largo plazo
            if self.db_pool:
                await self._persist_to_db(key, question, response, context_hash, source_documents, ttl)

            logger.debug(f"Cache SET: {key[:30]}... (TTL: {ttl}s)")
            return True

        except Exception as e:
            logger.error(f"Error guardando en cache: {e}")
            return False

    async def _persist_to_db(
        self,
        key: str,
        question: str,
        response: str,
        context_hash: Optional[str],
        source_documents: Optional[List[Dict]],
        ttl: int
    ) -> None:
        """Persiste respuesta en PostgreSQL."""
        if not self.db_pool:
            return

        question_hash = key.split(":")[1] if ":" in key else key

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO response_cache
                    (question_hash, context_hash, question, response, source_documents, expires_at)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (question_hash, context_hash)
                    DO UPDATE SET
                        response = EXCLUDED.response,
                        source_documents = EXCLUDED.source_documents,
                        expires_at = EXCLUDED.expires_at,
                        hit_count = response_cache.hit_count + 1
                """,
                    question_hash,
                    context_hash,
                    question,
                    response,
                    json.dumps(source_documents or []),
                    datetime.now() + timedelta(seconds=ttl)
                )
        except Exception as e:
            logger.error(f"Error persistiendo cache en DB: {e}")

    async def _update_hit_count(self, key: str) -> None:
        """Actualiza contador de hits en PostgreSQL."""
        if not self.db_pool:
            return

        question_hash = key.split(":")[1] if ":" in key else key

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE response_cache
                    SET hit_count = hit_count + 1
                    WHERE question_hash = $1
                """, question_hash)
        except Exception as e:
            logger.error(f"Error actualizando hit count: {e}")

    async def invalidate(
        self,
        question: Optional[str] = None,
        pattern: Optional[str] = None
    ) -> int:
        """
        Invalida entradas del cache.

        Args:
            question: Pregunta específica a invalidar
            pattern: Patrón de claves a invalidar (ej: "response_cache:*")

        Returns:
            Número de entradas invalidadas
        """
        if not self._redis:
            return 0

        count = 0

        try:
            if question:
                key = self._generate_key(question)
                deleted = await self._redis.delete(key)
                count = deleted
            elif pattern:
                async for key in self._redis.scan_iter(pattern):
                    await self._redis.delete(key)
                    count += 1

            logger.info(f"Cache invalidado: {count} entradas")
            return count

        except Exception as e:
            logger.error(f"Error invalidando cache: {e}")
            return 0

    async def get_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del cache."""
        if not self._redis:
            return {"error": "Redis no conectado"}

        try:
            info = await self._redis.info("stats")

            # Contar claves de cache
            cache_keys = 0
            async for _ in self._redis.scan_iter("response_cache:*"):
                cache_keys += 1

            return {
                "total_cached_responses": cache_keys,
                "redis_hits": info.get("keyspace_hits", 0),
                "redis_misses": info.get("keyspace_misses", 0),
                "hit_rate": round(
                    info.get("keyspace_hits", 0) /
                    max(info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0), 1) * 100,
                    2
                )
            }

        except Exception as e:
            logger.error(f"Error obteniendo stats de cache: {e}")
            return {"error": str(e)}

    async def cleanup_expired(self) -> int:
        """
        Limpia entradas expiradas de PostgreSQL.

        Returns:
            Número de entradas eliminadas
        """
        if not self.db_pool:
            return 0

        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.execute("""
                    DELETE FROM response_cache
                    WHERE expires_at < NOW()
                """)
                # Extraer número de filas eliminadas
                count = int(result.split()[-1]) if result else 0
                logger.info(f"Limpieza de cache: {count} entradas expiradas eliminadas")
                return count
        except Exception as e:
            logger.error(f"Error en limpieza de cache: {e}")
            return 0
