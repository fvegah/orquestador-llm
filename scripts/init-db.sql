-- Script de inicialización de la base de datos
-- Este script se ejecuta automáticamente al crear el contenedor de PostgreSQL

-- Habilitar extensión pgvector para búsqueda semántica
CREATE EXTENSION IF NOT EXISTS vector;

-- ============================================
-- TABLA DE DOCUMENTOS DEL SII
-- ============================================
CREATE TABLE IF NOT EXISTS sii_documents (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL DEFAULT 'sii',
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

-- Índices para búsqueda eficiente
CREATE INDEX IF NOT EXISTS idx_documents_source ON sii_documents(source);
CREATE INDEX IF NOT EXISTS idx_documents_doc_type ON sii_documents(doc_type);
CREATE INDEX IF NOT EXISTS idx_documents_published_date ON sii_documents(published_date);
CREATE INDEX IF NOT EXISTS idx_documents_scraped_at ON sii_documents(scraped_at);

-- Índice de texto completo para búsqueda por keywords
CREATE INDEX IF NOT EXISTS idx_documents_title_gin ON sii_documents USING gin(to_tsvector('spanish', title));
CREATE INDEX IF NOT EXISTS idx_documents_content_gin ON sii_documents USING gin(to_tsvector('spanish', content));

-- Índice vectorial para búsqueda semántica (IVFFlat)
-- Nota: Requiere al menos 100 registros para crear el índice
-- CREATE INDEX IF NOT EXISTS idx_documents_embedding ON sii_documents USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- ============================================
-- TABLA DE CACHÉ DE RESPUESTAS
-- ============================================
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
    category VARCHAR(50),
    UNIQUE(question_hash, context_hash)
);

CREATE INDEX IF NOT EXISTS idx_response_cache_hash ON response_cache(question_hash, context_hash);
CREATE INDEX IF NOT EXISTS idx_response_cache_expires ON response_cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_response_cache_category ON response_cache(category);

-- ============================================
-- TABLA DE HISTORIAL DE CONVERSACIONES
-- ============================================
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

CREATE INDEX IF NOT EXISTS idx_conversations_user ON conversations(slack_user_id);
CREATE INDEX IF NOT EXISTS idx_conversations_channel ON conversations(slack_channel_id);
CREATE INDEX IF NOT EXISTS idx_conversations_created ON conversations(created_at);

-- ============================================
-- TABLA DE JOBS DE SCRAPING
-- ============================================
CREATE TABLE IF NOT EXISTS scraping_jobs (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',  -- pending, running, completed, failed
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    documents_scraped INTEGER DEFAULT 0,
    documents_stored INTEGER DEFAULT 0,
    errors JSONB DEFAULT '[]',
    metadata JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_scraping_jobs_source ON scraping_jobs(source);
CREATE INDEX IF NOT EXISTS idx_scraping_jobs_status ON scraping_jobs(status);
CREATE INDEX IF NOT EXISTS idx_scraping_jobs_started ON scraping_jobs(started_at);

-- ============================================
-- VISTAS ÚTILES
-- ============================================

-- Vista de estadísticas de documentos
CREATE OR REPLACE VIEW document_stats AS
SELECT
    source,
    doc_type,
    COUNT(*) as total,
    MIN(scraped_at) as first_scraped,
    MAX(scraped_at) as last_scraped,
    COUNT(CASE WHEN embedding IS NOT NULL THEN 1 END) as with_embeddings
FROM sii_documents
GROUP BY source, doc_type
ORDER BY source, doc_type;

-- Vista de estadísticas de cache
CREATE OR REPLACE VIEW cache_stats AS
SELECT
    category,
    COUNT(*) as total_entries,
    SUM(hit_count) as total_hits,
    AVG(hit_count) as avg_hits,
    COUNT(CASE WHEN expires_at > NOW() THEN 1 END) as active_entries
FROM response_cache
GROUP BY category;

-- Vista de actividad reciente
CREATE OR REPLACE VIEW recent_activity AS
SELECT
    'conversation' as type,
    id,
    question as content,
    created_at,
    slack_user_id as user_id
FROM conversations
WHERE created_at > NOW() - INTERVAL '24 hours'
ORDER BY created_at DESC
LIMIT 100;

-- ============================================
-- FUNCIONES ÚTILES
-- ============================================

-- Función para limpiar cache expirado
CREATE OR REPLACE FUNCTION cleanup_expired_cache()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM response_cache WHERE expires_at < NOW();
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Función para obtener documentos similares
CREATE OR REPLACE FUNCTION search_similar_documents(
    query_embedding vector(1536),
    limit_count INTEGER DEFAULT 5,
    threshold FLOAT DEFAULT 0.7
)
RETURNS TABLE (
    id INTEGER,
    title TEXT,
    content TEXT,
    url VARCHAR(500),
    doc_type VARCHAR(50),
    similarity FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.id,
        d.title,
        LEFT(d.content, 500) as content,
        d.url,
        d.doc_type,
        (1 - (d.embedding <=> query_embedding))::FLOAT as similarity
    FROM sii_documents d
    WHERE d.embedding IS NOT NULL
    AND (1 - (d.embedding <=> query_embedding)) >= threshold
    ORDER BY d.embedding <=> query_embedding
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- DATOS INICIALES (Opcional)
-- ============================================

-- Insertar documento de ejemplo
INSERT INTO sii_documents (source, doc_type, title, content, url, metadata)
VALUES (
    'sii',
    'informativo',
    'Bienvenido al Sistema de Contabilidad Inteligente',
    'Este sistema está diseñado para ayudarte con consultas de contabilidad y tributación chilena.
    Puedes hacer preguntas sobre IVA, normativas del SII, cálculos tributarios y más.

    El sistema aprende continuamente de las normativas del SII para brindarte información actualizada.',
    'https://www.sii.cl',
    '{"type": "welcome", "version": "1.0"}'
) ON CONFLICT (content_hash) DO NOTHING;

-- Log de inicialización
DO $$
BEGIN
    RAISE NOTICE 'Base de datos inicializada correctamente';
    RAISE NOTICE 'Extensión pgvector habilitada';
    RAISE NOTICE 'Tablas creadas: sii_documents, response_cache, conversations, scraping_jobs';
END $$;
