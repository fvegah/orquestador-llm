# Arquitectura del Sistema de Contabilidad Inteligente

## Visión General

Sistema modular para scraping del SII, análisis con IA y respuestas a consultas contables via Slack.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SLACK BOT                                       │
│                    (Interfaz de usuario - Preguntas/Respuestas)             │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ORQUESTADOR LLM                                    │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │ Response Cache  │  │  LLM Router     │  │  Task Scheduler             │  │
│  │ (Redis)         │◄─┤  (Ollama/OpenAI)│  │  (APScheduler)              │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────┘  │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
┌───────────────────────┐ ┌───────────────────┐ ┌───────────────────────────┐
│     SII SCRAPER       │ │  KNOWLEDGE BASE   │ │   DATA CONNECTORS         │
│  ┌─────────────────┐  │ │  ┌─────────────┐  │ │  ┌─────────────────────┐  │
│  │ Normativa       │  │ │  │ PostgreSQL  │  │ │  │ API Compras/Ventas  │  │
│  │ Circulares      │  │ │  │ + pgvector  │  │ │  │ API Facturas        │  │
│  │ Resoluciones    │  │ │  │             │  │ │  │ Futuras Fuentes...  │  │
│  │ Formularios     │  │ │  │ Embeddings  │  │ │  └─────────────────────┘  │
│  └─────────────────┘  │ │  │ Semantic    │  │ │                           │
│                       │ │  │ Search      │  │ │                           │
└───────────────────────┘ └───────────────────┘ └───────────────────────────┘
```

## Componentes Principales

### 1. SII Scraper (`app/scrapers/sii/`)
- **Propósito**: Extraer información del Servicio de Impuestos Internos de Chile
- **Contenido a extraer**:
  - Normativas tributarias
  - Circulares
  - Resoluciones
  - Formularios y sus instrucciones
  - Tablas de valores (UTM, UF, etc.)
- **Scheduler**: Ejecución programada (diaria/semanal)

### 2. Knowledge Base (`app/knowledge/`)
- **PostgreSQL + pgvector**: Almacenamiento con búsqueda vectorial
- **Embeddings**: Generación de embeddings para búsqueda semántica
- **Indexación**: Documentos del SII indexados para consultas rápidas

### 3. Response Cache (`app/cache/`)
- **Redis**: Cache de respuestas IA
- **Estrategia**: Hash de pregunta + contexto como key
- **TTL configurable**: Expiración de respuestas cacheadas
- **Beneficio**: Reducción de costos de IA

### 4. Slack Bot (`app/integrations/slack/`)
- **Slack Bolt**: Framework para bots de Slack
- **Comandos**:
  - `/consulta` - Preguntas de contabilidad
  - `/normativa` - Buscar normativas del SII
  - `/iva` - Consultas específicas de IVA
- **Mentions**: Responder a menciones directas

### 5. Task Orchestrator (`app/orchestrator/`)
- **Extensible**: Sistema de plugins para nuevas fuentes de datos
- **Scheduler**: APScheduler para tareas programadas
- **Eventos**: Sistema de eventos para comunicación entre componentes

## Flujo de Datos

### Scraping (Scheduled)
```
Scheduler → SII Scraper → Parse HTML → Extract Data → Generate Embeddings → Store in PostgreSQL
```

### Consulta Usuario
```
Slack Message → Bot Handler → Check Response Cache
    │
    ├─ Cache HIT → Return Cached Response
    │
    └─ Cache MISS → Search Knowledge Base → Build Context → LLM Query → Cache Response → Return
```

## Base de Datos

### PostgreSQL Tables

```sql
-- Documentos del SII
CREATE TABLE sii_documents (
    id SERIAL PRIMARY KEY,
    doc_type VARCHAR(50),       -- 'circular', 'resolucion', 'normativa', 'formulario'
    doc_number VARCHAR(100),
    title TEXT,
    content TEXT,
    url VARCHAR(500),
    published_date DATE,
    scraped_at TIMESTAMP DEFAULT NOW(),
    embedding vector(1536)      -- pgvector para embeddings
);

-- Cache de respuestas
CREATE TABLE response_cache (
    id SERIAL PRIMARY KEY,
    question_hash VARCHAR(64),
    context_hash VARCHAR(64),
    question TEXT,
    response TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP,
    hit_count INTEGER DEFAULT 0
);

-- Historial de conversaciones
CREATE TABLE conversations (
    id SERIAL PRIMARY KEY,
    slack_user_id VARCHAR(50),
    slack_channel_id VARCHAR(50),
    question TEXT,
    response TEXT,
    source_documents JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
```

## Configuración de Entorno

```env
# Base de datos
DATABASE_URL=postgresql://user:pass@localhost:5432/contabilidad_db

# Redis
REDIS_URL=redis://localhost:6379/0

# LLM
LLM_SERVICE=ollama  # o 'openai'
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=mistral
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4

# Slack
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
SLACK_SIGNING_SECRET=...

# Embeddings
EMBEDDING_MODEL=text-embedding-ada-002  # o modelo local

# Scheduler
SCRAPING_SCHEDULE=0 2 * * *  # Diario a las 2 AM

# Cache
RESPONSE_CACHE_TTL=86400  # 24 horas en segundos
```

## Extensibilidad

### Agregar Nueva Fuente de Datos

1. Crear nuevo scraper en `app/scrapers/nueva_fuente/`
2. Implementar interfaz `BaseDataSource`
3. Registrar en el orquestador
4. Configurar scheduler si es necesario

```python
from app.orchestrator.base import BaseDataSource

class NuevaFuenteDataSource(BaseDataSource):
    async def fetch(self) -> List[Document]:
        # Implementar lógica de extracción
        pass

    async def process(self, documents: List[Document]) -> None:
        # Procesar y almacenar
        pass
```
