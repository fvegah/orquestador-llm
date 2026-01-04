# Orquestador LLM - Sistema de Contabilidad Inteligente

Sistema modular para scraping del SII (Servicio de Impuestos Internos de Chile), análisis con IA y respuestas a consultas contables vía Slack.

## Características

- **Scraping Automático del SII**: Extrae normativas, circulares, resoluciones y valores tributarios
- **Knowledge Base con Búsqueda Semántica**: PostgreSQL + pgvector para almacenamiento y búsqueda vectorial
- **Cache de Respuestas**: Redis para respuestas rápidas y reducción de costos de IA
- **Bot de Slack**: Interfaz conversacional para consultas contables
- **LLM Configurable**: Soporte para Ollama (local) u OpenAI (cloud)
- **Orquestador Extensible**: Fácil agregar nuevas fuentes de datos

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────────┐
│                              SLACK BOT                               │
│                    (Interfaz de usuario - Preguntas/Respuestas)     │
└─────────────────────────────────────┬───────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                           ORQUESTADOR LLM                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │
│  │ Response Cache  │  │  LLM Router     │  │  Task Scheduler     │  │
│  │ (Redis)         │◄─┤  (Ollama/OpenAI)│  │  (APScheduler)      │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────┘  │
└─────────────────────────────────────┬───────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
┌───────────────────────┐ ┌───────────────────┐ ┌───────────────────┐
│     SII SCRAPER       │ │  KNOWLEDGE BASE   │ │   DATA CONNECTORS │
│  - Normativas         │ │  PostgreSQL       │ │  - APIs externas  │
│  - Circulares         │ │  + pgvector       │ │  - Nuevas fuentes │
│  - Valores UTM/UF     │ │  Embeddings       │ │                   │
└───────────────────────┘ └───────────────────┘ └───────────────────┘
```

## Inicio Rápido

### 1. Clonar y Configurar

```bash
git clone <repo-url>
cd orquestador-llm

# Crear archivo de configuración
cp .env.example .env

# Editar .env con tus configuraciones
nano .env
```

### 2. Configurar Variables de Entorno

Editar `.env` con los valores apropiados:

```env
# LLM (elegir ollama para local, openai para cloud)
LLM_SERVICE=ollama
OLLAMA_MODEL=mistral

# O usar OpenAI
# LLM_SERVICE=openai
# OPENAI_API_KEY=sk-your-key

# Slack Bot (opcional)
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
```

### 3. Iniciar con Docker

```bash
# Iniciar todos los servicios
docker-compose up -d

# Ver logs
docker-compose logs -f app

# Iniciar con herramientas de admin (pgAdmin, Redis Commander)
docker-compose --profile admin up -d
```

### 4. Verificar Estado

```bash
# Health check
curl http://localhost:8000/health

# Estado completo
curl http://localhost:8000/status
```

## Uso

### API REST

#### Hacer una Pregunta
```bash
curl -X POST http://localhost:8000/preguntar \
  -H "Content-Type: application/json" \
  -d '{
    "rut": "12345678-9",
    "pregunta": "¿Cómo calculo el IVA de mis ventas?"
  }'
```

#### Buscar Documentos
```bash
curl -X POST http://localhost:8000/buscar \
  -H "Content-Type: application/json" \
  -d '{
    "query": "circular IVA crédito fiscal",
    "limit": 5
  }'
```

#### Disparar Scraping Manual
```bash
curl -X POST http://localhost:8000/admin/scrape \
  -H "Content-Type: application/json" \
  -d '{"source": "sii"}'
```

### Slack Bot

Una vez configurado, puedes interactuar con el bot de las siguientes formas:

- **Mensaje directo**: Envía un DM al bot con tu pregunta
- **Mención**: Menciona al bot en un canal: `@ContabilidadBot ¿cómo declaro el F29?`
- **Comandos slash**:
  - `/consulta ¿cuál es la tasa de IVA?`
  - `/normativa circular IVA`
  - `/iva ¿cómo calculo el débito fiscal?`
  - `/ayuda`

## Estructura del Proyecto

```
orquestador-llm/
├── app/
│   ├── main.py                    # Aplicación FastAPI principal
│   ├── core/                      # Módulos core (LLM, Redis, Kafka)
│   ├── services/                  # Servicios de datos
│   ├── models/                    # Esquemas Pydantic
│   ├── scrapers/
│   │   └── sii/                   # Scraper del SII
│   │       ├── scraper.py         # Lógica de scraping
│   │       ├── parser.py          # Parsing de HTML
│   │       └── models.py          # Modelos de datos
│   ├── orchestrator/              # Orquestador de tareas
│   │   ├── base.py                # Clases base para fuentes
│   │   ├── scheduler.py           # Scheduler de tareas
│   │   └── orchestrator.py        # Orquestador principal
│   ├── knowledge/                 # Knowledge Base
│   │   ├── embeddings.py          # Servicio de embeddings
│   │   └── knowledge_base.py      # PostgreSQL + pgvector
│   ├── cache/                     # Cache de respuestas
│   │   └── response_cache.py      # Cache en Redis
│   └── integrations/
│       └── slack/                 # Bot de Slack
│           ├── bot.py             # Bot principal
│           └── handlers.py        # Manejadores de mensajes
├── scripts/
│   └── init-db.sql                # Inicialización de BD
├── docker-compose.yml             # Orquestación Docker
├── Dockerfile                     # Imagen de la aplicación
├── requirements.txt               # Dependencias Python
├── .env.example                   # Ejemplo de configuración
└── ARCHITECTURE.md                # Documentación de arquitectura
```

## Agregar Nueva Fuente de Datos

El sistema es extensible. Para agregar una nueva fuente:

```python
from app.orchestrator.base import BaseDataSource, DataSourceRegistry, Document, FetchResult

@DataSourceRegistry.register
class MiFuenteDataSource(BaseDataSource):
    name = "mi_fuente"
    description = "Mi nueva fuente de datos"

    async def fetch(self, **kwargs) -> FetchResult:
        # Implementar lógica de extracción
        documents = []
        # ... obtener datos ...
        return FetchResult(success=True, documents=documents)

    async def process(self, documents: List[Document]) -> None:
        # Procesar y almacenar documentos
        pass
```

Luego registrar en el orquestador:
```python
from app.orchestrator import Orchestrator
from mi_modulo import MiFuenteDataSource

orchestrator = Orchestrator(knowledge_base=kb)
orchestrator.register_source(
    source=MiFuenteDataSource(),
    schedule_cron="0 3 * * *"  # Cada día a las 3 AM
)
```

## Configuración de Slack

### Crear App en Slack

1. Ir a [api.slack.com/apps](https://api.slack.com/apps)
2. Crear nueva app "From scratch"
3. Habilitar **Socket Mode** en Settings > Socket Mode
4. En **OAuth & Permissions**, agregar scopes:
   - `app_mentions:read`
   - `chat:write`
   - `commands`
   - `im:history`
   - `im:read`
   - `im:write`
5. En **Event Subscriptions**, suscribirse a:
   - `app_mention`
   - `message.im`
6. Crear comandos slash en **Slash Commands**:
   - `/consulta`
   - `/normativa`
   - `/iva`
   - `/ayuda`
7. Instalar app en workspace
8. Copiar tokens a `.env`

## Desarrollo

### Ejecutar Localmente (sin Docker)

```bash
# Crear entorno virtual
python -m venv venv
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt

# Iniciar servicios externos
docker-compose up -d postgres redis ollama

# Ejecutar aplicación
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Tests

```bash
pytest tests/ -v
```

## Endpoints API

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/status` | Estado completo del sistema |
| POST | `/preguntar` | Hacer pregunta de contabilidad |
| POST | `/buscar` | Buscar documentos en knowledge base |
| POST | `/admin/scrape` | Disparar scraping manual |
| POST | `/admin/update-cache` | Actualizar caché de datos |
| POST | `/admin/cache/invalidate` | Invalidar caché de respuestas |
| GET | `/admin/jobs` | Listar jobs programados |

## Servicios Docker

| Servicio | Puerto | Descripción |
|----------|--------|-------------|
| app | 8000 | Aplicación principal |
| postgres | 5432 | Base de datos PostgreSQL + pgvector |
| redis | 6379 | Cache Redis |
| ollama | 11434 | LLM local |
| pgadmin | 5050 | Admin PostgreSQL (profile: admin) |
| redis-commander | 8081 | Admin Redis (profile: admin) |

## Licencia

MIT
