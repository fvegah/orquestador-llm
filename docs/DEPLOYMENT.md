# Guía de Despliegue - MVP Gratuito

## Stack Recomendado (Gratis o muy económico)

| Componente | Servicio | Costo |
|------------|----------|-------|
| App FastAPI | [Railway](https://railway.app) | Gratis (500 hrs/mes) |
| PostgreSQL | [Supabase](https://supabase.com) | Gratis (500 MB) |
| Redis | [Upstash](https://upstash.com) | Gratis (10K/día) |
| LLM | [OpenAI API](https://platform.openai.com) | ~$5/mes |

**Costo total estimado: $0-10/mes**

---

## Paso 1: Configurar Supabase (PostgreSQL + pgvector)

1. Crear cuenta en [supabase.com](https://supabase.com)
2. Crear nuevo proyecto
3. Ir a **Settings > Database** y copiar la connection string:
   ```
   postgresql://postgres:[PASSWORD]@db.[PROJECT].supabase.co:5432/postgres
   ```
4. En el **SQL Editor**, ejecutar:
   ```sql
   -- Habilitar pgvector
   CREATE EXTENSION IF NOT EXISTS vector;
   ```
5. Ejecutar el contenido de `scripts/init-db.sql`

---

## Paso 2: Configurar Upstash (Redis)

1. Crear cuenta en [upstash.com](https://upstash.com)
2. Crear nueva base de datos Redis
3. Copiar la **UPSTASH_REDIS_REST_URL**:
   ```
   redis://default:[PASSWORD]@[REGION].upstash.io:6379
   ```

---

## Paso 3: Configurar OpenAI

1. Crear cuenta en [platform.openai.com](https://platform.openai.com)
2. Ir a **API Keys** y crear una nueva key
3. Agregar $5-10 de crédito (pay as you go)

---

## Paso 4: Desplegar en Railway

### Opción A: Deploy desde GitHub (Recomendado)

1. Crear cuenta en [railway.app](https://railway.app)
2. Click **"New Project"** > **"Deploy from GitHub repo"**
3. Seleccionar el repositorio `orquestador-llm`
4. Railway detectará automáticamente el Dockerfile

### Opción B: Deploy con Railway CLI

```bash
# Instalar CLI
npm install -g @railway/cli

# Login
railway login

# Crear proyecto
railway init

# Desplegar
railway up
```

---

## Paso 5: Configurar Variables de Entorno

En Railway, ir a **Variables** y agregar:

```env
# Base de datos (Supabase)
DATABASE_URL=postgresql://postgres:[PASSWORD]@db.[PROJECT].supabase.co:5432/postgres

# Redis (Upstash)
REDIS_URL=redis://default:[PASSWORD]@[REGION].upstash.io:6379

# LLM
LLM_SERVICE=openai
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-3.5-turbo

# Embeddings
EMBEDDING_MODEL=text-embedding-ada-002

# Slack (opcional)
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
SLACK_SIGNING_SECRET=...

# Otros
LOG_LEVEL=INFO
SCRAPING_SCHEDULE=0 2 * * *
```

---

## Paso 6: Verificar Despliegue

```bash
# Health check
curl https://tu-app.railway.app/health

# Estado
curl https://tu-app.railway.app/status

# Probar pregunta
curl -X POST https://tu-app.railway.app/preguntar \
  -H "Content-Type: application/json" \
  -d '{"pregunta": "¿Cuál es la tasa de IVA en Chile?"}'
```

---

## Alternativas de Despliegue

### Render.com (Alternativa a Railway)

1. Crear cuenta en [render.com](https://render.com)
2. **New** > **Web Service**
3. Conectar repositorio GitHub
4. Configurar:
   - **Runtime**: Docker
   - **Plan**: Free
5. Agregar variables de entorno

### Fly.io (Más control)

```bash
# Instalar CLI
curl -L https://fly.io/install.sh | sh

# Login
fly auth login

# Crear app
fly launch

# Desplegar
fly deploy

# Ver logs
fly logs
```

---

## Optimizaciones para MVP

### 1. Reducir costos de OpenAI

```python
# En .env usar modelo más económico
OPENAI_MODEL=gpt-3.5-turbo  # ~$0.002/1K tokens vs gpt-4 ~$0.06/1K
```

### 2. Cache agresivo

```python
# Aumentar TTL en .env
RESPONSE_CACHE_TTL=604800  # 7 días en vez de 24 horas
```

### 3. Limitar scraping

```python
# En .env, scraping semanal en vez de diario
SCRAPING_SCHEDULE=0 2 * * 0  # Solo domingos a las 2 AM
```

---

## Arquitectura MVP Simplificada

```
┌──────────────────┐     ┌──────────────────┐
│   Slack Bot      │     │   API REST       │
│   (opcional)     │     │   /preguntar     │
└────────┬─────────┘     └────────┬─────────┘
         │                        │
         └───────────┬────────────┘
                     ▼
         ┌───────────────────────┐
         │   Railway / Render    │
         │   (FastAPI App)       │
         └───────────┬───────────┘
                     │
       ┌─────────────┼─────────────┐
       ▼             ▼             ▼
┌───────────┐ ┌───────────┐ ┌───────────┐
│ Supabase  │ │  Upstash  │ │  OpenAI   │
│ PostgreSQL│ │  Redis    │ │  API      │
└───────────┘ └───────────┘ └───────────┘
```

---

## Costos Estimados

| Uso | Costo Mensual |
|-----|---------------|
| Bajo (< 100 consultas/día) | $0-5 |
| Medio (100-500 consultas/día) | $5-15 |
| Alto (500+ consultas/día) | $15-30 |

**Nota**: El mayor costo será OpenAI. El resto es gratis en los free tiers.

---

## Monitoreo Gratuito

- **Railway**: Dashboard incluido
- **Supabase**: Dashboard con métricas
- **Upstash**: Métricas de uso
- **BetterStack** (Logtail): Logs gratis hasta 1GB/mes

---

## Próximos Pasos para Escalar

Cuando el MVP valide el producto:

1. **Migrar a Ollama** en GPU cloud (Vast.ai, RunPod) para reducir costos LLM
2. **Aumentar tiers** en Supabase/Upstash si se necesita
3. **Kubernetes** para alta disponibilidad
