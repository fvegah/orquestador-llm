"""Scheduler de tareas para el orquestador."""
import logging
from datetime import datetime
from typing import Callable, Dict, List, Optional, Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.memory import MemoryJobStore

logger = logging.getLogger(__name__)


class TaskScheduler:
    """
    Scheduler para programar tareas de scraping y procesamiento.

    Soporta:
    - Tareas con expresión cron
    - Tareas con intervalo fijo
    - Tareas únicas (one-shot)
    """

    def __init__(self):
        self.scheduler = AsyncIOScheduler(
            jobstores={'default': MemoryJobStore()},
            job_defaults={
                'coalesce': True,  # Fusionar ejecuciones perdidas
                'max_instances': 1  # Solo una instancia a la vez
            }
        )
        self._jobs: Dict[str, Dict[str, Any]] = {}

    def add_cron_job(
        self,
        job_id: str,
        func: Callable,
        cron_expression: str,
        kwargs: Optional[Dict] = None,
        description: str = ""
    ) -> bool:
        """
        Agrega una tarea con expresión cron.

        Args:
            job_id: Identificador único de la tarea
            func: Función a ejecutar (async)
            cron_expression: Expresión cron (ej: "0 2 * * *" = 2 AM diario)
            kwargs: Argumentos para la función
            description: Descripción de la tarea

        Returns:
            True si se agregó exitosamente
        """
        try:
            # Parsear expresión cron
            parts = cron_expression.split()
            if len(parts) != 5:
                logger.error(f"Expresión cron inválida: {cron_expression}")
                return False

            trigger = CronTrigger(
                minute=parts[0],
                hour=parts[1],
                day=parts[2],
                month=parts[3],
                day_of_week=parts[4]
            )

            self.scheduler.add_job(
                func,
                trigger=trigger,
                id=job_id,
                kwargs=kwargs or {},
                replace_existing=True
            )

            self._jobs[job_id] = {
                "type": "cron",
                "cron": cron_expression,
                "description": description,
                "created_at": datetime.now().isoformat()
            }

            logger.info(f"Tarea cron agregada: {job_id} ({cron_expression})")
            return True

        except Exception as e:
            logger.error(f"Error agregando tarea cron {job_id}: {e}")
            return False

    def add_interval_job(
        self,
        job_id: str,
        func: Callable,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
        kwargs: Optional[Dict] = None,
        description: str = ""
    ) -> bool:
        """
        Agrega una tarea con intervalo fijo.

        Args:
            job_id: Identificador único de la tarea
            func: Función a ejecutar (async)
            hours: Intervalo en horas
            minutes: Intervalo en minutos
            seconds: Intervalo en segundos
            kwargs: Argumentos para la función
            description: Descripción de la tarea

        Returns:
            True si se agregó exitosamente
        """
        try:
            trigger = IntervalTrigger(
                hours=hours,
                minutes=minutes,
                seconds=seconds
            )

            self.scheduler.add_job(
                func,
                trigger=trigger,
                id=job_id,
                kwargs=kwargs or {},
                replace_existing=True
            )

            interval_str = f"{hours}h {minutes}m {seconds}s"
            self._jobs[job_id] = {
                "type": "interval",
                "interval": interval_str,
                "description": description,
                "created_at": datetime.now().isoformat()
            }

            logger.info(f"Tarea intervalo agregada: {job_id} (cada {interval_str})")
            return True

        except Exception as e:
            logger.error(f"Error agregando tarea intervalo {job_id}: {e}")
            return False

    def remove_job(self, job_id: str) -> bool:
        """Elimina una tarea programada."""
        try:
            self.scheduler.remove_job(job_id)
            self._jobs.pop(job_id, None)
            logger.info(f"Tarea eliminada: {job_id}")
            return True
        except Exception as e:
            logger.error(f"Error eliminando tarea {job_id}: {e}")
            return False

    def get_jobs(self) -> List[Dict[str, Any]]:
        """Obtiene información de todas las tareas programadas."""
        jobs = []
        for job_id, info in self._jobs.items():
            job = self.scheduler.get_job(job_id)
            if job:
                info["next_run"] = job.next_run_time.isoformat() if job.next_run_time else None
            jobs.append({"id": job_id, **info})
        return jobs

    def start(self):
        """Inicia el scheduler."""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler iniciado")

    def stop(self):
        """Detiene el scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler detenido")

    def pause_job(self, job_id: str) -> bool:
        """Pausa una tarea."""
        try:
            self.scheduler.pause_job(job_id)
            return True
        except Exception:
            return False

    def resume_job(self, job_id: str) -> bool:
        """Reanuda una tarea pausada."""
        try:
            self.scheduler.resume_job(job_id)
            return True
        except Exception:
            return False

    async def run_job_now(self, job_id: str) -> bool:
        """Ejecuta una tarea inmediatamente."""
        job = self.scheduler.get_job(job_id)
        if job:
            try:
                await job.func(**job.kwargs)
                return True
            except Exception as e:
                logger.error(f"Error ejecutando tarea {job_id}: {e}")
                return False
        return False
