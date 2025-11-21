from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from uuid import uuid4
from .queue import TaskQueue


class JobScheduler:
    def __init__(self, queue: TaskQueue):
        self.queue = queue
        self.scheduler = BackgroundScheduler()

    def add_cron_job(
        self, job_name: str, payload: dict, cron_expression: str, job_id: str = None
    ):
        job_id = job_id or f"cron_{job_name}_{uuid4()}"
        self.scheduler.add_job(
            func=lambda: self.queue.add(job_name, payload),
            trigger=CronTrigger.from_crontab(cron_expression),
            id=job_id,
            replace_existing=True,
        )

    def start(self):
        self.scheduler.start()

    def stop(self):
        self.scheduler.shutdown()
