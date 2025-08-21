from pydantic_settings import BaseSettings, SettingsConfigDict
from celery import Celery
from kombu import Exchange, Queue

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Celery
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/1"

    # Transfer service (ajuste no .env)
    MEDIA_BASE: str = "/media/noticias_www"
    REMOTE_BASE: str = "/mnt/dtecflex-site-root"
    SSH_USER: str = "ubuntu"
    SSH_HOST: str = "dtec-flex.com.br"
    SSH_PORT: int = 8022
    SSH_KEY_PATH: str = "/home/softon/keypairs/rsa_key_file_3072"

    # MySQL
    DB_USER: str | None = None
    DB_PASS: str | None = None
    DB_HOST: str | None = None
    DB_PORT: int | None = None
    DB_NAME: str | None = None

settings = Settings()

celery_app = Celery(
    "dtecflex_extract_api",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_track_started=True,
    task_send_sent_event=True,
    worker_send_task_events=True,
    result_expires=3600,

    # filas explícitas
    task_default_queue="celery",
    task_queues=(
        Queue("celery", Exchange("celery"), routing_key="celery"),
        Queue("transfer", Exchange("transfer"), routing_key="transfer"),
    ),
    task_default_exchange="celery",
    task_default_routing_key="celery",

    include=["src.dtecflex_extract_api.tasks.transfer"],  # garante import
)

celery_app.autodiscover_tasks(["dtecflex_extract_api"], related_name="tasks")
