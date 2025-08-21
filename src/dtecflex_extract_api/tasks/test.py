from src.dtecflex_extract_api.config.celery import celery_app


@celery_app.task(bind=True, name="app.tasks.add", max_retries=0, soft_time_limit=10)
def add(self, a: int, b: int) -> int:
    """Tarefa simples que soma dois números."""
    return a + b

@celery_app.task(name="app.tasks.ping")
def ping() -> str:
    """Tarefa bem simples só pra validar a fila."""
    return "pong"
