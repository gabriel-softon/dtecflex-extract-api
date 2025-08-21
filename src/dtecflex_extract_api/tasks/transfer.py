from celery.utils.log import get_task_logger
from src.dtecflex_extract_api.config.celery import celery_app
from src.dtecflex_extract_api.services.transfer_service import run_transfer

logger = get_task_logger(__name__)

@celery_app.task(name="dtecflex.transfer", bind=True, queue="transfer",
                 max_retries=0, soft_time_limit=60*30)
def transfer_task(self, date_directory: str | None = None, category: str | None = None):
    return run_transfer(date_directory=date_directory, category=category, logger=logger)