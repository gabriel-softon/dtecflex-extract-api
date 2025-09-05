from celery.utils.log import get_task_logger
from dtecflex_extract_api.utils.pubsub import publish, release_lock, save_meta
from src.dtecflex_extract_api.config.celery import celery_app
from src.dtecflex_extract_api.services.transfer_service import run_transfer

logger = get_task_logger(__name__)

@celery_app.task(name="dtecflex.transfer", bind=True, queue="transfer",
                 max_retries=0, soft_time_limit=60*30)
def transfer_task(self, date_directory: str | None = None, category: str | None = None, job_key: str | None = None):
    key = job_key
    try:
        def progress_cb(step: int, total: int, phase: str, extra: dict | None = None):
            pct = int((step / total) * 100) if total else 0
            payload = {
                "event": "PROGRESS",
                "task_id": self.request.id,
                "progress": pct,
                "state": phase,
                "step": step, "total": total,
                "date": date_directory, "category": category, "key": key,
                **(extra or {})
            }
            save_meta(key, **payload)   # ✅ sem colisão
            publish(key, payload)
            self.update_state(state="PROGRESS", meta=payload)

        result = run_transfer(date_directory=date_directory, category=category, logger=logger, progress_cb=progress_cb)

        done_payload = {
            "event": "DONE",
            "task_id": self.request.id,
            "progress": 100,
            "state": "DONE",
            "result": result,
            "date": date_directory, "category": category, "key": key
        }
        save_meta(key, **done_payload)  # ✅ sem colisão
        publish(key, done_payload)
        return result

    except Exception as e:
        fail_payload = {
            "event": "FAILED",
            "task_id": self.request.id,
            "progress": 0,
            "state": "FAILED",
            "error": str(e),
            "date": date_directory, "category": category, "key": key
        }
        save_meta(key, **fail_payload)  # ✅ sem colisão
        publish(key, fail_payload)
        raise
    finally:
        if key:
            release_lock(key)