import schedule
import time
from logger import logger


def start_scheduler(job):
    schedule.every().day.at("18:00").do(job)

    logger.info("ETL scheduler started. Waiting for next run...")

    while True:
        schedule.run_pending()
        time.sleep(60)

