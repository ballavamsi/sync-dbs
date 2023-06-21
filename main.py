import os
from apscheduler.schedulers.blocking import BlockingScheduler

import os
import datetime

from helpers.constants import *
from helpers.logging import logger
from helpers.syncdb import syncdbs


def scheduled_job():
    logger.info("starting the process")
    syncdbs()
    logger.info("process completed")


if __name__ == "__main__":
    run_on_startup = os.getenv("RUN_ON_STARTUP", "0")
    if run_on_startup == "1":
        scheduled_job()
    scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        scheduled_job,
        "interval",
        hours=12,
        start_date="2023-01-01 00:00:00",
        misfire_grace_time=60 * 60 * 2,
    )
    scheduler.start()
