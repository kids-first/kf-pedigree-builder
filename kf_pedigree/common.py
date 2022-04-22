import logging
import os
import sys

import colorlog
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# create logging directory if it doesn't exist
if not os.path.exists("logs"):
    os.makedirs("logs")


def get_logger(dunder_name, testing_mode) -> logging.Logger:
    log_format = (
        "%(asctime)s - "
        "%(name)s - "
        "%(funcName)s - "
        "%(levelname)s - "
        "%(message)s"
    )
    bold_seq = "\033[1m"
    colorlog_format = f"{bold_seq} " "%(log_color)s " f"{log_format}"
    colorlog.basicConfig(format=colorlog_format)
    logger = logging.getLogger(dunder_name)

    if testing_mode:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Output full log
    fh = logging.FileHandler("logs/app.log")
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(log_format)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Output warning log
    fh = logging.FileHandler("logs/app.warning.log")
    fh.setLevel(logging.WARNING)
    formatter = logging.Formatter(log_format)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Output error log
    fh = logging.FileHandler("logs/app.error.log")
    fh.setLevel(logging.ERROR)
    formatter = logging.Formatter(log_format)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


def postgres_test(db_url):
    try:
        logger.debug("attempting to connect to db")
        conn = psycopg2.connect(db_url, connect_timeout=1)
        conn.close()
        return True
    except psycopg2.OperationalError:
        logger.debug("connection failed")
        return False


KF_API_URLS = {
    "kf_dataservice_url": "https://kf-api-dataservice.kidsfirstdrc.org",
    "kf_dataserviceqa_url": "https://kf-api-dataservice.kidsfirstdrc.org",
    "kf_fhir_url": "https://kf-api-fhir-service.kidsfirstdrc.org/",
}
