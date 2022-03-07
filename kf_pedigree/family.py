import sys

from d3b_utils.requests_retry import Session

import pandas as pd
import psycopg2
from tqdm import tqdm

from kf_pedigree.common import KF_API_URLS, get_logger, postgres_test

logger = get_logger(__name__, testing_mode=False)


def find_family_from_family_list(api_or_db_url, family_list):
    if api_or_db_url.startswith(("http:", "https:")):
        return _find_family_from_study_with_http_api(api_or_db_url, family_list)
    else:
        return _find_family_from_study_with_db_conn(api_or_db_url, family_list)


def _find_family_from_study_with_db_conn(db_url, family_list):
    # Test connection to the database
    if not postgres_test(db_url):
        logger.error("Can't connect to the database")
        sys.exit()
    with psycopg2.connect(db_url) as conn:
        logger.info(
            f"fetching families from {len(family_list)} families"  # noqa
        )
        families = pd.read_sql(
            f"""
            select f.kf_id, f.external_id, f.visible
            from family f
            where f.kf_id in ({str(family_list)[1:-1]})
            """,
            conn,
        )
        logger.info(f"{len(families)} families found")
        if len(families) == 0:
            logger.error("No family relationships found")
            sys.exit()
    return families


def _find_family_from_study_with_http_api(api_url, family_list):
    if api_url == KF_API_URLS.get(
        "kf_dataservice_url"
    ) or api_url == KF_API_URLS.get("kf_dataserviceqa_url"):
        logger.debug("detected dataservice api")
        return _find_family_from_study_with_dataservice_api(
            api_url, family_list
        )
    else:
        logger.error("Could not identify supplied api_url")
        sys.exit()


def _find_family_from_study_with_dataservice_api(api_url, family_list):
    logger.info(f"fetching families from {len(family_list)} families")  # noqa
    families = []
    for family in tqdm(family_list):
        resp = Session().get(f"{api_url}/families/{family}")
        logger.debug(resp.url)
        if not resp.ok:
            logger.error(f"request url {resp.url}")
            logger.error(resp.text)
        else:
            logger.debug(f"family {family} found")
            families.append(resp.json()["results"])
    logger.debug(f"{len(families)} families found")
    if len(families) == 0:
        logger.error("No families found")
        sys.exit()
    if len(families) != len(family_list):
        logger.error("Not all families found!")
        sys.exit()
    df = pd.DataFrame(families)
    df = df[["kf_id", "external_id", "visible"]]
    return df
