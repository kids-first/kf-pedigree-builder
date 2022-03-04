"""
Method for finding participants from studies
"""
import sys

from kf_utils.dataservice.scrape import yield_entities_from_filter

import pandas as pd
import psycopg2

from kf_pedigree.common import get_logger, postgres_test

logger = get_logger(__name__, testing_mode=False)


def find_pts_from_study(api_or_db_url, study_id):
    if api_or_db_url.startswith(("http:", "https:")):
        return _find_pts_from_study_with_http_api(api_or_db_url, study_id)
    else:
        return _find_pts_from_study_with_db_conn(api_or_db_url, study_id)


def _find_pts_from_study_with_db_conn(db_url, study_id):
    # Test connection to the database
    if not postgres_test(db_url):
        logger.error("Can't connect to the database")
        sys.exit()
    with psycopg2.connect(db_url) as conn:
        logger.info(f"fetching participants in {study_id}")
        participants = pd.read_sql(
            f"""
SELECT p.kf_id,
       p.external_id,
       p.affected_status,
       p.is_proband,
       p.ethnicity,
       p.race,
       p.gender,
       p.visible,
       p.family_id,
       p.study_id
  FROM participant p
 WHERE p.study_id = '{study_id}'
 """,
            conn,
        )
        logger.debug(f"{len(participants)} participants found")
        if len(participants) == 0:
            logger.error("No participants found")
            sys.exit()
    return participants


def _find_pts_from_study_with_http_api(api_url, study_id):
    logger.info(f"fetching participants in {study_id}")
    participants = [
        p
        for p in yield_entities_from_filter(
            api_url, "participants", {"study_id": study_id}, show_progress=True
        )
    ]
    logger.debug(f"{len(participants)} participants found")
    if not participants:
        logger.erro("No participants found")
        sys.exit()
    df = pd.DataFrame(participants)
    # get the links
    links = (
        df["_links"]
        .apply(pd.Series)[["family", "self", "study"]]
        .apply(lambda x: x.str.rpartition("/")[2], axis=1)
    )
    links.columns = ["family_id", "kf_id", "study_id"]
    df = df[
        [
            "kf_id",
            "external_id",
            "affected_status",
            "is_proband",
            "ethnicity",
            "race",
            "gender",
            "visible",
        ]
    ]
    return df.merge(links, how="outer", on="kf_id")
