"""
Method for finding participants from studies
"""
import sys

from kf_utils.dataservice.scrape import (
    yield_entities_from_filter,
    yield_entities_from_kfids,
)

import pandas as pd
import psycopg2

from kf_pedigree.common import get_logger, postgres_test

logger = get_logger(__name__, testing_mode=False)


def study_db_connection(db_url, study_list):
    # Test connection to the database
    if not postgres_test(db_url):
        logger.error("Can't connect to the database")
        sys.exit()
    if len(study_list) == 1:
        study_list = [study_list[0]]
    return study_list


def find_pts_from_study(api_or_db_url, study_list):
    if api_or_db_url.startswith(("http:", "https:")):
        return _find_pts_from_study_with_http_api(api_or_db_url, study_list)
    else:
        return _find_pts_from_study_with_db_conn(api_or_db_url, study_list)


def _find_pts_from_study_with_db_conn(db_url, study_list):
    study_list = study_db_connection(db_url, study_list)
    with psycopg2.connect(db_url) as conn:
        logger.info(f"fetching participants in {study_list}")
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
 WHERE p.study_id in ({str(study_list)[1:-1]})
 """,
            conn,
        )
        logger.debug(f"{len(participants)} participants found")
        if len(participants) == 0:
            logger.error("No participants found")
            sys.exit()
    return participants


def _find_pts_from_study_with_http_api(api_url, study_list):
    logger.info(f"fetching participants in {study_list}")
    participants = []
    for s in study_list:
        study_participants = [
            p
            for p in yield_entities_from_filter(
                api_url, "participants", {"study_id": s}, show_progress=True
            )
        ]
        logger.debug(f"{len(study_participants)} participants found")
        if not study_participants:
            logger.erro("No participants found")
            sys.exit()
        participants.append(study_participants)
    df = pd.DataFrame(participants[0])
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


def find_studies_from_study(api_or_db_url, study_list):
    if api_or_db_url.startswith(("http:", "https:")):
        return _find_studies_from_study_with_http_api(api_or_db_url, study_list)
    else:
        return _find_studies_from_study_with_db_conn(api_or_db_url, study_list)


def _find_studies_from_study_with_db_conn(db_url, study_list):
    study_list = study_db_connection(db_url, study_list)
    with psycopg2.connect(db_url) as conn:
        logger.info(f"fetching studies: {study_list}")
        studies = pd.read_sql(
            f"""
SELECT s.kf_id as study_id,
       s.name as study_name,
       s.program as study_program,
       s.short_code as study_short_code,
       s.short_name as study_name
  FROM study s
 WHERE s.kf_id in ({str(study_list)[1:-1]})
 """,
            conn,
        )
        logger.debug(f"{len(studies)} studiesfound")
        if len(studies) == 0:
            logger.error("No studies found")
            sys.exit()
    return studies


def _find_studies_from_study_with_http_api(api_url, study_list):
    logger.info(f"fetching studies: {study_list}")
    studies = [
        s
        for s in yield_entities_from_kfids(
            api_url, study_list, show_progress=True
        )
    ]
    logger.debug(f"{len(studies)} studies found")
    if not studies:
        logger.erro("No participants found")
        sys.exit()
    df = pd.DataFrame(studies)
    # get the links
    df = df[["kf_id", "name", "program", "short_code", "short_name"]].rename(
        columns={
            "kf_id": "study_id",
            "name": "study_name",
            "program": "study_program",
            "short_code": "study_short_code",
            "short_name": "study_short_name",
        }
    )
    return df
