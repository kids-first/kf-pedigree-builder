import sys

from d3b_utils.requests_retry import Session
from kf_utils.dataservice.scrape import yield_entities_from_filter

import pandas as pd
import psycopg2
from tqdm import tqdm

from kf_pedigree.common import get_logger, postgres_test

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


def find_pts_from_family(api_or_db_url, family_list):
    if api_or_db_url.startswith(("http:", "https:")):
        return _find_pts_from_family_with_http_api(api_or_db_url, family_list)
    else:
        return _find_pts_from_family_with_db_conn(api_or_db_url, family_list)


def _find_pts_from_family_with_db_conn(db_url, family_list):
    # Test connection to the database
    if not postgres_test(db_url):
        logger.error("Can't connect to the database")
        sys.exit()
    with psycopg2.connect(db_url) as conn:
        logger.info(f"fetching participants in {family_list}")
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
 WHERE p.family_id in ({str(family_list)[1:-1]})
 """,
            conn,
        )
        logger.debug(f"{len(participants)} participants found")
        if len(participants) == 0:
            logger.error("No participants found")
            sys.exit()
    return participants


def _find_pts_from_family_with_http_api(api_url, family_list):
    logger.info(f"fetching participants in {family_list}")
    participants = []
    for f in family_list:
        family_participants = [
            p
            for p in yield_entities_from_filter(
                api_url,
                "participants",
                {"family_id": f},
                show_progress=True,
            )
        ]
        logger.debug(f"{f}: {len(family_participants)} participants found")
        if not family_participants:
            logger.erro("No participants found")
            sys.exit()
        participants.append(family_participants)
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
