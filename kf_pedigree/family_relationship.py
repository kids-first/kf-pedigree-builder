import sys

from d3b_utils.requests_retry import Session

import pandas as pd
import psycopg2
from tqdm import tqdm

from kf_pedigree.common import KF_API_URLS, get_logger, postgres_test

logger = get_logger(__name__, testing_mode=False)


def find_fr_from_participant_list(api_or_db_url, participant_list):
    if api_or_db_url.startswith(("http:", "https:")):
        return _find_fr_from_study_with_http_api(
            api_or_db_url, participant_list
        )
    else:
        return _find_fr_from_study_with_db_conn(api_or_db_url, participant_list)


def _find_fr_from_study_with_db_conn(db_url, participant_list):
    # Test connection to the database
    if not postgres_test(db_url):
        logger.error("Can't connect to the database")
        sys.exit()
    with psycopg2.connect(db_url) as conn:
        logger.info(
            f"fetching family relationships from {len(participant_list)} participants"  # noqa
        )
        logger.info("fetching participant1 - participant2 relationships")
        fr_1_2 = pd.read_sql(
            f"""
select fr.participant1_id participant1,
       fr.participant2_id participant2,
       fr.participant1_to_participant2_relation,
       fr.participant2_to_participant1_relation
  from participant p
  join family_relationship fr
    on p.kf_id = fr.participant1_id
 where p.kf_id in ({str(participant_list)[1:-1]})
 order by p.is_proband
 """,
            conn,
        )
        logger.debug(
            f"{len(fr_1_2)} family relationships where participant is p1 found"
        )
        logger.info("fetching participant2 - participant1 relationships")
        fr_2_1 = pd.read_sql(
            f"""
select fr.participant1_id participant1,
       fr.participant2_id participant2,
       fr.participant1_to_participant2_relation,
       fr.participant2_to_participant1_relation
  from participant p
  join family_relationship fr
    on p.kf_id = fr.participant2_id
 where p.kf_id in ({str(participant_list)[1:-1]})
 order by p.is_proband
 """,
            conn,
        )
        logger.debug(
            f"{len(fr_2_1)} family relationships where participant is p2 found"
        )
        fr = pd.concat([fr_1_2, fr_2_1]).drop_duplicates()
        if len(fr) == 0:
            logger.error("No family relationships found")
            sys.exit()
    return fr


def _find_fr_from_study_with_http_api(api_url, participant_list):
    if api_url == KF_API_URLS.get(
        "kf_dataservice_url"
    ) or api_url == KF_API_URLS.get("kf_dataserviceqa_url"):
        logger.debug("detected dataservice api")
        return _find_fr_from_study_with_dataservice_api(
            api_url, participant_list
        )
    else:
        logger.error("Could not identify supplied api_url")
        sys.exit()


def _find_fr_from_study_with_dataservice_api(api_url, participant_list):
    logger.info(
        f"fetching family relationships from {len(participant_list)} participants"  # noqa
    )
    family_relationships = []
    for participant in tqdm(participant_list):
        resp = Session().get(
            f"{api_url}/family-relationships?participant_id={participant}"
        )
        logger.debug(resp.url)
        if not resp.ok:
            logger.error(f"request url {resp.url}")
            logger.error(resp.text)
        else:
            logger.debug(f"{resp.json()['total']} relationships found")
            family_relationships.extend(resp.json()["results"])
    logger.debug(f"{len(family_relationships)} participants found")
    if len(family_relationships) == 0:
        logger.error("No family_relationships found")
        sys.exit()
    df = pd.DataFrame(family_relationships)
    # get the links
    links = (
        df["_links"]
        .apply(pd.Series)[["participant1", "participant2", "self"]]
        .apply(lambda x: x.str.rpartition("/")[2], axis=1)
    )
    links.columns = [
        "participant1",
        "participant2",
        "kf_id",
    ]
    df = df[
        [
            "participant1_to_participant2_relation",
            "participant2_to_participant1_relation",
            "kf_id",
        ]
    ]
    return df.merge(links, how="outer", on="kf_id")[
        [
            "participant1",
            "participant2",
            "participant1_to_participant2_relation",
            "participant2_to_participant1_relation",
        ]
    ]
