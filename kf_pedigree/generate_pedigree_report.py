import sys

from kf_pedigree.common import get_logger, kf_api_url
from kf_pedigree.family_relationship import find_fr_from_participant_list
from kf_pedigree.output import build_report, save_pedigree
from kf_pedigree.study import find_pts_from_study

logger = get_logger(__name__, testing_mode=False)


def gender(x):
    if x.lower() == "male":
        return "1"
    elif x.lower() == "female":
        return "2"
    else:
        return "0"


def status(x):
    if x:
        return "2"
    elif not x:
        return "1"
    else:
        return "0"


def parent(x):
    if x != x:
        return "0"
    else:
        return x


def generate_report(
    study_id=None,
    participant_csv=None,
    db_url=None,
    api_url="https://kf-api-dataservice.kidsfirstdrc.org",
    output_file="pedigree_report.csv",
    only_visible=True,
    use_external_ids=False,
):
    # validating
    error = False
    if not api_url:
        api_url = kf_api_url
        logger.info(f"setting api url to {api_url}")

    if db_url:
        connection_url = db_url
    else:
        connection_url = api_url

    if study_id:
        logger.info("Generating pedigree report from study id")
        participants = find_pts_from_study(connection_url, study_id)
    elif participant_csv:
        logger.info("Generating pedigree report from participant list")
        # load the participant list and then get the participant info
    else:
        logger.error("Must supply either a study ID or a participant list")
        error = True
    if error:
        sys.exit()
    # filter participant list to only visible participants
    if only_visible:
        logger.info("dropping non-visible participants from report")
        participants = participants[participants["visible"]]
    # fetch family relationships
    family_relationships = find_fr_from_participant_list(
        connection_url, participants["kf_id"].to_list()
    )
    # filter family relationships to only include information for participants
    #  that are released
    family_relationships = family_relationships[
        (family_relationships["participant1"].isin(participants["kf_id"]))
        & (family_relationships["participant2"].isin(participants["kf_id"]))
    ]
    report = build_report(
        participants=participants,
        family_relationships=family_relationships,
        use_external_ids=use_external_ids,
        api_or_db_url=connection_url,
    )
    if not output_file:
        output_file = "pedigree_report.csv"
    save_pedigree(report, output_file)
