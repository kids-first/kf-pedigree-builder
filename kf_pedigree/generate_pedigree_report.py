import sys

from kf_pedigree.common import KF_API_URLS, get_logger
from kf_pedigree.family import find_pts_from_family
from kf_pedigree.family_relationship import find_fr_from_participant_list
from kf_pedigree.output import build_metadata_report, build_report, save_report
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
    study_list=None,
    participant_csv=None,
    family_list=None,
    db_url=None,
    api_url=KF_API_URLS.get("kf_dataservice_url"),
    output_file="pedigree_report.csv",
    only_visible=True,
    use_external_ids=False,
    metadata=False,
):
    # validating
    error = False
    if db_url:
        connection_url = db_url
    else:
        connection_url = api_url

    if study_list:
        logger.info("Generating pedigree report from study id")
        participants = find_pts_from_study(connection_url, study_list)
    elif family_list:
        logger.info("Generating pedigree report from family_id(s)")
        participants = find_pts_from_family(connection_url, family_list)
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
    # that are released
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

    save_report(report, output_file)
    if metadata:
        metadata_report = build_metadata_report(
            participants=participants,
            api_or_db_url=connection_url,
        )
        parts = output_file.rsplit(".", 1)
        save_report(metadata_report, (parts[0] + "-metadata." + parts[1]))
