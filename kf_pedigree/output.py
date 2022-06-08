import pandas as pd

from kf_pedigree.common import get_logger
from kf_pedigree.family import find_family_from_family_list
from kf_pedigree.study import find_studies_from_study

logger = get_logger(__name__, testing_mode=False)


def gender(x):
    if isinstance(x, str):
        if x.lower() == "male":
            return "1"
        elif x.lower() == "female":
            return "2"
        else:
            return "0"
    else:
        return "0"


def case_control(row):
    if row["is_proband"] or row["affected_status"]:
        return "2"
    elif row["is_proband"] is False:
        return "1"
    else:
        return "0"


def parent(x):
    if x != x:
        return "0"
    else:
        return x


def build_report(
    participants,
    family_relationships,
    use_external_ids=False,
    api_or_db_url=None,
):
    logger.info("Building Pedigree Report")
    family = participants[["kf_id", "family_id"]]
    fr = (
        family_relationships.merge(
            family, left_on="participant1", right_on="kf_id", how="left"
        )
        .drop_duplicates()
        .drop(columns=["kf_id"])
    )
    fr["participant1_to_participant2_relation"] = fr[
        "participant1_to_participant2_relation"
    ].str.lower()
    fr["participant2_to_participant1_relation"] = fr[
        "participant2_to_participant1_relation"
    ].str.lower()
    pedi_1_2 = (
        fr[
            fr["participant1_to_participant2_relation"]
            .str.lower()
            .isin(["mother", "father"])
        ]
        .drop(columns=["participant2_to_participant1_relation"])
        .set_index(
            [
                "family_id",
                "participant2",
                "participant1_to_participant2_relation",
            ]
        )["participant1"]
        .unstack()
        .reset_index()
        .rename(columns={"participant2": "pt_id"})
    )
    pedi_2_1 = (
        fr[
            fr["participant2_to_participant1_relation"]
            .str.lower()
            .isin(["child"])
        ][["participant1", "family_id"]]
        .drop_duplicates()
        .rename(columns={"participant1": "pt_id"})
    )
    participant_info = participants[
        ["kf_id", "affected_status", "is_proband", "gender"]
    ].rename(columns={"kf_id": "pt_id"})
    pedigree = (
        pd.concat([pedi_1_2, pedi_2_1])
        .merge(participant_info, on="pt_id", how="left")
        .sort_values("family_id")
    )
    pedigree["gender"] = pedigree["gender"].apply(gender)
    pedigree["father"] = pedigree["father"].apply(parent)
    pedigree["mother"] = pedigree["mother"].apply(parent)
    pedigree["pheno_status"] = pedigree.apply(case_control, axis=1)
    pedigree = pedigree.drop(columns=["affected_status", "is_proband"])
    if use_external_ids:
        family_df = find_family_from_family_list(
            api_or_db_url, pedigree["family_id"].drop_duplicates().to_list()
        )
        external_participant_ids = participants[["kf_id", "external_id"]]

        pedigree = (
            pedigree.merge(family_df, left_on="family_id", right_on="kf_id")
            .drop(columns=["family_id", "kf_id", "visible"])
            .rename(columns={"external_id": "family_id"})
            .merge(
                external_participant_ids,
                left_on="pt_id",
                right_on="kf_id",
                how="left",
            )
            .drop(columns=["pt_id", "kf_id"])
            .rename(columns={"external_id": "pt_id"})
            .merge(
                external_participant_ids,
                left_on="father",
                right_on="kf_id",
                how="left",
            )
            .drop(columns=["father", "kf_id"])
            .rename(columns={"external_id": "father"})
            .merge(
                external_participant_ids,
                left_on="mother",
                right_on="kf_id",
                how="left",
            )
            .drop(columns=["mother", "kf_id"])
            .rename(columns={"external_id": "mother"})
            .reindex(
                columns=[
                    "family_id",
                    "pt_id",
                    "father",
                    "mother",
                    "gender",
                    "pheno_status",
                ]
            )
        )
    return pedigree


def build_metadata_report(
    participants,
    api_or_db_url=None,
):
    # Get the study info
    logger.info("Generating metadata report")
    studies = find_studies_from_study(
        api_or_db_url, participants["study_id"].drop_duplicates().to_list()
    )
    meta = participants.merge(studies, on="study_id", how="left")
    return meta


def save_report(df, output_file, index=False, header=True):
    delimiters = {"tsv": "\t", "csv": ",", "txt": "\t"}
    delim = delimiters[output_file.rsplit(".", 1)[-1].lower()]
    logger.info(f"saving report to {output_file}")
    df.to_csv(output_file, sep=delim, index=index, header=header)
