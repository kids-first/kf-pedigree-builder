from d3b_utils.requests_retry import Session

from kf_pedigree.common import KF_API_URLS


class fhir_session(Session):
    """
    Requests Session object with BRP specific tooling
    :param Session: an existing Session to use (optional)
    :type Session: d3b_utils.requests_retry.Session
    :param token: FHIR authentication cookie
    :type token: str
    :param host: BRP api url
    :type host: str
    """

    def __init__(
        self,
        cookie,
        api_host=KF_API_URLS.get("kf_fhir_url"),
    ):
        super().__init__()
        self.headers["Cookie"] = f"AWSELBAuthSessionCookie-0={cookie}"
        self.api_host = api_host

    def get_metadata(self):
        metadata_url = f"{self.api_host}metadata"
        return self.get(metadata_url)


dataservice_fhir_map = [
    {
        "dataservice_pg": "study",
        "dataservice_api": "studies",
        "fhir": "ResearchStudy",
    },
    {
        "dataservice_pg": "participant",
        "dataservice_api": "participants",
        "fhir": "Patient",
    },
    {
        "dataservice_pg": "family_relationship",
        "dataservice_api": "family-relationships",
        "fhir": "Observation",
    },
]


# c = "insert cookie here"
# foo = fhir_session(cookie=c)
breakpoint()
