import pytest
from httpx2 import AsyncClient, MockTransport, Request, Response

from utils.secrets import SecretsError, build_environment, read_secrets
from utils.settings import InfisicalSettings

URL = "http://infisical.test"
JWT = "pod-token"
PROJECT = "project-id"
ENVIRONMENT = "staging"


def handler(folders: dict[str, dict[str, str]], login: int = 200, seen: list[str] | None = None):
    """Return the Infisical API: a login, and the secrets of each folder."""

    def handle(request: Request) -> Response:
        if request.url.path == "/api/v1/auth/kubernetes-auth/login":
            body = request.read().decode()
            ok = login == 200 and JWT in body
            return Response(login if ok else 401, json={"accessToken": "access"} if ok else {})

        if request.headers.get("authorization") != "Bearer access":
            return Response(401, json={})

        params = request.url.params
        path = params["secretPath"]

        if seen is not None:
            seen.append(path)

        assert params["workspaceId"] == PROJECT
        assert params["environment"] == ENVIRONMENT
        assert params["include_imports"] == "true"

        if path not in folders:
            return Response(404, json={})

        return Response(200, json={"secrets": [{"secretKey": k, "secretValue": v} for k, v in folders[path].items()]})

    return handle


async def run(folders, paths, login=200, seen=None):
    """Read the paths through the fake API."""
    client = AsyncClient(transport=MockTransport(handler(folders, login, seen)), base_url=URL)
    settings = InfisicalSettings(
        INFISICAL_URL=URL,
        INFISICAL_IDENTITY_ID="identity",
        INFISICAL_PROJECT_ID=PROJECT,
        INFISICAL_ENVIRONMENT=ENVIRONMENT,
        INFISICAL_PATHS=",".join(paths),
    )

    return await read_secrets(client, settings, JWT)


@pytest.mark.parametrize(
    ("folders", "paths", "expected"),
    [
        pytest.param({"/a": {"K": "1"}}, ["/a"], {"K": "1"}, id="one_folder"),
        pytest.param(
            {"/a": {"K": "a"}, "/shared/w": {"K": "w", "W": "w"}},
            ["/a", "/shared/w"],
            {"K": "a", "W": "w"},
            id="the_first_folder_wins_a_repeated_key",
        ),
        pytest.param({"/a": {}, "/gke": {"H": "h"}}, ["/a", "/gke"], {"H": "h"}, id="an_empty_folder_adds_nothing"),
    ],
)
async def test_the_folders_are_read_in_order_and_the_first_value_of_a_key_wins(folders, paths, expected) -> None:
    """Read each folder in order, and keep the first value of a key."""
    seen: list[str] = []

    assert await run(folders, paths, seen=seen) == expected
    assert seen == paths


@pytest.mark.parametrize(
    ("folders", "login"),
    [
        pytest.param({"/a": {"K": "1"}}, 401, id="the_login_is_refused"),
        pytest.param({}, 200, id="a_folder_is_missing"),
    ],
)
async def test_a_failed_login_or_a_missing_folder_stops_the_job(folders, login) -> None:
    """Fail, instead of starting the pipeline without its secrets."""
    with pytest.raises(SecretsError):
        await run(folders, ["/a"], login)


@pytest.mark.parametrize(
    ("current", "secrets", "expected"),
    [
        pytest.param({"A": "1"}, {"B": "2"}, {"A": "1", "B": "2"}, id="a_new_key_is_added"),
        pytest.param({"A": "1"}, {"A": "2"}, {"A": "1"}, id="a_variable_of_the_job_wins"),
        pytest.param({}, {}, {}, id="nothing_to_add"),
    ],
)
def test_a_secret_never_replaces_a_variable_that_the_job_has(current, secrets, expected) -> None:
    """Merge the secrets under the environment of the job."""
    assert build_environment(current, secrets) == expected
