from pathlib import Path

import pytest
import yaml

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import PROXY_URL, URL_PROD, URL_STAGING
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.config import (
    read_token,
    resolve_proxy,
    resolve_url,
    secret_env_key,
)
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.discord import Environment

PREFECT_YAML = Path(__file__).resolve().parents[1] / "prefect.yaml"


def test_secret_env_key_follows_the_naming_of_the_oracle_keys_in_the_infisical_folder() -> None:
    assert secret_env_key("/db-oracle-nota-fiscal", "DB_HOST") == "DB_ORACLE_NOTA_FISCAL__DB_HOST"


def test_read_token_reads_the_key_with_the_name_stored_in_infisical() -> None:
    # O InfisicalSecret do Prefect copia cada chave com o nome literal; o token foi gravado sem prefixo de pasta.
    environ = {"NOTA_CARIOCA_ROTINAS_TOKEN": "  abc  ", "DB_ORACLE_NOTA_FISCAL__NOTA_CARIOCA_ROTINAS_TOKEN": "outro"}

    assert read_token(environ) == "abc"


@pytest.mark.parametrize(
    "environ", [{}, {"NOTA_CARIOCA_ROTINAS_TOKEN": " "}, {"DB_ORACLE_NOTA_FISCAL__NOTA_CARIOCA_ROTINAS_TOKEN": "x"}]
)
def test_read_token_fails_explicitly_when_missing(environ: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="NOTA_CARIOCA_ROTINAS_TOKEN"):
        read_token(environ)


def test_resolve_url_uses_homologation_in_staging_and_production_in_prod() -> None:
    assert resolve_url(None, Environment.STAGING) == URL_STAGING
    assert resolve_url(None, Environment.PROD) == URL_PROD
    assert "notacariocahom.rio.gov.br" in URL_STAGING
    assert URL_PROD.startswith("https://notacarioca.rio.gov.br/intranet/")


def test_resolve_url_accepts_an_explicit_https_url() -> None:
    assert resolve_url("https://outro.rio.gov.br/x.asmx", Environment.PROD) == "https://outro.rio.gov.br/x.asmx"
    with pytest.raises(ValueError, match="https"):
        resolve_url("http://notacarioca.rio.gov.br/x.asmx", Environment.PROD)


def deployments() -> dict[str, dict[str, object]]:
    data = yaml.safe_load(PREFECT_YAML.read_text(encoding="utf-8"))
    return {item["name"]: item for item in data["deployments"]}


def test_prefect_yaml_has_one_deployment_per_environment_without_schedule() -> None:
    found = deployments()

    assert set(found) == {"rj-smfp--nota-carioca-extracao-xml--staging", "rj-smfp--nota-carioca-extracao-xml--prod"}
    for item in found.values():
        assert "schedules" not in item
        assert item["concurrency_limit"] == {"limit": 1, "collision_strategy": "CANCEL_NEW"}


def test_prefect_yaml_uses_the_secret_of_each_environment() -> None:
    found = deployments()

    staging = found["rj-smfp--nota-carioca-extracao-xml--staging"]["work_pool"]
    prod = found["rj-smfp--nota-carioca-extracao-xml--prod"]["work_pool"]
    assert staging["job_variables"]["secretName"] == "prefect-jobs-secrets-staging"
    assert prod["job_variables"]["secretName"] == "prefect-jobs-secrets"


def test_resolve_proxy_uses_the_cluster_squid_by_default() -> None:
    # Os pods do k3s levam reset ao chamar a intranet direto; pelo squid do cluster o webservice responde.
    assert resolve_proxy(None) == PROXY_URL == "http://proxy.proxy.svc.cluster.local:3128"


def test_resolve_proxy_can_be_disabled_or_replaced() -> None:
    assert resolve_proxy("") is None
    assert resolve_proxy("  ") is None
    assert resolve_proxy("http://outro:3128") == "http://outro:3128"
    with pytest.raises(ValueError, match="proxy"):
        resolve_proxy("socks5://x:1080")
