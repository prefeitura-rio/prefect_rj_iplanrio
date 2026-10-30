"""Flow e deployments: contrato documentado, agenda inativa até o gate e estrutura igual à dos pacotes irmãos."""

from pathlib import Path

import yaml

from pipelines.rj_smfp__nota_carioca_parser import flow as modulo_flow
from pipelines.rj_smfp__nota_carioca_parser.constants import PIPELINE_DIR
from pipelines.rj_smfp__nota_carioca_parser.utils.tabelas import colunas


def test_flow_docstring_documenta_todas_as_colunas_dos_schemas():
    docstring = modulo_flow.__doc__
    for tabela in ("nfse", "dps", "pessoas", "eventos"):
        for coluna in colunas(tabela):
            assert coluna in docstring, (tabela, coluna)
    assert "quarentena(nsu, prioridade, erro, versao_parser, avaliado_em)" in docstring
    assert "parser_marca(versao_parser, ultimo_nsu)" in docstring


def test_flow_funcao_tem_o_nome_do_diretorio_do_pacote():
    assert modulo_flow.rj_smfp__nota_carioca_parser.fn.__name__ == PIPELINE_DIR.name


def deployments() -> list[dict]:
    return yaml.safe_load((PIPELINE_DIR / "prefect.yaml").read_text(encoding="utf-8"))["deployments"]


def test_flow_deployments_staging_e_prod_com_agenda_inativa_ate_o_gate():
    por_nome = {d["name"]: d for d in deployments()}
    assert set(por_nome) == {"rj-smfp--nota-carioca-parser--staging", "rj-smfp--nota-carioca-parser--prod"}
    for deployment in por_nome.values():
        assert deployment["entrypoint"].endswith("flow.py:rj_smfp__nota_carioca_parser")
        assert deployment["schedules"], "o gate liga a agenda: ela precisa existir, desativada"
        assert all(agenda["active"] is False for agenda in deployment["schedules"])
        assert deployment["concurrency_limit"]["limit"] == 1


def test_flow_staging_e_prod_usam_o_ambiente_e_o_dataset_da_convencao():
    por_nome = {d["name"]: d["parameters"] for d in deployments()}
    assert por_nome["rj-smfp--nota-carioca-parser--staging"] == {
        "dataset_id": "raw_nfse_nacional_staging",
        "ambiente": "staging",
    }
    assert por_nome["rj-smfp--nota-carioca-parser--prod"] == {"dataset_id": "raw_nfse_nacional", "ambiente": "prod"}


def test_flow_estrutura_do_pacote_igual_a_dos_irmaos():
    for arquivo in ("flow.py", "tasks.py", "constants.py", "prefect.yaml", "Dockerfile", "pyproject.toml"):
        assert (PIPELINE_DIR / arquivo).is_file(), arquivo
    assert (PIPELINE_DIR / "utils").is_dir() and (PIPELINE_DIR / "tests").is_dir()
    assert not list(Path(PIPELINE_DIR).glob("**/__init__.py"))
