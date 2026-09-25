"""Testes de caracterização das tasks do rj_rmi__run_dbt.

Rodam sem rede, sem dbt e sem servidor do Prefect: chamam o ``.fn`` de cada
task e trocam o log, o clone e o runner do dbt por registros.
"""

import base64
import json
import os
import re
import stat
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace

import git
import pytest
from dbt.artifacts.schemas import results as dbt_results
from dbt.cli.main import dbtRunnerResult

from pipelines.rj_rmi__run_dbt import tasks

SERVICE_ACCOUNT = {"type": "service_account", "client_email": "sa@rmi.iam"}
REPOSITORY_URL = "https://token@github.com/prefeitura-rio/queries-rj-rmi.git"


def exactly(message: str) -> str:
    """Devolve o padrão do ``pytest.raises`` que casa só com ``message``.

    :param message: Mensagem esperada, sem escapar.
    :returns: A mensagem escapada, com âncoras no começo e no fim.
    """
    return f"^{re.escape(message)}$"


def credentials_file() -> Path:
    """Devolve o arquivo para onde o ADC do Google aponta.

    :returns: O caminho em ``GOOGLE_APPLICATION_CREDENTIALS``.
    """
    return Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])


def node_result(unique_id: str, status: str) -> SimpleNamespace:
    """Imita o resultado de um nó com ``.node``, como os do ``build``.

    :param unique_id: ``unique_id`` do nó.
    :param status: Status do resultado, como ``RunStatus.Error``.
    :returns: Um objeto com ``status`` e ``node.unique_id``.
    """
    node = SimpleNamespace(unique_id=unique_id)
    return SimpleNamespace(status=status, node=node)


def run_dbt(
    command: str = "build",
    select: str = "",
    flag: str = "",
    target: str = "prod",
) -> None:
    """Chama a task com o clone em ``/clone``.

    :param command: Comando dbt.
    :param select: Valor do ``--select``.
    :param flag: Demais argumentos, separados como no shell.
    :param target: Target do ``profiles.yml``.
    """
    tasks.run_dbt_task.fn(
        project_dir="/clone",
        command=command,
        select=select,
        flag=flag,
        target=target,
    )


@pytest.fixture(autouse=True)
def isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Isola o ambiente e a pasta temporária de cada teste.

    :param monkeypatch: Fixture do pytest que desfaz as trocas no fim.
    :param tmp_path: Pasta do teste, usada como pasta temporária.
    :returns: Um gerador que roda o teste e depois restaura o ambiente.
    """
    environment = os.environ.copy()
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    yield
    os.environ.clear()
    os.environ.update(environment)


@pytest.fixture
def logs(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Troca o log do Prefect por uma lista de mensagens.

    :param monkeypatch: Fixture do pytest que desfaz as trocas no fim.
    :returns: A lista, que recebe cada mensagem logada.
    """
    messages: list[str] = []
    monkeypatch.setattr(tasks, "log", messages.append)
    return messages


@pytest.fixture
def clones(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str, dict]]:
    """Troca o clone do GitPython por um registro das chamadas.

    :param monkeypatch: Fixture do pytest que desfaz as trocas no fim.
    :returns: As chamadas, com a URL, a pasta e as opções de cada uma.
    """
    calls: list[tuple[str, str, dict]] = []

    def clone_from(url: str, to_path: str, **options: object) -> object:
        """Registra a chamada e devolve um repo no commit ``0123456``.

        :param url: URL do clone, com o token.
        :param to_path: Pasta do clone.
        :param options: Demais argumentos do ``clone_from``.
        :returns: Um objeto com ``head.commit.hexsha``.
        """
        calls.append((url, to_path, options))
        commit = SimpleNamespace(hexsha="0123456789abcdef")
        return SimpleNamespace(head=SimpleNamespace(commit=commit))

    monkeypatch.setattr(git.Repo, "clone_from", staticmethod(clone_from))
    return calls


@pytest.fixture
def dbt(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Troca o PrefectDbtRunner por um que registra as chamadas.

    O ``deps`` sempre dá certo, e o comando devolve ``outcome``.

    :param monkeypatch: Fixture do pytest que desfaz as trocas no fim.
    :returns: O registro, com ``options``, ``environment``, ``invocations``
        e ``outcome``.
    """
    record = SimpleNamespace(
        options=None,
        environment=None,
        invocations=[],
        outcome=dbtRunnerResult(success=True),
    )

    class Runner:
        """Imita o PrefectDbtRunner."""

        def __init__(self, **options: object) -> None:
            """Guarda as opções e o ambiente do momento da criação.

            :param options: Opções passadas ao PrefectDbtRunner.
            """
            record.options = options
            record.environment = os.environ.copy()

        def invoke(self, args: list[str]) -> dbtRunnerResult:
            """Registra os argumentos e devolve o resultado do comando.

            :param args: Argumentos do comando dbt.
            :returns: Sucesso para o ``deps`` e ``outcome`` para o resto.
            """
            record.invocations.append(args)
            if args == ["deps"]:
                return dbtRunnerResult(success=True)
            return record.outcome

    monkeypatch.setattr(tasks, "PrefectDbtRunner", Runner)
    return record


def test_json_key_is_written_to_a_private_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, logs: list[str]
) -> None:
    """Confere que a chave em JSON vai para um arquivo com permissão 600."""
    key = json.dumps(SERVICE_ACCOUNT)
    monkeypatch.setenv("RJ_RMI_SA", f"\n  {key}  \n")

    tasks.setup_credentials_task.fn()

    path = credentials_file()
    assert path.parent == tmp_path
    assert path.name.startswith("rj-rmi-sa-")
    assert path.suffix == ".json"
    assert path.read_text(encoding="utf-8") == key
    assert stat.S_IMODE(path.stat().st_mode) == stat.S_IRUSR | stat.S_IWUSR
    assert logs == ["Credencial GCP: sa@rmi.iam"]


@pytest.mark.parametrize(
    "encode",
    [base64.b64encode, base64.encodebytes],
    ids=["base64", "base64 em linhas"],
)
def test_base64_key_is_decoded(
    monkeypatch: pytest.MonkeyPatch,
    logs: list[str],
    encode: Callable[[bytes], bytes],
) -> None:
    """Confere que a chave em base64 é decodificada, com ou sem quebras."""
    key = json.dumps(SERVICE_ACCOUNT)
    monkeypatch.setenv("RJ_RMI_SA", encode(key.encode()).decode())

    tasks.setup_credentials_task.fn()

    assert credentials_file().read_text(encoding="utf-8") == key
    assert logs == ["Credencial GCP: sa@rmi.iam"]


def test_user_credentials_log_a_placeholder(
    monkeypatch: pytest.MonkeyPatch, logs: list[str]
) -> None:
    """Confere que um login de usuário do gcloud loga ``ADC de usuário``."""
    user = {"type": "authorized_user", "client_id": "id"}
    monkeypatch.setenv("RJ_RMI_SA", json.dumps(user))

    tasks.setup_credentials_task.fn()

    assert logs == ["Credencial GCP: ADC de usuário"]


@pytest.mark.parametrize(
    ("value", "listed"),
    [
        (None, ["DBT_QUERIES__RJ_RMI_SA"]),
        ("", ["DBT_QUERIES__RJ_RMI_SA", "RJ_RMI_SA"]),
        ("  \n", ["DBT_QUERIES__RJ_RMI_SA", "RJ_RMI_SA"]),
    ],
    ids=["ausente", "vazia", "em branco"],
)
def test_missing_key_lists_the_variables_named_rmi(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    value: str | None,
    listed: list[str],
) -> None:
    """Confere que, sem a chave, a task lista as variáveis com RMI no nome.

    E que ela não grava arquivo nem aponta o ADC.
    """
    for name in list(os.environ):
        if "RMI" in name.upper():
            monkeypatch.delenv(name)
    monkeypatch.setenv("DBT_QUERIES__RJ_RMI_SA", "outro nome")
    monkeypatch.setenv("PERMISSIONS", "RMI só no meio da palavra")
    if value is not None:
        monkeypatch.setenv("RJ_RMI_SA", value)
    message = f"RJ_RMI_SA ausente ou vazia. Variáveis com RMI no nome: {listed}"

    with pytest.raises(ValueError, match=exactly(message)):
        tasks.setup_credentials_task.fn()

    assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ
    assert list(tmp_path.iterdir()) == []


def test_invalid_key_fails_after_pointing_the_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confere que uma chave que não é JSON só falha depois de gravada.

    Fixa o comportamento atual. Validar a chave antes de gravar muda este
    teste.
    """
    monkeypatch.setenv("RJ_RMI_SA", base64.b64encode(b"not json").decode())

    with pytest.raises(json.JSONDecodeError):
        tasks.setup_credentials_task.fn()

    assert credentials_file().read_text(encoding="utf-8") == "not json"


def test_clone_uses_the_token_and_returns_the_folder(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    logs: list[str],
    clones: list[tuple[str, str, dict]],
) -> None:
    """Confere a URL, o branch e a pasta do clone, e o commit no log."""
    monkeypatch.setenv("GITHUB_TOKEN", "token")

    path = tasks.clone_repository_task.fn()

    options = {"depth": 1, "branch": "master"}
    assert clones == [(REPOSITORY_URL, path, options)]
    assert Path(path).parent == tmp_path
    assert Path(path).name.startswith("queries-rj-rmi-")
    assert logs == [
        "github.com/prefeitura-rio/queries-rj-rmi.git clonado no commit 0123456"
    ]


def test_clone_without_token_fails_before_creating_the_folder(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    clones: list[tuple[str, str, dict]],
) -> None:
    """Confere que, sem o token, a task falha antes de criar a pasta."""
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    message = "Environment variable 'GITHUB_TOKEN' not found."

    with pytest.raises(ValueError, match=exactly(message)):
        tasks.clone_repository_task.fn()

    assert clones == []
    assert list(tmp_path.iterdir()) == []


def test_run_dbt_installs_packages_then_runs_the_command(
    dbt: SimpleNamespace,
) -> None:
    """Confere que o ``deps`` roda antes do comando, com ``--target``."""
    run_dbt(
        command="source freshness",
        select="tag:daily",
        flag="--vars '{a: 1}' --threads 4",
    )

    assert dbt.options == {"raise_on_failure": False}
    assert dbt.invocations == [
        ["deps"],
        [
            "source",
            "freshness",
            "--target",
            "prod",
            "--vars",
            "{a: 1}",
            "--threads",
            "4",
            "--select",
            "tag:daily",
        ],
    ]


def test_run_dbt_without_select_omits_the_option(dbt: SimpleNamespace) -> None:
    """Confere que ``select`` vazio não passa ``--select``."""
    run_dbt(command="debug", target="dev")

    assert dbt.invocations == [["deps"], ["debug", "--target", "dev"]]


def test_run_dbt_installs_packages_before_parsing_the_flags(
    dbt: SimpleNamespace,
) -> None:
    """Confere que um ``flag`` mal formado só falha depois do ``deps``."""
    with pytest.raises(ValueError, match="No closing quotation"):
        run_dbt(flag="--vars '{a: 1}")

    assert dbt.invocations == [["deps"]]


def test_run_dbt_replaces_the_inherited_dbt_variables(
    monkeypatch: pytest.MonkeyPatch, dbt: SimpleNamespace
) -> None:
    """Confere que só o ``DBT_USER`` sobra dos ``DBT_*`` herdados.

    O runner já nasce com os caminhos do clone.
    """
    monkeypatch.setenv("DBT_PROFILES_DIR", "/secret/profiles")
    monkeypatch.setenv("DBT_ENGINE_PROFILES_DIR", "/secret/engine")
    monkeypatch.setenv("DBT_TARGET_PATH", "/secret/target")
    monkeypatch.setenv("DBT_USER", "prefixo")
    monkeypatch.setenv("OTHER_VARIABLE", "kept")

    run_dbt()

    seen = {
        name: value
        for name, value in dbt.environment.items()
        if name.startswith("DBT_")
    }
    assert seen == {
        "DBT_PROJECT_DIR": "/clone",
        "DBT_PROFILES_DIR": "/clone",
        "DBT_USER": "prefixo",
    }
    assert dbt.environment["OTHER_VARIABLE"] == "kept"


def test_run_dbt_failure_lists_the_failed_nodes(dbt: SimpleNamespace) -> None:
    """Confere que a falha lista só os nós com erro ou falha, na ordem."""
    results = [
        node_result("model.rmi.ok", dbt_results.RunStatus.Success),
        node_result("model.rmi.broken", dbt_results.RunStatus.Error),
        node_result("test.rmi.failing", dbt_results.TestStatus.Fail),
        node_result("test.rmi.warning", dbt_results.TestStatus.Warn),
        node_result("source.rmi.stale", dbt_results.FreshnessStatus.RuntimeErr),
        # run-operation devolve RunResultOutput, com unique_id e sem .node.
        SimpleNamespace(
            status=dbt_results.RunStatus.Error, unique_id="macro.rmi.op"
        ),
    ]
    dbt.outcome = dbtRunnerResult(
        success=False, result=SimpleNamespace(results=results)
    )
    message = (
        "dbt build terminou com erro: model.rmi.broken, test.rmi.failing, "
        "source.rmi.stale, macro.rmi.op"
    )

    with pytest.raises(RuntimeError, match=exactly(message)):
        run_dbt()


@pytest.mark.parametrize(
    "result",
    [
        False,
        None,
        SimpleNamespace(
            results=[node_result("test.rmi.w", dbt_results.TestStatus.Warn)]
        ),
    ],
    ids=["debug", "sem resultado", "só warn"],
)
def test_run_dbt_failure_without_failed_nodes_points_to_the_log(
    dbt: SimpleNamespace, result: object
) -> None:
    """Confere que a falha sem nó com erro manda ver o log."""
    dbt.outcome = dbtRunnerResult(success=False, result=result)
    message = "dbt debug terminou com erro: ver o log"

    with pytest.raises(RuntimeError, match=exactly(message)):
        run_dbt(command="debug")
