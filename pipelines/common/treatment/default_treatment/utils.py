# -*- coding: utf-8 -*-
import inspect
import json
import os
import re
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Optional, Union
from zoneinfo import ZoneInfo

import pytz
import requests
import yaml
from prefect import runtime
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment import constants
from pipelines.common.utils.cron import cron_get_last_date, cron_get_next_date
from pipelines.common.utils.fs import get_project_root_path
from pipelines.common.utils.gcp.bigquery import SourceTable
from pipelines.common.utils.redis import get_redis_client
from pipelines.common.utils.utils import convert_timezone, is_running_locally


class DBTTest:
    """
    Classe para configurar testes agendados do DBT

    Args:
        test_name (str, optional): O nome do teste a ser executado
        dataset_id (str, optional): ID do conjunto de dados do modelo dbt
        table_id (str, optional): ID da tabela do modelo dbt
        model (str, optional): Modelo específico a ser testado
        exclude (str, optional): Recurso dbt para ser excluído do teste
        test_descriptions (dict, optional): Dicionário com nome e as descrições dos testes
        delay_days_start (int): Quantidade de dias que serão subtraídos do horário inicial
            da materialização
        delay_days_end (int): Quantidade de dias que serão subtraídos do horário final
            da materialização
        truncate_date (bool): Se True, trunca as horas para testar o dia completo
                             (00:00:00 a 23:59:59)
        additional_vars (dict, optional): Variáveis adicionais para passar ao DBT
    """

    def __init__(  # noqa: PLR0913
        self,
        test_select: str,
        exclude: Optional[str] = None,
        test_descriptions: Optional[dict] = None,
        delay_days_start: int = 0,
        delay_days_end: int = 0,
        truncate_date: bool = False,
        additional_vars: Optional[dict] = None,
    ):
        self.test_select = test_select
        self.exclude = exclude
        self.test_descriptions = test_descriptions or {}
        self.delay_days_start = delay_days_start
        self.delay_days_end = delay_days_end
        self.truncate_date = truncate_date
        self.additional_vars = additional_vars or {}

    def __getitem__(self, key):
        return self.__dict__[key]

    def get_test_vars(self, datetime_start: datetime, datetime_end: datetime) -> dict:
        """
        Retorna dict para teste

        Args:
            datetime_start (datetime): Datetime inicial
            datetime_end (datetime): Datetime final

        Returns:
            dict: dicionário com parâmetros para o teste do dbt
        """

        pattern = constants.MATERIALIZATION_LAST_RUN_PATTERN

        datetime_start, datetime_end = self.adjust_datetime_range(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
        )

        final_dict = {
            "date_range_start": datetime_start.strftime(pattern),
            "date_range_end": datetime_end.strftime(pattern),
        }

        collision = final_dict.keys() & self.additional_vars.keys()
        if collision:
            raise ValueError(f"Variáveis reservadas não podem ser sobrescritas: {collision}")

        return final_dict | self.additional_vars

    def adjust_datetime_range(
        self, datetime_start: datetime, datetime_end: datetime
    ) -> tuple[datetime, datetime]:
        """
        Ajusta o range de datetime

        Args:
            datetime_start (datetime): Datetime inicial
            datetime_end (datetime): Datetime final

        Returns:
            tuple[datetime, datetime]: (datetime_start, datetime_end) ajustados
        """

        adjusted_start = datetime_start
        adjusted_end = datetime_end

        adjusted_start = adjusted_start - timedelta(days=self.delay_days_start)
        adjusted_end = adjusted_end - timedelta(days=self.delay_days_end)

        if self.truncate_date:
            adjusted_start = adjusted_start.replace(hour=0, minute=0, second=0, microsecond=0)
            adjusted_end = adjusted_end.replace(hour=23, minute=59, second=59, microsecond=0)

        return adjusted_start, adjusted_end


class DBTSelector:
    """
    Classe que representa um selector do DBT

    Args:
        name (str): nome do seletor no DBT
        schedule_cron (str, optional): expressão cron que representa a frequência com que o seletor
            é executado
        initial_datetime (datetime): primeiro datetime que o selector deve ser executado
            (é usado na criação da primeira variável date_range_start)
        incremental_delay_hours (int): quantidade de horas que serão subtraídas do horário atual
            ao criar a variável date_range_end
        redis_key_suffix (str, optional): sufixo para diferenciar redis_keys de selectores
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        initial_datetime: datetime,
        final_datetime: Optional[datetime] = None,
        flow_folder_name: Optional[str] = None,
        incremental_delay_hours: int = 0,
        redis_key_suffix: Optional[str] = None,
        pre_test: Optional[DBTTest] = None,
        post_test: Optional[DBTTest] = None,
        data_sources: Optional[list[Union["DBTSelector", SourceTable, dict]]] = None,
    ):
        self.name = name
        self.flow_folder_name = flow_folder_name
        self.incremental_delay_hours = incremental_delay_hours
        self.initial_datetime = convert_timezone(initial_datetime)
        self.final_datetime = (
            final_datetime if final_datetime is None else convert_timezone(final_datetime)
        )
        self.redis_key_suffix = redis_key_suffix
        self.schedule_cron = self._get_schedule_cron()
        self.pre_test = pre_test
        self.post_test = post_test

        self.data_sources = data_sources or []

    def __getitem__(self, key):
        return self.__dict__[key]

    def _get_redis_key(self, env: str) -> str:
        """
        Gera a chave do Redis para o selector

        Args:
            env (str): prod ou dev

        Returns:
            str: chave do Redis
        """
        redis_key = f"{env}.selector_{self.name}"
        if self.redis_key_suffix:
            return f"{redis_key}_{self.redis_key_suffix}"
        return redis_key

    def _get_schedule_cron(self) -> str:
        if self.flow_folder_name is None:
            flow_folder_path = Path(inspect.stack()[2].filename).parent

            flow_name = flow_folder_path.name

        else:
            flow_name = self.flow_folder_name

            flow_folder_path = Path(constants.__file__).resolve().parent.parent / flow_name

        with (flow_folder_path / "prefect.yaml").open("r") as f:
            prefect_file = yaml.safe_load(f)

        schedules = next(
            d
            for d in prefect_file["deployments"]
            if d["name"] == f"rj-{flow_name.replace('__', '--', 1)}--prod"
        ).get("schedules", [{}])

        return schedules[0].get("cron")

    def get_last_materialized_datetime(self, env: str) -> Optional[datetime]:
        """
        Pega o último datetime materializado no Redis

        Args:
            env (str): prod ou dev

        Returns:
            datetime: a data vinda do Redis
        """
        redis_key = self._get_redis_key(env)
        redis_client = get_redis_client(host="localhost")
        content = redis_client.get(redis_key)
        if content is None:
            last_datetime = self.initial_datetime
        else:
            last_datetime = datetime.strptime(
                content[constants.REDIS_LAST_MATERIALIZATION_TS_KEY],
                constants.MATERIALIZATION_LAST_RUN_PATTERN,
            ).replace(tzinfo=ZoneInfo(smtr_constants.TIMEZONE))

        return convert_timezone(timestamp=last_datetime)

    def get_datetime_end(self, timestamp: datetime) -> datetime:
        """
        Calcula o datetime final da materialização com base em um timestamp

        Args:
            timestamp (datetime): datetime de referência

        Returns:
            datetime: datetime_end calculado
        """
        return timestamp - timedelta(hours=self.incremental_delay_hours)

    def is_up_to_date(self, env: str, timestamp: datetime) -> bool:
        """
        Confere se o selector está atualizado em relação a um timestamp

        Args:
            env (str): prod ou dev
            timestamp (datetime): datetime de referência

        Returns:
            bool: se está atualizado ou não
        """
        if self.schedule_cron is None:
            raise ValueError("O selector não possui agendamento")
        last_materialization = self.get_last_materialized_datetime(env=env)

        last_schedule = cron_get_last_date(cron_expr=self.schedule_cron, timestamp=timestamp)

        return last_materialization >= last_schedule - timedelta(hours=self.incremental_delay_hours)

    def get_next_schedule_datetime(self, timestamp: datetime) -> datetime:
        """
        Pega a próxima data de execução do selector em relação a um datetime
        com base no schedule_cron

        Args:
            timestamp (datetime): datetime de referência

        Returns:
            datetime: próximo datetime do cron
        """
        if self.schedule_cron is None:
            raise ValueError("O selector não possui agendamento")
        return cron_get_next_date(cron_expr=self.schedule_cron, timestamp=timestamp)

    def set_redis_materialized_datetime(self, env: str, timestamp: datetime):
        """
        Atualiza a timestamp de materialização no Redis

        Args:
            env (str): prod ou dev
            timestamp (datetime): data a ser salva no Redis
        """
        value = timestamp.strftime(constants.MATERIALIZATION_LAST_RUN_PATTERN)
        redis_key = self._get_redis_key(env)
        print(f"Salvando timestamp {value} na key: {redis_key}")
        redis_client = get_redis_client(host="localhost")
        content = redis_client.get(redis_key)
        if not content:
            content = {constants.REDIS_LAST_MATERIALIZATION_TS_KEY: value}
            redis_client.set(redis_key, content)
        elif (
            convert_timezone(
                datetime.strptime(
                    content[constants.REDIS_LAST_MATERIALIZATION_TS_KEY],
                    constants.MATERIALIZATION_LAST_RUN_PATTERN,
                ).replace(tzinfo=ZoneInfo(smtr_constants.TIMEZONE))
            )
            < timestamp
        ):
            content[constants.REDIS_LAST_MATERIALIZATION_TS_KEY] = value
            redis_client.set(redis_key, content)


class DBTSelectorMaterializationContext:
    def __init__(  # noqa: PLR0913
        self,
        env: str,
        selector: DBTSelector,
        timestamp: datetime,
        datetime_start: Optional[str],
        datetime_end: Optional[str],
        additional_vars: Optional[dict],
        test_scheduled_time: time,
        force_test_run: bool,
    ):
        """
        Objeto contendo as informações básicas para captura de dados.

        Args:
            source (SourceTable): SourceTable da captura.
            timestamp (datetime): Timestamp da captura.
        """
        self.env = env
        self.selector = selector
        self.timestamp = timestamp.astimezone(tz=pytz.timezone(smtr_constants.TIMEZONE))
        self.datetime_start = self.get_datetime_start(datetime_start=datetime_start)
        self.datetime_end = self.get_datetime_end(datetime_end=datetime_end)

        self.dbt_vars = self.get_dbt_vars(
            datetime_start=self.datetime_start,
            datetime_end=self.datetime_end,
            additional_vars=additional_vars,
        )

        self.should_run = (
            False
            if (
                selector.final_datetime is not None
                and self.datetime_start > selector.final_datetime
            )
            else True
        )

        is_test_scheduled_time = (
            force_test_run or test_scheduled_time is None or timestamp.time() == test_scheduled_time
        ) and self.should_run

        self.should_run_pre_test = selector.pre_test is not None and is_test_scheduled_time

        self.should_run_post_test = selector.post_test is not None and is_test_scheduled_time

        self.pre_test_dbt_vars = (
            selector.pre_test.get_test_vars(
                datetime_start=self.datetime_start,
                datetime_end=self.datetime_end,
            )
            if self.should_run_pre_test
            else None
        )

        self.post_test_dbt_vars = (
            selector.post_test.get_test_vars(
                datetime_start=self.datetime_start,
                datetime_end=self.datetime_end,
            )
            if self.should_run_post_test
            else None
        )

        self.pre_test_log = None
        self.post_test_log = None

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def get_datetime_start(
        self,
        datetime_start: Optional[str],
    ) -> Optional[datetime]:
        """
        Retorna o datetime de inicio da materialização

        Args:
            datetime_start (Optional[str]): Força um valor no datetime_start

        Returns:
            Optional[datetime]: datetime de inicio da materialização
        """
        if datetime_start is not None:
            datetime_start = datetime.fromisoformat(datetime_start)
        else:
            datetime_start = self.selector.get_last_materialized_datetime(env=self.env)

        datetime_start = convert_timezone(timestamp=datetime_start)

        if datetime_start < self.selector.initial_datetime:
            return self.selector.initial_datetime

        return datetime_start

    def get_datetime_end(
        self,
        datetime_end: Optional[str],
    ) -> datetime:
        """
        Retorna o datetime de fim da materialização

        Args:
            datetime_end (Optional[str]): Força um valor no datetime_end

        Returns:
            datetime: datetime de fim da materialização
        """
        if datetime_end is not None:
            datetime_end = datetime.fromisoformat(datetime_end)
        else:
            datetime_end = self.selector.get_datetime_end(timestamp=self.timestamp)

        datetime_end = convert_timezone(timestamp=datetime_end)

        if self.selector.final_datetime is not None and datetime_end > self.selector.final_datetime:
            return self.selector.final_datetime

        return datetime_end

    def get_repo_version(self) -> str:
        """
        Retorna o SHA do último commit do repositório no GITHUB

        Returns:
            str: SHA do último commit do repositório no GITHUB
        """
        response = requests.get(
            f"{constants.REPO_URL}/commits",
            timeout=60,
        )

        response.raise_for_status()

        return response.json()[0]["sha"]

    def get_dbt_vars(
        self,
        datetime_start: datetime,
        datetime_end: datetime,
        additional_vars: Optional[dict],
    ):
        """
        Cria a lista de variaveis para rodar o modelo DBT,
        unindo a versão do repositório com as variaveis de datetime

        Args:
            datetime_start (datetime): Datetime inicial da materialização parametrizado
            datetime_end (datetime): Datetime final da materialização parametrizado
            additional_vars (dict): Variáveis extras para executar o modelo DBT

        Returns:
            dict[str]: Variáveis para executar o modelo DBT
        """

        pattern = constants.MATERIALIZATION_LAST_RUN_PATTERN

        dbt_vars = {
            "date_range_start": datetime_start.strftime(pattern),
            "date_range_end": datetime_end.strftime(pattern),
            "version": self.get_repo_version(),
        }

        if additional_vars:
            dbt_vars.update(additional_vars)

        return dbt_vars


def run_dbt(
    dbt_obj: Optional[Union[DBTSelector, DBTTest]] = None,
    dbt_vars: Optional[dict] = None,
    flags: Optional[list[str]] = None,
    raise_on_failure=True,
):
    root_path = get_project_root_path()
    project_dir = root_path / "queries"
    flags = flags or []
    log_dir = f"{project_dir}/logs/{runtime.task_run.id}"

    flags = [*flags, "--log-path", log_dir, "--log-level-file", "info", "--log-format", "json"]
    if is_running_locally():
        profiles_dir = project_dir / "dev"
    else:
        profiles_dir = project_dir

    invoke = []
    if dbt_obj is not None:
        if isinstance(dbt_obj, DBTSelector):
            invoke = ["run", "--selector", dbt_obj.name]
        elif isinstance(dbt_obj, DBTTest):
            invoke = ["test", "--select", dbt_obj.test_select]

    if dbt_vars is not None:
        vars_yaml = yaml.safe_dump(dbt_vars, default_flow_style=True)
        invoke = [*invoke, "--vars", vars_yaml]

    invoke = invoke + flags
    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target_path=project_dir / "target",
        ),
        raise_on_failure=raise_on_failure,
    ).invoke(invoke)

    with (Path(log_dir) / "dbt.log").open("r") as logs:
        return logs.read()


def parse_dbt_test_output(dbt_logs: str) -> dict:
    """Parses DBT test output and returns a list of test results.

    Args:
        dbt_logs: The DBT test output as a string.

    Returns:
        A list of dictionaries, each representing a test result with the following keys:
        - name: The test name.
        - result: "PASS", "FAIL" or "ERROR".
        - query: Query to see test failures.
        - error: Message error.
    """

    log_lines = re.split(r"(?m)(?=^)", dbt_logs)

    results = {}
    root_path = get_project_root_path()
    queries_path = filepath = root_path / "queries"

    for line in log_lines:
        if line.strip() == "":
            continue
        log_line_json = json.loads(line)
        data = log_line_json["data"]

        node_info = data.get("node_info", {})
        if node_info.get("materialized", "") == "test":
            test_name = node_info["node_name"]
            status = data.get("status")
            if status is not None:
                results[test_name] = {"result": status.upper()}

            path = data.get("path")

            if (
                path is not None
                and "compiled code at" in log_line_json.get("info", {}).get("msg", "").lower()
            ):
                filepath = queries_path / Path(os.path.relpath(path, queries_path))
                filepath = filepath.resolve()
                with filepath.open("r") as f:
                    query = f.read()

                query = re.sub(r"\n+", "\n", query)
                results[test_name]["query"] = query

    log_message = ""
    for test, info in results.items():
        result = info["result"]
        log_message += f"Test: {test} Status: {result}\n"

        if result == "FAIL":
            log_message += "Query:\n"
            log_message += f"{info['query']}\n"

        if result == "ERROR":
            log_message += f"Error: {info['error']}\n"

        log_message += "\n"

    print(log_message)

    return results


class DBTTestFailedError(Exception): ...


class IncompleteDataError(Exception): ...
