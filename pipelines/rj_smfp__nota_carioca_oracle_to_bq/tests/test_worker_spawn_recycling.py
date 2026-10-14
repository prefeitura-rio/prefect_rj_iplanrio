# ruff: noqa: ARG005
"""Pool real, com processos ``spawn``: o reciclador do Python e o fechamento da conexão ao sair.

Fica num arquivo à parte, com importações leves, porque cada processo ``spawn`` importa este módulo.
"""

import importlib
import os
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import worker
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import OracleConfig

CONFIG = OracleConfig("u", "p", "h", "1521", "s", "DFEN")


class FakeConnection:
    """Conexão falsa que deixa um arquivo ao ser fechada, para o teste ver que o processo a fechou ao sair."""

    def __init__(self, marker_dir: Path) -> None:
        self.marker_dir = marker_dir

    def close(self) -> None:
        (self.marker_dir / f"closed-{os.getpid()}").touch()


def init_fake_worker(marker_dir: Path) -> None:
    (marker_dir / f"init-{os.getpid()}").touch()
    worker_module = importlib.import_module(worker.__name__)
    worker_module.connect = lambda config: FakeConnection(marker_dir)
    worker_module.init_worker(CONFIG, marker_dir)


def test_recycled_workers_rerun_the_initializer_and_close_their_connection_when_they_exit(tmp_path: Path) -> None:
    # Given a pool of one spawn worker that is replaced every 2 tasks, whose initializer runs the real init_worker
    with ProcessPoolExecutor(
        max_workers=1,
        mp_context=get_context("spawn"),
        initializer=init_fake_worker,
        initargs=(tmp_path,),
        max_tasks_per_child=2,
    ) as pool:
        # When 4 tasks run one after the other
        pids = [pool.submit(os.getpid).result(timeout=120) for _ in range(4)]
    # Then 2 processes served 2 tasks each, and each ran the initializer and closed its connection on exit
    assert sorted(Counter(pids).values()) == [2, 2]
    inits = {path.name.removeprefix("init-") for path in tmp_path.glob("init-*")}
    closed = {path.name.removeprefix("closed-") for path in tmp_path.glob("closed-*")}
    assert inits == {str(pid) for pid in pids}
    assert closed == inits
