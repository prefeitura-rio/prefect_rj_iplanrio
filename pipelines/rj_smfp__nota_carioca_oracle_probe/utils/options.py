"""Parâmetros validados da sonda."""

from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import validate_identifier
from pipelines.rj_smfp__nota_carioca_oracle_probe.constants import SUPPORTED_COMPRESSIONS


@dataclass(frozen=True)
class ProbeOptions:
    """Parâmetros do flow, já validados.

    :param run_id: Identificador do flow run; isola a tarefa do Oracle e os arquivos do GCS.
    :param schema: Dono das tabelas.
    :param tables: Tabelas a medir, em maiúsculas.
    :param infisical_secret_path: Pasta do segredo do Oracle no Infisical.
    :param chunk_size_blocks: Tamanho aproximado de cada faixa de ROWID, em blocos.
    :param sample_chunks: Faixas medidas por tabela.
    :param worker_counts: Números de workers do teste de escala.
    :param worker_memory_mb: Orçamento de memória de um worker, em MiB.
    :param batch_rows: Teto de linhas por lote lido do Oracle.
    :param test_upload: Se o Parquet é enviado ao GCS (e apagado) na medição.
    :param gcs_bucket: Bucket do teste de envio.
    :param project: Projeto do GCS.
    :param compare_no_scn: Se mede também a leitura sem ``AS OF SCN``.
    :param compression_variants: Compressões do Parquet; a primeira é a usada no caminho completo.
    :raises ValueError: Se algum parâmetro for inválido.
    """

    run_id: str
    schema: str
    tables: tuple[str, ...]
    infisical_secret_path: str
    chunk_size_blocks: int
    sample_chunks: int
    worker_counts: tuple[int, ...]
    worker_memory_mb: int
    batch_rows: int
    test_upload: bool
    gcs_bucket: str
    project: str
    compare_no_scn: bool
    compression_variants: tuple[str, ...]

    def __post_init__(self) -> None:
        """Valida identificadores, faixas numéricas e compressões."""
        validate_identifier(self.schema)
        if not self.tables:
            raise ValueError("table_ids não pode ser vazio.")
        for table in self.tables:
            validate_identifier(table)
        for name, value in (
            ("chunk_size_blocks", self.chunk_size_blocks),
            ("sample_chunks", self.sample_chunks),
            ("worker_memory_mb", self.worker_memory_mb),
            ("batch_rows", self.batch_rows),
        ):
            if value < 1:
                raise ValueError(f"{name} deve ser positivo, recebido {value}.")
        if not self.worker_counts or min(self.worker_counts) < 1:
            raise ValueError(f"worker_counts deve ter inteiros positivos, recebido {list(self.worker_counts)}.")
        unknown = set(self.compression_variants) - set(SUPPORTED_COMPRESSIONS)
        if not self.compression_variants or unknown:
            raise ValueError(
                f"compression_variants inválido {sorted(unknown)}; aceitos: {list(SUPPORTED_COMPRESSIONS)}."
            )

    @property
    def main_compression(self) -> str:
        """Retorna a compressão do caminho completo (a primeira variante)."""
        return self.compression_variants[0]
