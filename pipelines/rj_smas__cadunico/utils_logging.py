# -*- coding: utf-8 -*-
# ruff: noqa
"""
Utilidades de logging otimizadas para a pipeline CadÚnico.
Fornece logs estruturados, consolidados e informativos.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from iplanrio.pipelines_utils.logging import log


class PipelineLogger:
    """Logger estruturado para pipeline CadÚnico com fases e métricas"""

    def __init__(self, pipeline_name: str = "CadÚnico"):
        self.pipeline_name = pipeline_name
        self.start_time = datetime.now()
        self.current_phase = None
        self.phase_start_time = None

    def start_phase(self, phase_name: str, details: Optional[Dict[str, Any]] = None):
        """Inicia uma nova fase da pipeline"""
        self.current_phase = phase_name
        self.phase_start_time = datetime.now()
        elapsed = self._get_elapsed_time()

        message = f"🚀 [{elapsed}] FASE: {phase_name}"
        if details:
            details_str = self._format_details(details)
            message += f" | {details_str}"

        log(message, level="info")

    def log_progress(self, action: str, metrics: Optional[Dict[str, Any]] = None):
        """Log de progresso dentro da fase atual"""
        elapsed = self._get_elapsed_time()
        phase_elapsed = self._get_phase_elapsed_time()

        message = f"   ⏳ [{elapsed}] (+{phase_elapsed}) {action}"

        if metrics:
            metrics_str = self._format_details(metrics)
            message += f" | {metrics_str}"

        log(message, level="info")

    def complete_phase(self, success: bool = True, summary: Optional[Dict[str, Any]] = None):
        """Completa a fase atual"""
        elapsed = self._get_elapsed_time()
        phase_elapsed = self._get_phase_elapsed_time()
        status = "✅ CONCLUÍDA" if success else "❌ FALHOU"

        message = f"   {status} [{elapsed}] (+{phase_elapsed}) {self.current_phase}"
        if summary:
            summary_str = self._format_details(summary)
            message += f" | {summary_str}"

        log(message, level="info" if success else "error")

    def log_warning_summary(self, title: str, warnings: List[str], max_display: int = None):
        """Log consolidado de warnings - mostra TODOS os warnings sem omissão"""
        total = len(warnings)
        if total == 0:
            log(f"✅ {title}: Nenhum problema encontrado", level="info")
            return

        log(f"⚠️  {title}: {total} problema(s) encontrado(s)", level="warning")

        # Mostrar TODOS os warnings (sem limitação)
        warning_msg = ""
        for i, warning in enumerate(warnings):
            warning_msg += f"   {i + 1}. {warning}\n"
        log(msg=warning_msg, level="warning")

    def log_validation_summary(self, validation_results: Dict[str, List[str]]):
        """Log consolidado de validação de versões"""
        total_issues = sum(len(issues) for issues in validation_results.values())
        affected_tables = len([table for table, issues in validation_results.items() if issues])

        if total_issues == 0:
            log(
                "✅ VALIDAÇÃO DE VERSÕES: Todas as colunas consistentes entre versões",
                level="info",
            )
            return

        log(
            f"⚠️  VALIDAÇÃO DE VERSÕES: {total_issues} inconsistências em {affected_tables} tabela(s)",
            level="warning",
        )

        for table, missing_columns in validation_results.items():
            if missing_columns:
                display_columns = missing_columns[:3]
                remaining = len(missing_columns) - len(display_columns)

                columns_str = ", ".join(display_columns)
                if remaining > 0:
                    columns_str += f" (+{remaining} outras)"

                log(f"   📋 Tabela {table}: {columns_str}", level="warning")

    def log_file_processing_summary(self, processed_files: List[Dict[str, Any]]):
        """Log consolidado de processamento de arquivos"""
        total_files = len(processed_files)
        total_size_mb = sum(f.get("size_mb", 0) for f in processed_files)
        successful = len([f for f in processed_files if f.get("success", False)])

        log(
            f"📊 PROCESSAMENTO DE ARQUIVOS: {successful}/{total_files} processados com sucesso | {total_size_mb:.1f} MB total",
            level="info",
        )

        # Log consolidado de arquivos com falha
        failed_files = [f for f in processed_files if not f.get("success", True)]
        if failed_files:
            failed_msg = f"❌ ARQUIVOS COM FALHA ({len(failed_files)}): \n"
            for f in failed_files[:3]:  # Mostrar até 3 falhas
                failed_msg += f"   • {f.get('name', 'Unknown')}: {f.get('error', 'Erro desconhecido')}\n"
            log(failed_msg, level="error")

    def log_model_generation_summary(self, created_models: List[str], tables_dict: Dict[str, str]):
        """Log consolidado de geração de modelos DBT"""
        total_models = len(created_models)

        if total_models == 0:
            log("⚠️  GERAÇÃO DE MODELOS: Nenhum modelo foi criado", level="warning")
            return

        log(
            f"✅ GERAÇÃO DE MODELOS: {total_models} modelo(s) DBT criado(s)",
            level="info",
        )

        # Agrupar por tipo de tabela
        model_types = {}
        for model_path in created_models:
            # Extrair nome da tabela do caminho do modelo
            for table_name in tables_dict.values():
                if table_name in model_path:
                    model_types[table_name] = model_types.get(table_name, 0) + 1
                    break

        # Log consolidado dos tipos criados
        if model_types:
            types_msg = ""
            for table_name, count in model_types.items():
                types_msg += f"   📄 {table_name}: {count} arquivo(s)\n"
            log(types_msg, level="info")

    def log_comparison_table(self, title: str, comparisons: Dict[str, Dict[str, bool]]):
        """Log de tabela de comparação consolidado (ex: raw vs staging)"""
        comparison_msg = f"📊 {title}:\n"

        for item, status in comparisons.items():
            status_icons = []
            actions = []

            for location, exists in status.items():
                icon = "✅" if exists else "❌"
                status_icons.append(f"{location}={icon}")

            # Determinar ação baseada no status
            if status.get("raw", False) and not status.get("staging", False):
                actions.append("→ SERÁ PROCESSADO")
            elif status.get("staging", False) and not status.get("raw", False):
                actions.append("→ SÓ EM STAGING")
            elif status.get("staging", False) and status.get("raw", False):
                actions.append("→ JÁ PROCESSADO")
            else:
                actions.append("→ INCONSISTENTE")

            action_str = " ".join(actions)
            status_str = " | ".join(status_icons)

            comparison_msg += f"   {item}: {status_str} {action_str}\n"

        log(comparison_msg, level="info")

    def _format_details(self, details: Dict[str, Any]) -> str:
        """Formata dicionário para exibição em logs"""
        formatted = []
        for key, value in details.items():
            if isinstance(value, (int, float)):
                formatted.append(f"{key}: {value}")
            elif isinstance(value, str):
                # Truncar strings longas
                display_value = value[:50] + "..." if len(value) > 50 else value
                formatted.append(f"{key}: {display_value}")
            else:
                formatted.append(f"{key}: {value!s}")

        return " | ".join(formatted)

    def _get_elapsed_time(self) -> str:
        """Tempo decorrido desde o início da pipeline"""
        delta = datetime.now() - self.start_time
        return str(delta).split(".")[0]

    def _get_phase_elapsed_time(self) -> str:
        """Tempo decorrido da fase atual"""
        if self.phase_start_time:
            delta = datetime.now() - self.phase_start_time
            return str(delta).split(".")[0]
        return "00:00:00"


def log_version_comparison(raw_versions: List[str], staging_versions: List[str]):
    """Log consolidado de comparação de versões entre raw e staging"""
    logger = PipelineLogger()

    all_versions = sorted(set(raw_versions + staging_versions))
    comparisons = {}

    for version in all_versions:
        comparisons[version] = {
            "staging": version in staging_versions,
            "raw": version in raw_versions,
        }

    logger.log_comparison_table("COMPARAÇÃO DE VERSÕES (staging vs raw)", comparisons)

    # Resumo estatístico
    to_process = len([v for v in all_versions if v in raw_versions and v not in staging_versions])
    already_processed = len([v for v in all_versions if v in staging_versions])

    log(
        f"📈 RESUMO: {len(raw_versions)} versões no raw | {already_processed} no staging | {to_process} para processar",
        level="info",
    )


def log_dbt_execution_result(command_result, command_args: List[str]):
    """Log consolidado de resultado de execução DBT"""
    command_str = " ".join(command_args)

    if hasattr(command_result, "success") and command_result.success:
        log(f"✅ DBT EXECUTADO COM SUCESSO: {command_str}", level="info")

        # Extrair métricas se disponível
        if hasattr(command_result, "result") and command_result.result:
            result_str = str(command_result.result)
            if "models" in result_str.lower():
                log(f"   📊 Resultado: {result_str[:200]}...", level="info")
    else:
        log(f"❌ FALHA NA EXECUÇÃO DBT: {command_str}", level="error")

        # Log do erro se disponível
        if hasattr(command_result, "exception") and command_result.exception:
            error_msg = str(command_result.exception)[:300]
            log(f"   🔴 Erro: {error_msg}...", level="error")


def log_git_operation_result(operation: str, success: bool, details: Optional[Dict[str, Any]] = None):
    """Log consolidado de operações Git"""
    status = "✅ SUCESSO" if success else "❌ FALHA"

    message = f"{status}: {operation}"
    if details:
        details_str = " | ".join([f"{k}: {v}" for k, v in details.items()])
        message += f" | {details_str}"

    log(message, level="info" if success else "error")


class FileProcessingLogger:
    """Logger especializado para processamento de arquivos do dump"""

    def __init__(self, file_id: str, file_name: str):
        self.file_id = file_id
        self.file_name = file_name
        self.start_time = datetime.now()
        self.current_step = None
        self.step_start_time = None
        self.metrics = {}

    def start_processing(self, file_size_mb: float):
        """Inicia o processamento do arquivo"""
        elapsed = self._get_elapsed_time()
        log(
            f"🚀 [{elapsed}] [{self.file_id}] INICIANDO: {self.file_name} ({file_size_mb:.1f} MB)",
            level="info",
        )

    def start_step(self, step_name: str, step_number: int, total_steps: int):
        """Inicia uma etapa do processamento"""
        self.current_step = step_name
        self.step_start_time = datetime.now()
        elapsed = self._get_elapsed_time()

        log(
            f"   ⏳ [{elapsed}] [{self.file_id}] ETAPA {step_number}/{total_steps}: {step_name}",
            level="info",
        )

    def complete_step(self, success: bool = True, metrics: Optional[Dict[str, Any]] = None):
        """Completa a etapa atual"""
        if not self.current_step:
            return

        elapsed = self._get_elapsed_time()
        step_elapsed = self._get_step_elapsed_time()
        status = "✅" if success else "❌"

        message = f"   {status} [{elapsed}] (+{step_elapsed}) {self.current_step}"
        if metrics:
            self.metrics.update(metrics)
            metrics_str = self._format_metrics(metrics)
            message += f" | {metrics_str}"

        log(message, level="info" if success else "error")

    def complete_processing(self, success: bool = True, final_metrics: Optional[Dict[str, Any]] = None):
        """Completa o processamento do arquivo"""
        elapsed = self._get_elapsed_time()
        status = "✅ CONCLUÍDO" if success else "❌ FALHOU"

        if final_metrics:
            self.metrics.update(final_metrics)

        message = f"{status} [{elapsed}] [{self.file_id}] {self.file_name}"
        if self.metrics:
            metrics_str = self._format_metrics(self.metrics)
            message += f" | {metrics_str}"

        log(message, level="info" if success else "error")

    def log_file_analysis(self, txt_files_count: int, txt_files_info: List[Dict[str, Any]]):
        """Log consolidado de análise de arquivos TXT"""
        elapsed = self._get_elapsed_time()

        log(
            f"   📊 [{elapsed}] [{self.file_id}] ANÁLISE: {txt_files_count} arquivo(s) TXT encontrado(s)",
            level="info",
        )

        # Log resumido dos arquivos grandes
        large_files = [f for f in txt_files_info if f.get("size_gb", 0) > 1.0]
        if large_files:
            log(
                f"      🔍 Arquivos grandes (>1GB): {len(large_files)} | Maior: {max(f.get('size_gb', 0) for f in large_files):.2f} GB",
                level="info",
            )

    def _format_metrics(self, metrics: Dict[str, Any]) -> str:
        """Formata métricas para exibição"""
        formatted = []
        for key, value in metrics.items():
            if isinstance(value, float):
                formatted.append(f"{key}: {value:.1f}")
            else:
                formatted.append(f"{key}: {value}")
        return " | ".join(formatted)

    def _get_elapsed_time(self) -> str:
        """Tempo decorrido desde o início do processamento"""
        delta = datetime.now() - self.start_time
        return str(delta).split(".")[0]

    def _get_step_elapsed_time(self) -> str:
        """Tempo decorrido da etapa atual"""
        if self.step_start_time:
            delta = datetime.now() - self.step_start_time
            return str(delta).split(".")[0]
        return "00:00:00"


def log_partition_comparison(staging_partitions: List[str], raw_partitions: List[str], files_to_ingest: List[str]):
    """Log consolidado de comparação de partições"""
    logger = PipelineLogger("Comparação Partições")

    total_raw = len(raw_partitions)
    total_staging = len(staging_partitions)
    to_ingest = len(files_to_ingest)

    logger.log_comparison_table(
        "PARTIÇÕES (raw vs staging)",
        {
            partition: {
                "raw": partition in raw_partitions,
                "staging": partition in staging_partitions,
            }
            for partition in sorted(set(raw_partitions + staging_partitions))
        },
    )

    log(
        f"📈 RESUMO PARTIÇÕES: {total_raw} raw | {total_staging} staging | {to_ingest} para ingerir",
        level="info",
    )


def log_ingestion_summary(
    files_to_ingest: List[str],
    max_concurrent: int,
    processing_results: List[Dict[str, Any]],
):
    """Log consolidado de resultado da ingestão"""
    total_files = len(files_to_ingest)
    successful = len([r for r in processing_results if r.get("success", False)])
    total_size_mb = sum(r.get("size_mb", 0) for r in processing_results)
    avg_time_per_file = (
        sum(r.get("duration_seconds", 0) for r in processing_results) / total_files if total_files > 0 else 0
    )

    # Log principal
    log(
        f"✅ INGESTÃO COMPLETA: {successful}/{total_files} arquivos processados | {total_size_mb:.1f} MB total | Concorrência: {max_concurrent}",
        level="info",
    )
    log(
        f"   📊 Performance: {avg_time_per_file:.1f}s médio por arquivo | {total_size_mb / max(1, avg_time_per_file * total_files):.1f} MB/s throughput",
        level="info",
    )

    # Log consolidado de arquivos com falha se houver
    failed_results = [r for r in processing_results if not r.get("success", True)]
    if failed_results:
        failed_msg = f"❌ FALHAS ({len(failed_results)}): \n"
        for result in failed_results[:3]:  # Mostrar até 3 falhas
            failed_msg += f"   • {result.get('file_name', 'Unknown')}: {result.get('error', 'Erro desconhecido')}\n"
        log(failed_msg, level="error")

    # Estatísticas de partições criadas
    partitions_created = list(set(r.get("partition") for r in processing_results if r.get("partition") is not None))
    if partitions_created:
        # Filtrar valores None antes de ordenar
        valid_partitions = [p for p in partitions_created if p is not None]
        log(
            f"📅 PARTIÇÕES CRIADAS ({len(valid_partitions)}): {', '.join(sorted(valid_partitions))}",
            level="info",
        )
