"""
BigQuery Loader Module

Loads expected NFs from BigQuery despesas_recorte table for compliance validation.

Required environment variables:
    BIGQUERY_DATASET_ID: BigQuery dataset ID containing the NF tables
"""

import os
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

from ..core.config import BIGQUERY_SERVICE_ACCOUNT_PATH


def resolve_dataset(dataset_id: str | None) -> str:
    """Resolve dataset ID from parameter or environment variable."""
    effective = dataset_id or os.getenv("BIGQUERY_DATASET_ID")
    if not effective:
        raise ValueError(
            "dataset_id must be provided or BIGQUERY_DATASET_ID env var must be set"
        )
    return effective


def load_expected_nfs_from_bigquery(
    pdf_names: list[str] | None = None,
    service_account_path: Path | None = None,
    dataset_id: str | None = None,
) -> list[dict]:
    """
    Load expected NFs from BigQuery despesas_recorte table with company opening dates.

    :param pdf_names: Optional list of PDF names to filter by (descricao_limpa
        field). If None, loads all NFs from the table.
    :param service_account_path: Optional path to BigQuery service account JSON.
        If None, uses BIGQUERY_SERVICE_ACCOUNT_PATH from config.
    :returns: List of expected NF dicts with keys:
        - pdf_name: PDF filename (from descricao_limpa)
        - cnpj: CNPJ from table
        - numero_nf: NF number (from num_documento)
        - valor_total: Expected total value (from valor_documento)
        - data_envio: Submission date
        - data_emissao: Emission date (for date mismatch validation)
        - cod_organizacao: Organization code
        - cod_unidade: Unit code
        - cnpj_data_abertura: Company opening date from bcadastro (for service
          date validation)
        - page: Page number (not available from BigQuery, set to 'Unknown')

    Example::

        # Load expected NFs for specific PDFs
        expected_nfs = load_expected_nfs_from_bigquery([
            'invoice_001.pdf',
            'invoice_002.pdf'
        ])

        # Load all expected NFs
        all_nfs = load_expected_nfs_from_bigquery()
    """
    # Use default path if not provided
    if service_account_path is None:
        service_account_path = BIGQUERY_SERVICE_ACCOUNT_PATH

    # Check if credentials file exists
    if not service_account_path.exists():
        raise FileNotFoundError(
            f"BigQuery service account file not found: {service_account_path}\n"
            f"Please place your BigQuery service account JSON at:\n"
            f"  {service_account_path}"
        )

    # Load credentials
    credentials = service_account.Credentials.from_service_account_file(
        str(service_account_path)
    )

    # Create BigQuery client
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    project = client.project
    ds = resolve_dataset(dataset_id)

    # Build query with bcadastro join to get company opening date
    query = f"""
    WITH despesas AS (
        SELECT
            id_documento,
            descricao_limpa,
            cnpj,
            num_documento,
            valor_documento,
            data_envio,
            data_emissao,
            cod_organizacao,
            cod_unidade,
            valor_pago,
            LPAD(REGEXP_REPLACE(cnpj, r'[^0-9]', ''), 14, '0') AS cleaned_cnpj
        FROM
            `{project}.{ds}.osinfo_despesas_recorte`
    ),
    bcadastro AS (
        SELECT
            LPAD(REGEXP_REPLACE(CAST(cnpj_particao AS STRING), r'[^0-9]', ''), 14, '0') AS cleaned_cnpj_reg,
            inicio_atividade_data AS cnpj_data_abertura
        FROM
            `{project}.{ds}.bcadastro_cnpj_recorte`
    )
    SELECT
        d.id_documento,
        d.descricao_limpa AS pdf_name,
        d.cnpj,
        d.num_documento AS numero_nf,
        d.valor_documento AS valor_total,
        d.data_envio,
        d.data_emissao,
        d.cod_organizacao,
        d.cod_unidade,
        d.valor_pago,
        b.cnpj_data_abertura,
        SUM(d.valor_pago) AS valor_pago_total,
        COUNT(*) AS num_parcelas
    FROM
        despesas d
    LEFT JOIN
        bcadastro b ON d.cleaned_cnpj = b.cleaned_cnpj_reg
    """

    # Add WHERE clause if filtering by PDF names
    if pdf_names:
        pdf_list_str = ", ".join(f"'{pdf}'" for pdf in pdf_names)
        query += f"""
    WHERE
        d.descricao_limpa IN ({pdf_list_str})
        """

    query += f"""
    GROUP BY
        d.id_documento,
        d.descricao_limpa,
        d.cnpj,
        d.num_documento,
        d.valor_documento,
        d.data_envio,
        d.data_emissao,
        d.cod_organizacao,
        d.cod_unidade,
        d.valor_pago,
        b.cnpj_data_abertura
    ORDER BY
        d.data_envio,
        d.id_documento
    """

    # Execute query
    query_job = client.query(query)
    results = query_job.result()

    # Convert to expected NF format
    expected_nfs = []
    for row in results:
        expected_nfs.append(
            {
                "id_documento": row.id_documento,
                "pdf_name": row.pdf_name,
                "cnpj": row.cnpj,
                "numero_nf": str(row.numero_nf) if row.numero_nf is not None else "",
                "valor_total": (
                    float(row.valor_total) if row.valor_total is not None else 0.0
                ),
                "data_envio": row.data_envio,  # Submission date for duplicate detection
                "data_emissao": (
                    str(row.data_emissao) if row.data_emissao is not None else None
                ),  # Emission date for date mismatch validation
                "cod_organizacao": row.cod_organizacao,  # Organization code for duplicate detection
                "cod_unidade": row.cod_unidade,  # Unit code for duplicate detection
                "cnpj_data_abertura": (
                    str(row.cnpj_data_abertura) if row.cnpj_data_abertura is not None else None
                ),  # Company opening date for service date validation
                "page": "Unknown",  # Page number not available in BigQuery
            }
        )

    return expected_nfs


def get_pdf_list_from_bigquery(
    service_account_path: Path | None = None,
    dataset_id: str | None = None,
) -> list[str]:
    """
    Get list of unique PDF names from BigQuery despesas_recorte table.

    :param service_account_path: Optional path to BigQuery service account JSON.
    :returns: List of unique PDF filenames.

    Example::

        pdf_list = get_pdf_list_from_bigquery()
        print(f"Found {len(pdf_list)} PDFs in BigQuery")
    """
    if service_account_path is None:
        service_account_path = BIGQUERY_SERVICE_ACCOUNT_PATH

    if not service_account_path.exists():
        raise FileNotFoundError(
            f"BigQuery service account file not found: {service_account_path}"
        )

    credentials = service_account.Credentials.from_service_account_file(
        str(service_account_path)
    )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    project = client.project
    ds = resolve_dataset(dataset_id)

    query = f"""
    SELECT DISTINCT descricao_limpa AS pdf_name
    FROM `{project}.{ds}.osinfo_despesas_recorte`
    ORDER BY pdf_name
    """

    query_job = client.query(query)
    results = query_job.result()

    return [row.pdf_name for row in results]


def get_company_start_date(
    cnpj: str,
    service_account_path: Path | None = None,
    dataset_id: str | None = None,
) -> str | None:
    """
    Query company start date from BigQuery.

    :param cnpj: CNPJ (any format, will be normalized to 14 digits).
    :param service_account_path: Optional path to BigQuery service account JSON.
    :returns: inicio_atividade_data as string or None if not found.

    Example::

        start_date = get_company_start_date("48.439.771/0001-06")
        # Returns: "2020-01-15" or None
    """
    # Import here to avoid circular dependency
    from ..compliance import normalize_cnpj

    # Normalize CNPJ to 14 digits
    cnpj_normalized = normalize_cnpj(cnpj)

    if not cnpj_normalized:
        return None

    # Convert CNPJ to INT64 (BigQuery table stores as integer)
    try:
        cnpj_int = int(cnpj_normalized)
    except (ValueError, TypeError):
        return None

    # Use default credentials if not provided
    if service_account_path is None:
        service_account_path = BIGQUERY_SERVICE_ACCOUNT_PATH

    if not service_account_path.exists():
        raise FileNotFoundError(
            f"BigQuery credentials not found: {service_account_path}"
        )

    # Create BigQuery client
    credentials = service_account.Credentials.from_service_account_file(
        str(service_account_path)
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    project = client.project
    ds = resolve_dataset(dataset_id)

    # Query table (cnpj column is INT64)
    query = f"""
    SELECT inicio_atividade_data
    FROM `{project}.{ds}.bcadastro_cnpj_recorte`
    WHERE cnpj = @cnpj_param
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cnpj_param", "INT64", cnpj_int)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    for row in results:
        if row.inicio_atividade_data:
            return str(row.inicio_atividade_data)

    return None


def get_deduplication_lookup_from_bigquery(
    service_account_path: Path | None = None,
    dataset_id: str | None = None,
) -> dict:
    """
    Build deduplication lookup from entire BigQuery despesas_recorte table.

    This queries ALL (cnpj, num_documento, cod_organizacao, cod_unidade) combinations
    in the database and returns which PDF files contain each combination, along with
    their submission dates and IDs for duplicate detection.

    Duplicate Detection Rules:
    - Key: (cnpj, numero_nf, cod_organizacao, cod_unidade) - exact 4-tuple must repeat
    - Special case: sede (cod_organizacao == cod_unidade) - never duplicate
    - Order: By (data_envio, id_documento) - first is original, rest are duplicates

    :param service_account_path: Optional path to BigQuery service account JSON.
    :returns: Dict mapping (cnpj_norm, numero_norm, cod_org, cod_unit) -> list
        of entry dicts.

    Example::

        {
            ('12345678000100', '123', 'ORG10', 'UNIT20'): [
                {'pdf_name': 'file1.pdf', 'data_envio': '2024-01-15', 'id_documento': 1001,
                 'cod_organizacao': 'ORG10', 'cod_unidade': 'UNIT20'},  # Original
                {'pdf_name': 'file2.pdf', 'data_envio': '2024-02-10', 'id_documento': 1002,
                 'cod_organizacao': 'ORG10', 'cod_unidade': 'UNIT20'}   # Duplicate (same 4-tuple)
            ],
            ('12345678000100', '123', 'ORG20', 'UNIT30'): [
                {'pdf_name': 'file3.pdf', 'data_envio': '2024-01-20', 'id_documento': 1003,
                 'cod_organizacao': 'ORG20', 'cod_unidade': 'UNIT30'}   # NOT duplicate (different 4-tuple)
            ]
        }
    """
    # Import here to avoid circular dependency
    from ..compliance import normalize_cnpj, normalize_number

    if service_account_path is None:
        service_account_path = BIGQUERY_SERVICE_ACCOUNT_PATH

    if not service_account_path.exists():
        raise FileNotFoundError(
            f"BigQuery credentials not found: {service_account_path}"
        )

    credentials = service_account.Credentials.from_service_account_file(
        str(service_account_path)
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    project = client.project
    ds = resolve_dataset(dataset_id)

    # Query: Get all unique (cnpj, num_documento, org, unit, descricao_limpa, data_envio, id) combinations
    query = f"""
    SELECT DISTINCT
        id_documento,
        cnpj,
        num_documento,
        descricao_limpa AS pdf_name,
        data_envio,
        cod_organizacao,
        cod_unidade
    FROM
        `{project}.{ds}.osinfo_despesas_recorte`
    WHERE
        cnpj IS NOT NULL
        AND num_documento IS NOT NULL
    ORDER BY
        cnpj, num_documento, cod_organizacao, cod_unidade, data_envio, id_documento
    """

    query_job = client.query(query)
    results = query_job.result()

    # Build lookup: (cnpj_norm, numero_norm, cod_org, cod_unit) -> [{'pdf_name', 'data_envio', 'id_documento'}]
    # 4-field dedup key: exact (cnpj, numero, org, unit) combination must repeat to be duplicate
    deduplication_lookup = {}

    for row in results:
        # Normalize keys (same normalization as ComplianceValidator)
        cnpj_norm = normalize_cnpj(row.cnpj)
        numero_norm = normalize_number(
            str(row.num_documento) if row.num_documento else ""
        )
        cod_org = row.cod_organizacao if row.cod_organizacao else ""
        cod_unit = row.cod_unidade if row.cod_unidade else ""

        if not cnpj_norm or not numero_norm:
            continue  # Skip invalid entries

        # 4-field dedup key: (cnpj, numero, cod_org, cod_unit)
        dedup_key = (cnpj_norm, numero_norm, cod_org, cod_unit)

        if dedup_key not in deduplication_lookup:
            deduplication_lookup[dedup_key] = []

        # Store fields needed for duplicate detection
        entry = {
            "pdf_name": row.pdf_name,
            "data_envio": row.data_envio,  # Can be string "YYYY-MM-DD" or datetime object
            "id_documento": row.id_documento,  # Tie-breaker when dates are equal
            "cod_organizacao": cod_org,
            "cod_unidade": cod_unit,
        }

        # Only add if not already in list (should be DISTINCT, but safe)
        if not any(
            e["pdf_name"] == row.pdf_name for e in deduplication_lookup[dedup_key]
        ):
            deduplication_lookup[dedup_key].append(entry)

    return deduplication_lookup


def validate_payment_totals(
    pdf_names: list[str] | None = None,
    service_account_path: Path | None = None,
    dataset_id: str | None = None,
) -> list[dict]:
    """
    Check for payment validation issues (overpayments).

    :param pdf_names: Optional list of PDF names to filter by.
    :param service_account_path: Optional path to BigQuery service account JSON.
    :returns: List of dicts with payment validation results:
        - pdf_name: PDF filename
        - cnpj: CNPJ
        - numero_nf: NF number
        - valor_documento: Expected total
        - valor_pago_total: Sum of payments
        - difference: valor_pago_total - valor_documento
        - status: 'OK', 'OVERPAID', or 'UNDERPAID'

    Example::

        issues = validate_payment_totals()
        overpaid = [nf for nf in issues if nf['status'] == 'OVERPAID']
        print(f"Found {len(overpaid)} overpayment issues")
    """
    if service_account_path is None:
        service_account_path = BIGQUERY_SERVICE_ACCOUNT_PATH

    if not service_account_path.exists():
        raise FileNotFoundError(
            f"BigQuery service account file not found: {service_account_path}"
        )

    credentials = service_account.Credentials.from_service_account_file(
        str(service_account_path)
    )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    project = client.project
    ds = resolve_dataset(dataset_id)

    query = f"""
    SELECT
        descricao_limpa AS pdf_name,
        cnpj,
        num_documento AS numero_nf,
        valor_documento,
        SUM(valor_pago) AS valor_pago_total,
        ROUND(SUM(valor_pago) - valor_documento, 2) AS difference,
        CASE
            WHEN SUM(valor_pago) > valor_documento THEN 'OVERPAID'
            WHEN SUM(valor_pago) = valor_documento THEN 'OK'
            WHEN SUM(valor_pago) < valor_documento THEN 'UNDERPAID'
            ELSE 'UNKNOWN'
        END AS status
    FROM
        `{project}.{ds}.osinfo_despesas_recorte`
    """

    if pdf_names:
        pdf_list_str = ", ".join(f"'{pdf}'" for pdf in pdf_names)
        query += f"""
    WHERE
        descricao_limpa IN ({pdf_list_str})
        """

    query += """
    GROUP BY
        descricao_limpa,
        cnpj,
        num_documento,
        valor_documento
    HAVING
        SUM(valor_pago) != valor_documento
    ORDER BY
        difference DESC
    """

    query_job = client.query(query)
    results = query_job.result()

    validation_results = []
    for row in results:
        validation_results.append(
            {
                "pdf_name": row.pdf_name,
                "cnpj": row.cnpj,
                "numero_nf": str(row.numero_nf) if row.numero_nf is not None else "",
                "valor_documento": (
                    float(row.valor_documento)
                    if row.valor_documento is not None
                    else 0.0
                ),
                "valor_pago_total": (
                    float(row.valor_pago_total)
                    if row.valor_pago_total is not None
                    else 0.0
                ),
                "difference": (
                    float(row.difference) if row.difference is not None else 0.0
                ),
                "status": row.status,
            }
        )

    return validation_results
