from prefect import flow, task
from prefect.logging import get_run_logger
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
import pymssql
import pandas as pd
import traceback
import re
from pipelines.rj_iplanrio__1746_seconverva_salesforce_poc.tasks import get_1746_credentials

DEFAULT_QUERY = """
SELECT DISTINCT
    ch.id_chamado,
    ch.dt_inicio AS [data_bruta],
    pr.ds_codigo_fk AS [protocolo_sgrc],
    ct.id_categoria AS [codigo_classificacao],
    ct.no_categoria AS [nome_classificacao],
    tp.id_tipo,
    tp.no_tipo AS [nome_tipo],
    sub.id_subtipo,
    sub.no_subtipo AS [nome_subtipo],
    CASE
        WHEN fotos.total_fotos > 0
             OR ch.ds_chamado LIKE '%ANEXOS:%'
             OR ch.ds_url_foto IS NOT NULL
        THEN 'Sim'
        ELSE 'Não'
    END AS [tem_anexo_sgrc],
    ch.ds_url_foto AS [url_anexo_direto],
    st.no_status,
    ch.ds_chamado AS [descricao_abertura],
    an.ds_andamento AS [resposta_finalizacao],
    lg.no_logradouro AS [endereco_logradouro],
    iplan.no_logradouro_completo AS [endereco_logradouro_completo],
    ch.ds_endereco_numero AS [endereco_numero],
    ch.ds_endereco_complemento AS [endereco_complemento],
    ch.ds_endereco_referencia AS [endereco_referencia],
    br.no_bairro AS [bairro],
    bl.id_logradouro_fk AS [id_logradouro_ipp],
    bl.id_bairro_fk AS [id_bairro_ipp],
    COALESCE(sc_num.nu_coord_x, sc_rua.nu_coord_x) AS [coord_utm_x],
    COALESCE(sc_num.nu_coord_y, sc_rua.nu_coord_Y) AS [coord_utm_y],
    CASE
        WHEN sc_num.id_coordenada IS NOT NULL THEN 'numero'
        WHEN sc_rua.id_coordenada IS NOT NULL THEN 'logradouro'
        ELSE NULL
    END AS [coord_precisao],
    oc.no_origem_ocorrencia AS [canal_abertura],
    pe.no_pessoa AS [nome_cidadao],
    pe.ds_cpf AS [cpf_cidadao],
    pe.ds_telefone_1 AS [telefone_1],
    pe.ds_telefone_2 AS [telefone_2],
    pe.ds_telefone_3 AS [telefone_3],
    CONVERT(VARCHAR, CONVERT(DATETIME, ch.dt_inicio, 10), 20) AS [dt_inicio_formatado],
    CONVERT(VARCHAR, CONVERT(DATETIME, ch.dt_fim, 10), 20) AS [dt_fim],
    uo.no_unidade_organizacional AS [unidade_atual],
    CASE
        WHEN EXISTS (
            SELECT 1 FROM tb_protocolo_chamado WITH (NOLOCK)
            WHERE id_chamado_fk = ch.id_chamado AND ic_motivo = 'E'
        ) THEN 'Sim'
        ELSE 'Não'
    END AS [is_equivalencia],
    CASE
        WHEN (REPLACE(REPLACE(REPLACE(pe.ds_telefone_1, '(', ''), ')', ''), '-', '') LIKE '__9________' AND REPLACE(REPLACE(REPLACE(pe.ds_telefone_1, '(', ''), ')', ''), '-', '') <> '21999999999')
             OR (REPLACE(REPLACE(REPLACE(pe.ds_telefone_2, '(', ''), ')', ''), '-', '') LIKE '__9________' AND REPLACE(REPLACE(REPLACE(pe.ds_telefone_2, '(', ''), ')', ''), '-', '') <> '21999999999')
             OR (REPLACE(REPLACE(REPLACE(pe.ds_telefone_3, '(', ''), ')', ''), '-', '') LIKE '__9________' AND REPLACE(REPLACE(REPLACE(pe.ds_telefone_3, '(', ''), ')', ''), '-', '') <> '21999999999')
        THEN 'Sim'
        ELSE 'Não'
    END AS [possui_celular]

FROM tb_chamado AS ch WITH (NOLOCK)

INNER JOIN tb_origem_ocorrencia AS oc WITH (NOLOCK)
    ON oc.id_origem_ocorrencia = ch.id_origem_ocorrencia_fk

INNER JOIN tb_status AS st WITH (NOLOCK)
    ON st.id_status = ch.id_status_fk

-- Responsavel atual -> UO atual
INNER JOIN (
    SELECT MAX(id_responsavel_chamado) AS Responsavel_ID, id_chamado_fk
    FROM tb_responsavel_chamado WITH (NOLOCK)
    GROUP BY id_chamado_fk
) AS last_rc ON last_rc.id_chamado_fk = ch.id_chamado
INNER JOIN tb_responsavel_chamado AS rc WITH (NOLOCK)
    ON rc.id_responsavel_chamado = last_rc.Responsavel_ID
INNER JOIN tb_unidade_organizacional AS uo WITH (NOLOCK)
    ON uo.id_unidade_organizacional = rc.id_unidade_organizacional_fk

-- Classificacao atual -> categoria / subtipo / tipo
INNER JOIN (
    SELECT MAX(id_classificacao_chamado) AS Ultima_Classificacao, id_chamado_fk
    FROM tb_classificacao_chamado WITH (NOLOCK)
    GROUP BY id_chamado_fk
) AS last_cl ON last_cl.id_chamado_fk = ch.id_chamado
INNER JOIN tb_classificacao_chamado AS cl WITH (NOLOCK)
    ON cl.id_classificacao_chamado = last_cl.Ultima_Classificacao
INNER JOIN tb_classificacao AS cll WITH (NOLOCK)
    ON cll.id_classificacao = cl.id_classificacao_fk
INNER JOIN tb_categoria AS ct WITH (NOLOCK)
    ON ct.id_categoria = cll.id_categoria_fk
INNER JOIN tb_subtipo AS sub WITH (NOLOCK)
    ON sub.id_subtipo = cll.id_subtipo_fk
INNER JOIN tb_tipo AS tp WITH (NOLOCK)
    ON tp.id_tipo = sub.id_tipo_fk

-- Todos os protocolos vinculados (DISTINCT garante 1 linha por chamado+protocolo)
LEFT JOIN tb_protocolo_chamado AS pc WITH (NOLOCK)
    ON pc.id_chamado_fk = ch.id_chamado
LEFT JOIN tb_protocolo AS pr WITH (NOLOCK)
    ON pr.id_protocolo = pc.id_protocolo_fk
LEFT JOIN tb_pessoa AS pe WITH (NOLOCK)
    ON pe.id_pessoa = pr.id_pessoa_fk

-- Ultimo andamento
LEFT JOIN (
    SELECT MAX(id_andamento) AS Ultimo_Andamento_ID, id_chamado_fk
    FROM tb_andamento WITH (NOLOCK)
    GROUP BY id_chamado_fk
) AS last_an ON last_an.id_chamado_fk = ch.id_chamado
LEFT JOIN tb_andamento AS an WITH (NOLOCK)
    ON an.id_andamento = last_an.Ultimo_Andamento_ID

-- Fotos
LEFT JOIN (
    SELECT id_chamado_fk, COUNT(*) AS total_fotos
    FROM tb_chamado_foto WITH (NOLOCK)
    GROUP BY id_chamado_fk
) AS fotos ON fotos.id_chamado_fk = ch.id_chamado

-- Endereco
LEFT JOIN tb_bairro_logradouro AS bl WITH (NOLOCK)
    ON bl.id_bairro_logradouro = ch.id_bairro_logradouro_fk
LEFT JOIN tb_logradouro AS lg WITH (NOLOCK)
    ON lg.id_logradouro = bl.id_logradouro_fk
LEFT JOIN tb_bairro AS br WITH (NOLOCK)
    ON br.id_bairro = bl.id_bairro_fk

-- Dados enriquecidos do IPP (nome completo do logradouro, tipo)
LEFT JOIN st_bairro_logradouro_iplan AS iplan WITH (NOLOCK)
    ON iplan.id_logradouro = bl.id_logradouro_fk
    AND iplan.id_bairro = bl.id_bairro_fk

-- Coordenada UTM por logradouro+numero exato
LEFT JOIN st_coordenada AS sc_num WITH (NOLOCK)
    ON sc_num.id_logradouro_fk = bl.id_logradouro_fk
    AND sc_num.ds_endereco_numero = ch.ds_endereco_numero

-- Fallback: coordenada do ponto central do logradouro (sem numero)
LEFT JOIN st_coordenada AS sc_rua WITH (NOLOCK)
    ON sc_rua.id_logradouro_fk = bl.id_logradouro_fk
    AND sc_rua.ds_endereco_numero IS NULL

WHERE
    uo.id_unidade_organizacional = 182
    AND cll.id_categoria_fk = 2
    AND ch.dt_inicio >= '2026-01-01'
    AND ch.dt_inicio <  '2027-01-01'

ORDER BY ch.dt_inicio DESC
"""


@task(retries=0)
def fetch_data_from_db(query: str, config: dict):
    logger = get_run_logger()
    logger.info(f"Conectando ao banco na query: {query}")
    try:
        conn = pymssql.connect(**config)
        df = pd.read_sql(query, conn)
        logger.info(f"Query retornou {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao executar a query:\n{traceback.format_exc()}")
        raise e
    finally:
        if "conn" in locals() and conn:
            conn.close()


@task
def post_process(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pós-processamento do DataFrame:
    1. Extrai links de anexos do campo descricao_abertura -> url_anexo_direto
    2. Normaliza campos de telefone (remove .0 de float)
    3. Cria colunas celulares e celular_principal
    4. Remove duplicatas de equivalência do mesmo cidadão (mesmo CPF)
    5. Remove colunas brutas de telefone e possui_celular
    """
    logger = get_run_logger()

    # ------------------------------------------------------------------
    # 1. Extração de anexos
    # ------------------------------------------------------------------
    def extrair_anexos(row):
        url_direto = row.get("url_anexo_direto")
        descricao = row.get("descricao_abertura") or ""

        links = []
        match = re.search(
            r"ANEXOS:\s*(.+?)(?:\s*>>|$)", descricao, re.IGNORECASE | re.DOTALL
        )
        if match:
            trecho = match.group(1)
            for parte in trecho.split("||"):
                link = parte.strip()
                if link.startswith("http"):
                    links.append(link)

        if links:
            return ";".join(links)

        if pd.notna(url_direto) and str(url_direto).strip():
            return url_direto

        return None

    if "url_anexo_direto" in df.columns:
        df["url_anexo_direto"] = df.apply(extrair_anexos, axis=1)

    # ------------------------------------------------------------------
    # 2. Normalizar telefones (float -> string sem ".0")
    # ------------------------------------------------------------------
    for col in ["telefone_1", "telefone_2", "telefone_3"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: str(int(float(x)))
                if pd.notna(x) and str(x).strip() not in ("", "nan")
                else ""
            )

    # ------------------------------------------------------------------
    # 3. Celulares
    # ------------------------------------------------------------------
    NUMEROS_FAKE = {"21999999999", ""}

    def _limpar_tel(tel):
        return (
            tel.replace("(", "")
            .replace(")", "")
            .replace("-", "")
            .replace(" ", "")
            .strip()
        )

    def _is_celular(tel):
        if not tel:
            return False
        limpo = _limpar_tel(tel)
        return limpo not in NUMEROS_FAKE and len(limpo) == 11 and limpo[2] == "9"

    def coletar_celulares(row):
        vistos = []
        for col in ["telefone_1", "telefone_2", "telefone_3"]:
            tel = row.get(col, "")
            if _is_celular(tel):
                limpo = _limpar_tel(tel)
                if limpo not in vistos:
                    vistos.append(limpo)
        return ";".join(vistos)

    def celular_principal(row):
        for col in ["telefone_1", "telefone_2", "telefone_3"]:
            tel = row.get(col, "")
            if _is_celular(tel):
                return _limpar_tel(tel)
        return ""

    df["celulares"] = df.apply(coletar_celulares, axis=1)
    df["celular_principal"] = df.apply(celular_principal, axis=1)

    # ------------------------------------------------------------------
    # 4. Deduplicação de equivalências do mesmo cidadão (mesmo CPF)
    # ------------------------------------------------------------------
    linhas_antes = len(df)

    if "is_equivalencia" in df.columns and "cpf_cidadao" in df.columns:
        sem_equiv = df[df["is_equivalencia"] == "Não"].copy()
        com_equiv = df[df["is_equivalencia"] == "Sim"].copy()

        linhas_manter = []
        for chamado_id, grupo in com_equiv.groupby("id_chamado"):
            if len(grupo) == 1:
                linhas_manter.append(grupo)
                continue

            com_cpf = grupo[
                grupo["cpf_cidadao"].notna()
                & (grupo["cpf_cidadao"].astype(str).str.strip() != "")
            ]
            sem_cpf = grupo[
                grupo["cpf_cidadao"].isna()
                | (grupo["cpf_cidadao"].astype(str).str.strip() == "")
            ]

            grupos_cpf = []
            if not com_cpf.empty:
                for cpf, subgrupo in com_cpf.groupby("cpf_cidadao"):
                    # Manter apenas o protocolo mais antigo (menor string = mais antigo)
                    mais_antigo = subgrupo.sort_values("protocolo_sgrc").iloc[[0]]
                    grupos_cpf.append(mais_antigo)
            if not sem_cpf.empty:
                grupos_cpf.append(sem_cpf)

            if grupos_cpf:
                linhas_manter.append(pd.concat(grupos_cpf))

        com_equiv_dedup = (
            pd.concat(linhas_manter) if linhas_manter else com_equiv.iloc[0:0]
        )
        df = (
            pd.concat([sem_equiv, com_equiv_dedup])
            .sort_values("data_bruta", ascending=False)
            .reset_index(drop=True)
        )

    linhas_removidas = linhas_antes - len(df)
    logger.info(
        f"Deduplicação de equivalências: {linhas_removidas} linhas removidas (mesmo CPF)."
    )

    # ------------------------------------------------------------------
    # 5. Converter coordenadas UTM (SIRGAS2000 zona 23S) → lat/long WGS84
    # ------------------------------------------------------------------
    if "coord_utm_x" in df.columns and "coord_utm_y" in df.columns:
        try:
            from pyproj import Transformer
            transformer = Transformer.from_crs("EPSG:31983", "EPSG:4326", always_xy=True)

            def _utm_to_latlong(row):
                x = row["coord_utm_x"]
                y = row["coord_utm_y"]
                if pd.isna(x) or pd.isna(y):
                    return pd.Series({"latitude": None, "longitude": None})
                lon, lat = transformer.transform(float(x), float(y))
                return pd.Series({"latitude": round(lat, 7), "longitude": round(lon, 7)})

            coords = df.apply(_utm_to_latlong, axis=1)
            df["latitude"] = coords["latitude"]
            df["longitude"] = coords["longitude"]
            logger.info("Conversão UTM → lat/long concluída.")
        except ImportError:
            logger.warning("pyproj não instalado. Mantendo coordenadas UTM brutas.")

    # ------------------------------------------------------------------
    # 6. Remover colunas brutas substituídas
    # ------------------------------------------------------------------
    cols_remover = [
        c
        for c in ["telefone_1", "telefone_2", "telefone_3", "possui_celular"]
        if c in df.columns
    ]
    df = df.drop(columns=cols_remover)

    logger.info(
        f"Pós-processamento concluído. Total de linhas: {len(df)}, chamados únicos: {df['id_chamado'].nunique()}"
    )
    return df


@task
def debug_environment_variables():
    import os

    logger = get_run_logger()
    db_keys = [k for k in os.environ.keys() if "DB_" in k.upper()]
    logger.info(
        f"DEBUG - Variáveis de ambiente 'DB' detectadas no container: {db_keys}"
    )


@flow(name="rj_iplanrio__1746_seconverva_salesforce_poc", log_prints=True)
def rj_iplanrio__1746_seconverva_salesforce_poc(
    query: str = DEFAULT_QUERY,
    db_host: str = "10.6.99.27",
    db_port: str = "1433",
    db_database: str = "pcrj",
    db_timeout: int = 300,
    spreadsheet_id: str = None,
    sheet_name: str = "Dados",
    execution_mode: str = "INCREMENTAL",
):
    """
    Fluxo para rodar queries exploratórias no banco do 1746 via Prefect,
    utilizando secrets da Infisical para nao expor credenciais.
    """
    logger = get_run_logger()
    debug_environment_variables()

    # Injetar secrets ambientais (Base dos Dados)
    inject_bd_credentials_task(environment="prod")

    # Buscar credenciais do banco 1746
    credentials = get_1746_credentials()

    # Montar configuração do banco
    config = {
        "server": db_host,
        "port": int(db_port),
        "user": credentials["user"],
        "password": credentials["password"],
        "database": db_database,
        "timeout": db_timeout,
    }

    try:
        df = fetch_data_from_db(query, config)
        df = post_process(df)

        if spreadsheet_id:
            from pipelines.rj_iplanrio__1746_seconverva_salesforce_poc.tasks import (
                write_to_google_sheets_task,
                get_existing_tickets_from_sheets_task,
                append_to_google_sheets_task
            )

            if execution_mode.upper() == "FULL":
                logger.info("Modo FULL selecionado: Sobrescrevendo a planilha com todos os dados capturados.")
                write_to_google_sheets_task(
                    dataframe=df, spreadsheet_id=spreadsheet_id, sheet_name=sheet_name
                )
            elif execution_mode.upper() == "INCREMENTAL":
                logger.info("Modo INCREMENTAL selecionado: Filtrando chamados já existentes.")
                existing_ids = get_existing_tickets_from_sheets_task(spreadsheet_id, sheet_name)
                
                # Assume que a coluna se chama 'id_chamado' no df final
                df_novos = df[~df["id_chamado"].astype(str).isin(existing_ids)].copy()
                
                logger.info(f"Dos {len(df)} chamados capturados, {len(df_novos)} são novos.")
                
                if not df_novos.empty:
                    append_to_google_sheets_task(
                        dataframe=df_novos, spreadsheet_id=spreadsheet_id, sheet_name=sheet_name
                    )
                else:
                    logger.info("Nenhum chamado novo para enviar à planilha.")
            else:
                logger.warning(f"Modo de execução desconhecido: {execution_mode}. Use INCREMENTAL ou FULL.")

    except Exception as e:
        logger.error(f"Falha na exploração: {e}")


if __name__ == "__main__":
    # Teste isolado nao recomendavel por conta de conexões dependentes do infisical.
    pass
