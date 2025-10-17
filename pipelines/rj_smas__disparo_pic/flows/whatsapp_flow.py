from prefect import flow
from tasks.extract import extract_from_bigquery
from tasks.validate import validate_data
from tasks.transform import transform_data
from tasks.prepare import prepare_destinations
from tasks.dispatch import send_to_api


@flow(name="whatsapp_dispatch_pipeline")
def whatsapp_flow(
    sql: str,
    config_path: str,
    id_hsm: int,
    campaign_name: str,
    cost_center_id: int,
    infisical_secret_path: str = "/wetalkie",
    dispatch_route: str = "dispatch",
    login_route: str = "users/login",
    params: dict | None = None,
):
    """
    Pipeline completa de disparo WhatsApp (BigQuery → Transform → API).

    Credenciais da API WeTalkie são obtidas automaticamente via Infisical.

    Args:
        sql: Query SQL para extrair dados do BigQuery
        config_path: Caminho para arquivo YAML com configurações de transformação
        id_hsm: ID do template HSM para envio
        campaign_name: Nome da campanha
        cost_center_id: ID do centro de custo
        infisical_secret_path: Path no Infisical onde estão as credenciais
        dispatch_route: Rota da API para dispatch
        login_route: Rota da API para login
        params: Parâmetros opcionais para substituição na query SQL
    """
    df = extract_from_bigquery(sql, params)
    df_validated = validate_data(df)
    df_transformed = transform_data(df_validated, config_path)
    destinations = prepare_destinations(df_transformed)

    send_to_api(
        destinations=destinations,
        id_hsm=id_hsm,
        campaign_name=campaign_name,
        cost_center_id=cost_center_id,
        infisical_secret_path=infisical_secret_path,
        dispatch_route=dispatch_route,
        login_route=login_route,
    )
