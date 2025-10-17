from flows.whatsapp_flow import whatsapp_flow

whatsapp_flow(
    sql="SELECT celular_disparo, nome_completo, cpf, uf FROM dataset.tabela WHERE campanha='2025Q1'",
    config_path="config/example_transform.yaml",
    id_hsm=12345,
    campaign_name="Cadastro Único RJ",
    cost_center_id=42,
    infisical_secret_path="/wetalkie",
)
