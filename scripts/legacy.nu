# The pipelines that still import the removed legacy modules and cannot use the shared flows.
# The deploy and `lint imports` skip them, so their current deployments keep the old images.
export const LEGACY = [
    rj_cor__precipitacao_alertario_sftp
    rj_crm__agentforce_classificacao_llm
    rj_crm__disparo_template
    rj_crm__geolocalizacao_residencia
    rj_crm__get_history_data
    rj_crm__modelo_qualidade_telefone
    rj_crm__relatorio_engajamento_hsm
    rj_crm__salesforce_datacloud
    rj_crm__whitelist_whatsapp
    rj_crm__whitelist_whatsapp_relatorio
    rj_iplanrio__1746_seconverva_salesforce_poc
    rj_iplanrio__1746_ticket_capture
    rj_iplanrio__cor_alerts_aggregator
    rj_iplanrio__cor_alerts_test_writer
    rj_iplanrio__eai_history
    rj_iplanrio__sisbicho_images
    rj_iplanrio__taxirio
    rj_iplanrio__test
    rj_pic__disparos_email
    rj_smas__cadunico
    rj_smas__disparo_cadunico
    rj_smas__disparo_pic
    rj_sms__sisreg_disparo_lembretes
    rj_smfp__nota_carioca_bq_to_oracle
    rj_smfp__nota_carioca_bq_to_oracle_validation
    rj_smfp__nota_carioca_oracle_probe
    rj_smfp__nota_carioca_oracle_to_bq
]
