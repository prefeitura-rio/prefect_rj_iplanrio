-- Espelho dev de rj-segovi.adm_central_atendimento_1746.chamado_cpf
CREATE TABLE IF NOT EXISTS `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.chamado_cpf`
(
    id_chamado STRING,
    cpf STRING,
    id_pessoa STRING,
    id_protocolo STRING,
    id_protocolo_chamado STRING,
    numero_protocolo STRING,
    ic_motivo STRING,
    extracted_at TIMESTAMP,
    data_particao DATE
);
