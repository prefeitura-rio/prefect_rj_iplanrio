-- SMS - Confirmação / lembrete de procedimento (Supercentro)
-- Colunas da DE: SubscriberKey;telefone;nome_sobrenome;tipo_atendimento;data;horario;unidade;
--                id_agendamento;id_solicitacao;no_equipamento;janela_confirmacao
-- ATENÇÃO: a tabela de origem é datada (disparo_whatsapp_supercentro_AAAAMMDD) — atualizar a cada lote.
select
  cpf,
  `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(telefone) as telefone,
  nome as nome_sobrenome,
  procedimento as tipo_atendimento,
  data,
  hora as horario,
  concat(local, " - ", endereco) as unidade,
  id_agendamento,
  id_solicitacao,
  concat("no ", local) as no_equipamento,
  dias_para_atendimento as janela_confirmacao
from `rj-sms.projeto_whatsapp.disparo_whatsapp_supercentro_20261127`
where
  cpf is not null
  and `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(telefone) is not null
