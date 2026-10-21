-- Kill-switch operacional do disparo, usado por check_flow_status() em utils/dispatch.py.
-- Lê a planilha de controle (tabela externa) e devolve a configuração da campanha
-- no ambiente. nome_hsm é a chave técnica da campanha (o campaign_name do
-- deployment); nome_campanha é o rótulo legível, usado só nas mensagens.
-- A tabela externa mapeia as colunas da planilha por posição: se a ordem das colunas
-- mudar na planilha, o schema da tabela precisa ser atualizado junto.
select ativo, data_limite_disparo, nome_hsm, nome_campanha
from `rj-crm-registry.brutos_salesforce_staging.disparos_ativos`
where nome_hsm = '$campaign_name' and ambiente = '$flow_environment'
limit 1
