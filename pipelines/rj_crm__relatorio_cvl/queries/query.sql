SELECT
distinct 
   CASE
     WHEN LENGTH(contato.telefone) = 12 THEN
       CONCAT(
         SUBSTR(contato.telefone, 1, 4),
         '9',
         SUBSTR(contato.telefone, 5)
       )
     ELSE contato.telefone
     END AS contato_telefone,
     contato.telefone,
   msg.data AS mensagem_datahora,
   DATE_TRUNC(DATE(msg.data), MONTH) AS mes,
   ROW_NUMBER() OVER (
     PARTITION BY contato.telefone
     ORDER BY msg.data
   ) AS rn,
   protocolo,
 FROM `rj-crm-registry.intermediario_rmi_conversas.base_receptivo`,
 UNNEST(mensagens) as msg
 WHERE msg.data IS NOT NULL
   and DATE(msg.data) BETWEEN 
    DATE_SUB(DATE('{START_DATE_PLACEHOLDER}'), INTERVAL 10 DAY) AND DATE_ADD(DATE('{END_DATE_PLACEHOLDER}'), INTERVAL 10 DAY)
    and data_particao >= DATE_SUB(DATE('{START_DATE_PLACEHOLDER}'), interval 10 day)
    and data_particao <= DATE_ADD(DATE('{END_DATE_PLACEHOLDER}'), interval 10 day)
    and msg.fonte = 'CUSTOMER'
