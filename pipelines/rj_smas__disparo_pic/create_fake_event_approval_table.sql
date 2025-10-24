-- Criação da tabela
CREATE or replace TABLE `rj-crm-registry-dev.brutos_wetalkie_staging.aprovacao_evento` (
  aprovacao_disparo string,             -- Indicates if the dispatch was approved
  data_primeiro_disparo STRING,         -- Date when the dispatch should be made
  data_lembrete STRING,
  data_evento STRING                    -- Date of the event (to get the PIC)
);

-- Inserção de dados fake
INSERT INTO `rj-crm-registry-dev.brutos_wetalkie_staging.aprovacao_evento` (
  aprovacao_disparo,
  data_primeiro_disparo,
  data_lembrete,
  data_evento
)
VALUES
  ("aprovado",  cast(current_date() as string), cast(date_add(current_date(), interval 4 day) as string), cast(date_add(current_date(), interval 5 day) as string)),
  -- ("nao aprovado",  cast(current_date() as string), cast(date_add(current_date(), interval 4 day) as string), cast(date_add(current_date(), interval 5 day) as string)),
  ("nao aprovado", '2025-09-25', '2025-09-27', '2025-09-28'),
  ("aprovado",  '2025-09-26','2025-09-29', '2025-09-30'),
  ("nao aprovado", '2025-09-27', '2025-09-30', '2025-10-01'),
  ("aprovado",  '2025-10-01', '2025-10-04', '2025-10-05');


  select * from `rj-crm-registry-dev.brutos_wetalkie_staging.aprovacao_evento`
