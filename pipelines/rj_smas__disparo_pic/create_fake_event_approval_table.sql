-- Criação da tabela de aprovação dos disparos
CREATE or replace TABLE `rj-crm-registry-dev.brutos_wetalkie_staging.aprovacao_evento` (
  APROVACAO_DISPARO_AVISO string,             -- Indicates if the dispatch was approved
  APROVACAO_DISPARO_LEMBRETE string,
  DATA_DISPARO_AVISO STRING,         -- Date when the dispatch should be made
  DATA_DISPARO_LEMBRETE STRING,
  DATA_ENTREGA STRING                    -- Date of the event (to get the PIC)

);

-- Inserção de dados fake na tabela de aprovação dos disparos
INSERT INTO `rj-crm-registry-dev.brutos_wetalkie_staging.aprovacao_evento` (
  APROVACAO_DISPARO_AVISO,
  APROVACAO_DISPARO_LEMBRETE,
  DATA_DISPARO_AVISO,
  DATA_DISPARO_LEMBRETE,
  DATA_ENTREGA
)
VALUES
  ("aprovado", "aprovado", cast(current_date() as string), cast(date_add(current_date(), interval 400 day) as string), cast(date_add(current_date(), interval 500 day) as string)),
  -- ("nao aprovado",  "nao aprovado", cast(current_date() as string), cast(date_add(current_date(), interval 4 day) as string), cast(date_add(current_date(), interval 5 day) as string)),
  ("nao aprovado", "nao aprovado", '2025-09-25', '2025-09-27', '2025-09-28'),
  ("aprovado", "aprovado", '2025-09-26','2025-09-29', '2025-09-30'),
  ("nao aprovado","nao aprovado", '2025-09-27', '2025-09-30', '2025-10-01'),
  ("aprovado", "aprovado", '2025-10-01', '2025-10-04', '2025-10-05');


  select * from `rj-crm-registry-dev.brutos_wetalkie_staging.aprovacao_evento`
