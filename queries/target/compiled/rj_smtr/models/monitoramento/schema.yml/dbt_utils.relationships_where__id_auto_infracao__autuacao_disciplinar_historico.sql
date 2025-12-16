




with left_table as (

  select
    `id_auto_infracao` as id

  from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`monitoramento`.`autuacao_disciplinar_historico` where data_inclusao_datalake between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))

  where `id_auto_infracao` is not null
    and id_infracao in (
  '016.II',
  '016.III',
  '016.IV',
  '016.V',
  '016.VI',
  '017.IV',
  '017.IV',
  '019.I',
  '019.II',
  '019.III',
  '023.VII',
  '024.VIII',
  '024.IX',
  '024.X',
  '024.XII',
  '024.XIII',
  '024.XIV',
  '024.XV',
  '024.XVI',
  '024.XVII',
  '025.I',
  '025.II',
  '025.III',
  '025.V',
  '025.VII',
  '025.X',
  '025.XI',
  '025.XII',
  '025.XIII',
  '025.XIV',
  '025.XV',
  '025.XVI',
  '026.I',
  '026.II',
  '026.III',
  '026.IV',
  '026.V',
  '026.VI',
  '026.VII',
  '026.VIII',
  '026.IX',
  '026.X',
  '026.XI',
  '026.XII',
  '026.XIII',
  '026.XIV'
)
and modo = 'ONIBUS'

),

right_table as (

  select
    id_auto_infracao as id

  from `rj-smtr`.`monitoramento`.`veiculo_fiscalizacao_lacre`

  where id_auto_infracao is not null
    and 1=1

),

exceptions as (

  select
    left_table.id,
    right_table.id as right_id

  from left_table

  left join right_table
         on left_table.id = right_table.id

  where right_table.id is null

)

select * from exceptions

