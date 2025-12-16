

    select *
    from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`projeto_subsidio_sppo`.`aux_viagem_registros` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
    where n_registros_middle <= 0

