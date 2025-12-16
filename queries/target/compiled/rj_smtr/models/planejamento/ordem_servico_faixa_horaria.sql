


    
        
    
    


with
    dados as (
        select
            safe_cast(data_versao as date) as data_versao,
            safe_cast(tipo_os as string) as tipo_os,
            safe_cast(servico as string) as servico,
            safe_cast(json_value(content, '$.vista') as string) as vista,
            safe_cast(json_value(content, '$.consorcio') as string) as consorcio,
            safe_cast(json_value(content, '$.extensao_ida') as float64) as extensao_ida,
            safe_cast(
                json_value(content, '$.extensao_volta') as float64
            ) as extensao_volta,
            
                safe_cast(
                    json_value(content, "$.horario_inicio_dias_uteis") as string
                ) as horario_inicio_dias_uteis,
                safe_cast(
                    json_value(content, "$.horario_fim_dias_uteis") as string
                ) as horario_fim_dias_uteis,
                safe_cast(
                    json_value(content, "$.partidas_ida_dias_uteis") as string
                ) as partidas_ida_dias_uteis,
                safe_cast(
                    json_value(content, "$.partidas_volta_dias_uteis") as string
                ) as partidas_volta_dias_uteis,
                safe_cast(
                    json_value(content, "$.viagens_dias_uteis") as string
                ) as viagens_dias_uteis,
                safe_cast(
                    json_value(content, "$.quilometragem_dias_uteis") as string
                ) as quilometragem_dias_uteis,
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_00h_e_03h_dias_uteis"
                            ) as string
                        )
                        as partidas_ida_entre_00h_e_03h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_00h_e_03h_dias_uteis"
                            ) as string
                        )
                        as partidas_volta_entre_00h_e_03h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_00h_e_03h_dias_uteis"
                            ) as string
                        )
                        as partidas_entre_00h_e_03h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_00h_e_03h_dias_uteis"
                            ) as string
                        )
                        as quilometragem_entre_00h_e_03h_dias_uteis,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_03h_e_06h_dias_uteis"
                            ) as string
                        )
                        as partidas_ida_entre_03h_e_06h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_03h_e_06h_dias_uteis"
                            ) as string
                        )
                        as partidas_volta_entre_03h_e_06h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_03h_e_06h_dias_uteis"
                            ) as string
                        )
                        as partidas_entre_03h_e_06h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_03h_e_06h_dias_uteis"
                            ) as string
                        )
                        as quilometragem_entre_03h_e_06h_dias_uteis,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_06h_e_09h_dias_uteis"
                            ) as string
                        )
                        as partidas_ida_entre_06h_e_09h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_06h_e_09h_dias_uteis"
                            ) as string
                        )
                        as partidas_volta_entre_06h_e_09h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_06h_e_09h_dias_uteis"
                            ) as string
                        )
                        as partidas_entre_06h_e_09h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_06h_e_09h_dias_uteis"
                            ) as string
                        )
                        as quilometragem_entre_06h_e_09h_dias_uteis,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_09h_e_12h_dias_uteis"
                            ) as string
                        )
                        as partidas_ida_entre_09h_e_12h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_09h_e_12h_dias_uteis"
                            ) as string
                        )
                        as partidas_volta_entre_09h_e_12h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_09h_e_12h_dias_uteis"
                            ) as string
                        )
                        as partidas_entre_09h_e_12h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_09h_e_12h_dias_uteis"
                            ) as string
                        )
                        as quilometragem_entre_09h_e_12h_dias_uteis,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_12h_e_15h_dias_uteis"
                            ) as string
                        )
                        as partidas_ida_entre_12h_e_15h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_12h_e_15h_dias_uteis"
                            ) as string
                        )
                        as partidas_volta_entre_12h_e_15h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_12h_e_15h_dias_uteis"
                            ) as string
                        )
                        as partidas_entre_12h_e_15h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_12h_e_15h_dias_uteis"
                            ) as string
                        )
                        as quilometragem_entre_12h_e_15h_dias_uteis,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_15h_e_18h_dias_uteis"
                            ) as string
                        )
                        as partidas_ida_entre_15h_e_18h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_15h_e_18h_dias_uteis"
                            ) as string
                        )
                        as partidas_volta_entre_15h_e_18h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_15h_e_18h_dias_uteis"
                            ) as string
                        )
                        as partidas_entre_15h_e_18h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_15h_e_18h_dias_uteis"
                            ) as string
                        )
                        as quilometragem_entre_15h_e_18h_dias_uteis,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_18h_e_21h_dias_uteis"
                            ) as string
                        )
                        as partidas_ida_entre_18h_e_21h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_18h_e_21h_dias_uteis"
                            ) as string
                        )
                        as partidas_volta_entre_18h_e_21h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_18h_e_21h_dias_uteis"
                            ) as string
                        )
                        as partidas_entre_18h_e_21h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_18h_e_21h_dias_uteis"
                            ) as string
                        )
                        as quilometragem_entre_18h_e_21h_dias_uteis,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_21h_e_24h_dias_uteis"
                            ) as string
                        )
                        as partidas_ida_entre_21h_e_24h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_21h_e_24h_dias_uteis"
                            ) as string
                        )
                        as partidas_volta_entre_21h_e_24h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_21h_e_24h_dias_uteis"
                            ) as string
                        )
                        as partidas_entre_21h_e_24h_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_21h_e_24h_dias_uteis"
                            ) as string
                        )
                        as quilometragem_entre_21h_e_24h_dias_uteis,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_24h_e_03h_dia_seguinte_dias_uteis"
                            ) as string
                        )
                        as partidas_ida_entre_24h_e_03h_dia_seguinte_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_24h_e_03h_dia_seguinte_dias_uteis"
                            ) as string
                        )
                        as partidas_volta_entre_24h_e_03h_dia_seguinte_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_24h_e_03h_dia_seguinte_dias_uteis"
                            ) as string
                        )
                        as partidas_entre_24h_e_03h_dia_seguinte_dias_uteis,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_24h_e_03h_dia_seguinte_dias_uteis"
                            ) as string
                        )
                        as quilometragem_entre_24h_e_03h_dia_seguinte_dias_uteis,
                    
                
            
                safe_cast(
                    json_value(content, "$.horario_inicio_sabado") as string
                ) as horario_inicio_sabado,
                safe_cast(
                    json_value(content, "$.horario_fim_sabado") as string
                ) as horario_fim_sabado,
                safe_cast(
                    json_value(content, "$.partidas_ida_sabado") as string
                ) as partidas_ida_sabado,
                safe_cast(
                    json_value(content, "$.partidas_volta_sabado") as string
                ) as partidas_volta_sabado,
                safe_cast(
                    json_value(content, "$.viagens_sabado") as string
                ) as viagens_sabado,
                safe_cast(
                    json_value(content, "$.quilometragem_sabado") as string
                ) as quilometragem_sabado,
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_00h_e_03h_sabado"
                            ) as string
                        )
                        as partidas_ida_entre_00h_e_03h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_00h_e_03h_sabado"
                            ) as string
                        )
                        as partidas_volta_entre_00h_e_03h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_00h_e_03h_sabado"
                            ) as string
                        )
                        as partidas_entre_00h_e_03h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_00h_e_03h_sabado"
                            ) as string
                        )
                        as quilometragem_entre_00h_e_03h_sabado,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_03h_e_06h_sabado"
                            ) as string
                        )
                        as partidas_ida_entre_03h_e_06h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_03h_e_06h_sabado"
                            ) as string
                        )
                        as partidas_volta_entre_03h_e_06h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_03h_e_06h_sabado"
                            ) as string
                        )
                        as partidas_entre_03h_e_06h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_03h_e_06h_sabado"
                            ) as string
                        )
                        as quilometragem_entre_03h_e_06h_sabado,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_06h_e_09h_sabado"
                            ) as string
                        )
                        as partidas_ida_entre_06h_e_09h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_06h_e_09h_sabado"
                            ) as string
                        )
                        as partidas_volta_entre_06h_e_09h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_06h_e_09h_sabado"
                            ) as string
                        )
                        as partidas_entre_06h_e_09h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_06h_e_09h_sabado"
                            ) as string
                        )
                        as quilometragem_entre_06h_e_09h_sabado,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_09h_e_12h_sabado"
                            ) as string
                        )
                        as partidas_ida_entre_09h_e_12h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_09h_e_12h_sabado"
                            ) as string
                        )
                        as partidas_volta_entre_09h_e_12h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_09h_e_12h_sabado"
                            ) as string
                        )
                        as partidas_entre_09h_e_12h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_09h_e_12h_sabado"
                            ) as string
                        )
                        as quilometragem_entre_09h_e_12h_sabado,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_12h_e_15h_sabado"
                            ) as string
                        )
                        as partidas_ida_entre_12h_e_15h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_12h_e_15h_sabado"
                            ) as string
                        )
                        as partidas_volta_entre_12h_e_15h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_12h_e_15h_sabado"
                            ) as string
                        )
                        as partidas_entre_12h_e_15h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_12h_e_15h_sabado"
                            ) as string
                        )
                        as quilometragem_entre_12h_e_15h_sabado,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_15h_e_18h_sabado"
                            ) as string
                        )
                        as partidas_ida_entre_15h_e_18h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_15h_e_18h_sabado"
                            ) as string
                        )
                        as partidas_volta_entre_15h_e_18h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_15h_e_18h_sabado"
                            ) as string
                        )
                        as partidas_entre_15h_e_18h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_15h_e_18h_sabado"
                            ) as string
                        )
                        as quilometragem_entre_15h_e_18h_sabado,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_18h_e_21h_sabado"
                            ) as string
                        )
                        as partidas_ida_entre_18h_e_21h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_18h_e_21h_sabado"
                            ) as string
                        )
                        as partidas_volta_entre_18h_e_21h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_18h_e_21h_sabado"
                            ) as string
                        )
                        as partidas_entre_18h_e_21h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_18h_e_21h_sabado"
                            ) as string
                        )
                        as quilometragem_entre_18h_e_21h_sabado,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_21h_e_24h_sabado"
                            ) as string
                        )
                        as partidas_ida_entre_21h_e_24h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_21h_e_24h_sabado"
                            ) as string
                        )
                        as partidas_volta_entre_21h_e_24h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_21h_e_24h_sabado"
                            ) as string
                        )
                        as partidas_entre_21h_e_24h_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_21h_e_24h_sabado"
                            ) as string
                        )
                        as quilometragem_entre_21h_e_24h_sabado,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_24h_e_03h_dia_seguinte_sabado"
                            ) as string
                        )
                        as partidas_ida_entre_24h_e_03h_dia_seguinte_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_24h_e_03h_dia_seguinte_sabado"
                            ) as string
                        )
                        as partidas_volta_entre_24h_e_03h_dia_seguinte_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_24h_e_03h_dia_seguinte_sabado"
                            ) as string
                        )
                        as partidas_entre_24h_e_03h_dia_seguinte_sabado,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_24h_e_03h_dia_seguinte_sabado"
                            ) as string
                        )
                        as quilometragem_entre_24h_e_03h_dia_seguinte_sabado,
                    
                
            
                safe_cast(
                    json_value(content, "$.horario_inicio_domingo") as string
                ) as horario_inicio_domingo,
                safe_cast(
                    json_value(content, "$.horario_fim_domingo") as string
                ) as horario_fim_domingo,
                safe_cast(
                    json_value(content, "$.partidas_ida_domingo") as string
                ) as partidas_ida_domingo,
                safe_cast(
                    json_value(content, "$.partidas_volta_domingo") as string
                ) as partidas_volta_domingo,
                safe_cast(
                    json_value(content, "$.viagens_domingo") as string
                ) as viagens_domingo,
                safe_cast(
                    json_value(content, "$.quilometragem_domingo") as string
                ) as quilometragem_domingo,
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_00h_e_03h_domingo"
                            ) as string
                        )
                        as partidas_ida_entre_00h_e_03h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_00h_e_03h_domingo"
                            ) as string
                        )
                        as partidas_volta_entre_00h_e_03h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_00h_e_03h_domingo"
                            ) as string
                        )
                        as partidas_entre_00h_e_03h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_00h_e_03h_domingo"
                            ) as string
                        )
                        as quilometragem_entre_00h_e_03h_domingo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_03h_e_06h_domingo"
                            ) as string
                        )
                        as partidas_ida_entre_03h_e_06h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_03h_e_06h_domingo"
                            ) as string
                        )
                        as partidas_volta_entre_03h_e_06h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_03h_e_06h_domingo"
                            ) as string
                        )
                        as partidas_entre_03h_e_06h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_03h_e_06h_domingo"
                            ) as string
                        )
                        as quilometragem_entre_03h_e_06h_domingo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_06h_e_09h_domingo"
                            ) as string
                        )
                        as partidas_ida_entre_06h_e_09h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_06h_e_09h_domingo"
                            ) as string
                        )
                        as partidas_volta_entre_06h_e_09h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_06h_e_09h_domingo"
                            ) as string
                        )
                        as partidas_entre_06h_e_09h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_06h_e_09h_domingo"
                            ) as string
                        )
                        as quilometragem_entre_06h_e_09h_domingo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_09h_e_12h_domingo"
                            ) as string
                        )
                        as partidas_ida_entre_09h_e_12h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_09h_e_12h_domingo"
                            ) as string
                        )
                        as partidas_volta_entre_09h_e_12h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_09h_e_12h_domingo"
                            ) as string
                        )
                        as partidas_entre_09h_e_12h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_09h_e_12h_domingo"
                            ) as string
                        )
                        as quilometragem_entre_09h_e_12h_domingo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_12h_e_15h_domingo"
                            ) as string
                        )
                        as partidas_ida_entre_12h_e_15h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_12h_e_15h_domingo"
                            ) as string
                        )
                        as partidas_volta_entre_12h_e_15h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_12h_e_15h_domingo"
                            ) as string
                        )
                        as partidas_entre_12h_e_15h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_12h_e_15h_domingo"
                            ) as string
                        )
                        as quilometragem_entre_12h_e_15h_domingo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_15h_e_18h_domingo"
                            ) as string
                        )
                        as partidas_ida_entre_15h_e_18h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_15h_e_18h_domingo"
                            ) as string
                        )
                        as partidas_volta_entre_15h_e_18h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_15h_e_18h_domingo"
                            ) as string
                        )
                        as partidas_entre_15h_e_18h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_15h_e_18h_domingo"
                            ) as string
                        )
                        as quilometragem_entre_15h_e_18h_domingo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_18h_e_21h_domingo"
                            ) as string
                        )
                        as partidas_ida_entre_18h_e_21h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_18h_e_21h_domingo"
                            ) as string
                        )
                        as partidas_volta_entre_18h_e_21h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_18h_e_21h_domingo"
                            ) as string
                        )
                        as partidas_entre_18h_e_21h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_18h_e_21h_domingo"
                            ) as string
                        )
                        as quilometragem_entre_18h_e_21h_domingo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_21h_e_24h_domingo"
                            ) as string
                        )
                        as partidas_ida_entre_21h_e_24h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_21h_e_24h_domingo"
                            ) as string
                        )
                        as partidas_volta_entre_21h_e_24h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_21h_e_24h_domingo"
                            ) as string
                        )
                        as partidas_entre_21h_e_24h_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_21h_e_24h_domingo"
                            ) as string
                        )
                        as quilometragem_entre_21h_e_24h_domingo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_24h_e_03h_dia_seguinte_domingo"
                            ) as string
                        )
                        as partidas_ida_entre_24h_e_03h_dia_seguinte_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_24h_e_03h_dia_seguinte_domingo"
                            ) as string
                        )
                        as partidas_volta_entre_24h_e_03h_dia_seguinte_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_24h_e_03h_dia_seguinte_domingo"
                            ) as string
                        )
                        as partidas_entre_24h_e_03h_dia_seguinte_domingo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_24h_e_03h_dia_seguinte_domingo"
                            ) as string
                        )
                        as quilometragem_entre_24h_e_03h_dia_seguinte_domingo,
                    
                
            
                safe_cast(
                    json_value(content, "$.horario_inicio_ponto_facultativo") as string
                ) as horario_inicio_ponto_facultativo,
                safe_cast(
                    json_value(content, "$.horario_fim_ponto_facultativo") as string
                ) as horario_fim_ponto_facultativo,
                safe_cast(
                    json_value(content, "$.partidas_ida_ponto_facultativo") as string
                ) as partidas_ida_ponto_facultativo,
                safe_cast(
                    json_value(content, "$.partidas_volta_ponto_facultativo") as string
                ) as partidas_volta_ponto_facultativo,
                safe_cast(
                    json_value(content, "$.viagens_ponto_facultativo") as string
                ) as viagens_ponto_facultativo,
                safe_cast(
                    json_value(content, "$.quilometragem_ponto_facultativo") as string
                ) as quilometragem_ponto_facultativo,
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_00h_e_03h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_ida_entre_00h_e_03h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_00h_e_03h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_volta_entre_00h_e_03h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_00h_e_03h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_entre_00h_e_03h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_00h_e_03h_ponto_facultativo"
                            ) as string
                        )
                        as quilometragem_entre_00h_e_03h_ponto_facultativo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_03h_e_06h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_ida_entre_03h_e_06h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_03h_e_06h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_volta_entre_03h_e_06h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_03h_e_06h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_entre_03h_e_06h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_03h_e_06h_ponto_facultativo"
                            ) as string
                        )
                        as quilometragem_entre_03h_e_06h_ponto_facultativo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_06h_e_09h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_ida_entre_06h_e_09h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_06h_e_09h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_volta_entre_06h_e_09h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_06h_e_09h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_entre_06h_e_09h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_06h_e_09h_ponto_facultativo"
                            ) as string
                        )
                        as quilometragem_entre_06h_e_09h_ponto_facultativo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_09h_e_12h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_ida_entre_09h_e_12h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_09h_e_12h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_volta_entre_09h_e_12h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_09h_e_12h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_entre_09h_e_12h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_09h_e_12h_ponto_facultativo"
                            ) as string
                        )
                        as quilometragem_entre_09h_e_12h_ponto_facultativo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_12h_e_15h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_ida_entre_12h_e_15h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_12h_e_15h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_volta_entre_12h_e_15h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_12h_e_15h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_entre_12h_e_15h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_12h_e_15h_ponto_facultativo"
                            ) as string
                        )
                        as quilometragem_entre_12h_e_15h_ponto_facultativo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_15h_e_18h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_ida_entre_15h_e_18h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_15h_e_18h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_volta_entre_15h_e_18h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_15h_e_18h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_entre_15h_e_18h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_15h_e_18h_ponto_facultativo"
                            ) as string
                        )
                        as quilometragem_entre_15h_e_18h_ponto_facultativo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_18h_e_21h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_ida_entre_18h_e_21h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_18h_e_21h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_volta_entre_18h_e_21h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_18h_e_21h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_entre_18h_e_21h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_18h_e_21h_ponto_facultativo"
                            ) as string
                        )
                        as quilometragem_entre_18h_e_21h_ponto_facultativo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_21h_e_24h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_ida_entre_21h_e_24h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_21h_e_24h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_volta_entre_21h_e_24h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_21h_e_24h_ponto_facultativo"
                            ) as string
                        )
                        as partidas_entre_21h_e_24h_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_21h_e_24h_ponto_facultativo"
                            ) as string
                        )
                        as quilometragem_entre_21h_e_24h_ponto_facultativo,
                    
                
                    
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_24h_e_03h_dia_seguinte_ponto_facultativo"
                            ) as string
                        )
                        as partidas_ida_entre_24h_e_03h_dia_seguinte_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_24h_e_03h_dia_seguinte_ponto_facultativo"
                            ) as string
                        )
                        as partidas_volta_entre_24h_e_03h_dia_seguinte_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_24h_e_03h_dia_seguinte_ponto_facultativo"
                            ) as string
                        )
                        as partidas_entre_24h_e_03h_dia_seguinte_ponto_facultativo,
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_24h_e_03h_dia_seguinte_ponto_facultativo"
                            ) as string
                        )
                        as quilometragem_entre_24h_e_03h_dia_seguinte_ponto_facultativo,
                    
                
            
        from
            `rj-smtr-staging`.`br_rj_riodejaneiro_gtfs_staging`.`ordem_servico_faixa_horaria`
        where data_versao = '2024-12-31'
        
    ),
    dados_dia as (
        select
            data_versao,
            tipo_os,
            servico,
            vista,
            consorcio,
            extensao_ida,
            extensao_volta,
            case
                when column_name like '%dias_uteis%'
                then 'Dia Útil'
                when column_name like '%sabado%'
                then 'Sabado'
                when column_name like '%domingo%'
                then 'Domingo'
                when column_name like '%ponto_facultativo%'
                then 'Ponto Facultativo'
            end as tipo_dia,
            max(
                case
                    when column_name like '%horario_inicio%'
                    then nullif(safe_cast(value as string), "—")
                end
            ) as horario_inicio,
            max(
                case
                    when column_name like '%horario_fim%'
                    then nullif(safe_cast(value as string), "—")
                end
            ) as horario_fim,
            max(
                case
                    when
                        column_name like '%partidas_ida_%'
                        and not column_name like '%entre%'
                    then safe_cast(value as int64)
                end
            ) as partidas_ida_dia,
            max(
                case
                    when
                        column_name like '%partidas_volta_%'
                        and not column_name like '%entre%'
                    then safe_cast(value as int64)
                end
            ) as partidas_volta_dia,
            max(
                case
                    when column_name like '%viagens_%' then safe_cast(value as float64)
                end
            ) as viagens_dia,
            max(
                case
                    when column_name like '%quilometragem_%'
                    then safe_cast(value as float64)
                end
            ) as quilometragem_dia
        from
            dados unpivot include nulls(
                value for column_name in (
                    
                        horario_inicio_dias_uteis,
                        horario_fim_dias_uteis,
                        partidas_ida_dias_uteis,
                        partidas_volta_dias_uteis,
                        viagens_dias_uteis,
                        quilometragem_dias_uteis
                        ,
                    
                        horario_inicio_sabado,
                        horario_fim_sabado,
                        partidas_ida_sabado,
                        partidas_volta_sabado,
                        viagens_sabado,
                        quilometragem_sabado
                        ,
                    
                        horario_inicio_domingo,
                        horario_fim_domingo,
                        partidas_ida_domingo,
                        partidas_volta_domingo,
                        viagens_domingo,
                        quilometragem_domingo
                        ,
                    
                        horario_inicio_ponto_facultativo,
                        horario_fim_ponto_facultativo,
                        partidas_ida_ponto_facultativo,
                        partidas_volta_ponto_facultativo,
                        viagens_ponto_facultativo,
                        quilometragem_ponto_facultativo
                        
                    
                )
            )
        group by 1, 2, 3, 4, 5, 6, 7, 8
    ),
    dados_faixa as (
        select
            data_versao,
            tipo_os,
            servico,
            vista,
            consorcio,
            extensao_ida,
            extensao_volta,
            case
                when column_name like '%dias_uteis%'
                then 'Dia Útil'
                when column_name like '%sabado%'
                then 'Sabado'
                when column_name like '%domingo%'
                then 'Domingo'
                when column_name like '%ponto_facultativo%'
                then 'Ponto Facultativo'
            end as tipo_dia,
            case
                
                    
                        when
                            column_name
                            like '%00h_e_03h%'
                        then '00:00:00'
                    
                
                    
                        when
                            column_name
                            like '%03h_e_06h%'
                        then '03:00:00'
                    
                
                    
                        when
                            column_name
                            like '%06h_e_09h%'
                        then '06:00:00'
                    
                
                    
                        when
                            column_name
                            like '%09h_e_12h%'
                        then '09:00:00'
                    
                
                    
                        when
                            column_name
                            like '%12h_e_15h%'
                        then '12:00:00'
                    
                
                    
                        when
                            column_name
                            like '%15h_e_18h%'
                        then '15:00:00'
                    
                
                    
                        when
                            column_name
                            like '%18h_e_21h%'
                        then '18:00:00'
                    
                
                    
                        when
                            column_name
                            like '%21h_e_24h%'
                        then '21:00:00'
                    
                
                    
                        when
                            column_name
                            like '%24h_e_03h_dia_seguinte%'
                        then '24:00:00'
                    
                
            end as faixa_horaria_inicio,
            case
                
                    
                        when
                            column_name
                            like '%00h_e_03h%'
                        then '02:59:59'
                    
                
                    
                        when
                            column_name
                            like '%03h_e_06h%'
                        then '05:59:59'
                    
                
                    
                        when
                            column_name
                            like '%06h_e_09h%'
                        then '08:59:59'
                    
                
                    
                        when
                            column_name
                            like '%09h_e_12h%'
                        then '11:59:59'
                    
                
                    
                        when
                            column_name
                            like '%12h_e_15h%'
                        then '14:59:59'
                    
                
                    
                        when
                            column_name
                            like '%15h_e_18h%'
                        then '17:59:59'
                    
                
                    
                        when
                            column_name
                            like '%18h_e_21h%'
                        then '20:59:59'
                    
                
                    
                        when
                            column_name
                            like '%21h_e_24h%'
                        then '23:59:59'
                    
                
                    
                        when
                            column_name
                            like '%24h_e_03h_dia_seguinte%'
                        then '26:59:59'
                    
                
            end as faixa_horaria_fim,
            sum(
                case
                    when column_name like '%partidas_ida_entre%'
                    then safe_cast(value as int64)
                    else 0
                end
            ) as partidas_ida,
            sum(
                case
                    when column_name like '%partidas_volta_entre%'
                    then safe_cast(value as int64)
                    else 0
                end
            ) as partidas_volta,
            sum(
                case
                    when column_name like '%partidas_entre%'
                    then safe_cast(value as int64)
                    else 0
                end
            ) as partidas,
            sum(
                case
                    when column_name like '%quilometragem%'
                    then safe_cast(value as float64)
                    else 0
                end
            ) as quilometragem
        from
            dados unpivot include nulls(
                value for column_name in (
                    
                        
                            
                                partidas_ida_entre_00h_e_03h_dias_uteis,
                                partidas_volta_entre_00h_e_03h_dias_uteis,
                                partidas_entre_00h_e_03h_dias_uteis,
                                quilometragem_entre_00h_e_03h_dias_uteis,
                            
                        
                            
                                partidas_ida_entre_03h_e_06h_dias_uteis,
                                partidas_volta_entre_03h_e_06h_dias_uteis,
                                partidas_entre_03h_e_06h_dias_uteis,
                                quilometragem_entre_03h_e_06h_dias_uteis,
                            
                        
                            
                                partidas_ida_entre_06h_e_09h_dias_uteis,
                                partidas_volta_entre_06h_e_09h_dias_uteis,
                                partidas_entre_06h_e_09h_dias_uteis,
                                quilometragem_entre_06h_e_09h_dias_uteis,
                            
                        
                            
                                partidas_ida_entre_09h_e_12h_dias_uteis,
                                partidas_volta_entre_09h_e_12h_dias_uteis,
                                partidas_entre_09h_e_12h_dias_uteis,
                                quilometragem_entre_09h_e_12h_dias_uteis,
                            
                        
                            
                                partidas_ida_entre_12h_e_15h_dias_uteis,
                                partidas_volta_entre_12h_e_15h_dias_uteis,
                                partidas_entre_12h_e_15h_dias_uteis,
                                quilometragem_entre_12h_e_15h_dias_uteis,
                            
                        
                            
                                partidas_ida_entre_15h_e_18h_dias_uteis,
                                partidas_volta_entre_15h_e_18h_dias_uteis,
                                partidas_entre_15h_e_18h_dias_uteis,
                                quilometragem_entre_15h_e_18h_dias_uteis,
                            
                        
                            
                                partidas_ida_entre_18h_e_21h_dias_uteis,
                                partidas_volta_entre_18h_e_21h_dias_uteis,
                                partidas_entre_18h_e_21h_dias_uteis,
                                quilometragem_entre_18h_e_21h_dias_uteis,
                            
                        
                            
                                partidas_ida_entre_21h_e_24h_dias_uteis,
                                partidas_volta_entre_21h_e_24h_dias_uteis,
                                partidas_entre_21h_e_24h_dias_uteis,
                                quilometragem_entre_21h_e_24h_dias_uteis,
                            
                        
                            
                                partidas_ida_entre_24h_e_03h_dia_seguinte_dias_uteis,
                                partidas_volta_entre_24h_e_03h_dia_seguinte_dias_uteis,
                                partidas_entre_24h_e_03h_dia_seguinte_dias_uteis,
                                quilometragem_entre_24h_e_03h_dia_seguinte_dias_uteis
                            
                        
                        ,
                    
                        
                            
                                partidas_ida_entre_00h_e_03h_sabado,
                                partidas_volta_entre_00h_e_03h_sabado,
                                partidas_entre_00h_e_03h_sabado,
                                quilometragem_entre_00h_e_03h_sabado,
                            
                        
                            
                                partidas_ida_entre_03h_e_06h_sabado,
                                partidas_volta_entre_03h_e_06h_sabado,
                                partidas_entre_03h_e_06h_sabado,
                                quilometragem_entre_03h_e_06h_sabado,
                            
                        
                            
                                partidas_ida_entre_06h_e_09h_sabado,
                                partidas_volta_entre_06h_e_09h_sabado,
                                partidas_entre_06h_e_09h_sabado,
                                quilometragem_entre_06h_e_09h_sabado,
                            
                        
                            
                                partidas_ida_entre_09h_e_12h_sabado,
                                partidas_volta_entre_09h_e_12h_sabado,
                                partidas_entre_09h_e_12h_sabado,
                                quilometragem_entre_09h_e_12h_sabado,
                            
                        
                            
                                partidas_ida_entre_12h_e_15h_sabado,
                                partidas_volta_entre_12h_e_15h_sabado,
                                partidas_entre_12h_e_15h_sabado,
                                quilometragem_entre_12h_e_15h_sabado,
                            
                        
                            
                                partidas_ida_entre_15h_e_18h_sabado,
                                partidas_volta_entre_15h_e_18h_sabado,
                                partidas_entre_15h_e_18h_sabado,
                                quilometragem_entre_15h_e_18h_sabado,
                            
                        
                            
                                partidas_ida_entre_18h_e_21h_sabado,
                                partidas_volta_entre_18h_e_21h_sabado,
                                partidas_entre_18h_e_21h_sabado,
                                quilometragem_entre_18h_e_21h_sabado,
                            
                        
                            
                                partidas_ida_entre_21h_e_24h_sabado,
                                partidas_volta_entre_21h_e_24h_sabado,
                                partidas_entre_21h_e_24h_sabado,
                                quilometragem_entre_21h_e_24h_sabado,
                            
                        
                            
                                partidas_ida_entre_24h_e_03h_dia_seguinte_sabado,
                                partidas_volta_entre_24h_e_03h_dia_seguinte_sabado,
                                partidas_entre_24h_e_03h_dia_seguinte_sabado,
                                quilometragem_entre_24h_e_03h_dia_seguinte_sabado
                            
                        
                        ,
                    
                        
                            
                                partidas_ida_entre_00h_e_03h_domingo,
                                partidas_volta_entre_00h_e_03h_domingo,
                                partidas_entre_00h_e_03h_domingo,
                                quilometragem_entre_00h_e_03h_domingo,
                            
                        
                            
                                partidas_ida_entre_03h_e_06h_domingo,
                                partidas_volta_entre_03h_e_06h_domingo,
                                partidas_entre_03h_e_06h_domingo,
                                quilometragem_entre_03h_e_06h_domingo,
                            
                        
                            
                                partidas_ida_entre_06h_e_09h_domingo,
                                partidas_volta_entre_06h_e_09h_domingo,
                                partidas_entre_06h_e_09h_domingo,
                                quilometragem_entre_06h_e_09h_domingo,
                            
                        
                            
                                partidas_ida_entre_09h_e_12h_domingo,
                                partidas_volta_entre_09h_e_12h_domingo,
                                partidas_entre_09h_e_12h_domingo,
                                quilometragem_entre_09h_e_12h_domingo,
                            
                        
                            
                                partidas_ida_entre_12h_e_15h_domingo,
                                partidas_volta_entre_12h_e_15h_domingo,
                                partidas_entre_12h_e_15h_domingo,
                                quilometragem_entre_12h_e_15h_domingo,
                            
                        
                            
                                partidas_ida_entre_15h_e_18h_domingo,
                                partidas_volta_entre_15h_e_18h_domingo,
                                partidas_entre_15h_e_18h_domingo,
                                quilometragem_entre_15h_e_18h_domingo,
                            
                        
                            
                                partidas_ida_entre_18h_e_21h_domingo,
                                partidas_volta_entre_18h_e_21h_domingo,
                                partidas_entre_18h_e_21h_domingo,
                                quilometragem_entre_18h_e_21h_domingo,
                            
                        
                            
                                partidas_ida_entre_21h_e_24h_domingo,
                                partidas_volta_entre_21h_e_24h_domingo,
                                partidas_entre_21h_e_24h_domingo,
                                quilometragem_entre_21h_e_24h_domingo,
                            
                        
                            
                                partidas_ida_entre_24h_e_03h_dia_seguinte_domingo,
                                partidas_volta_entre_24h_e_03h_dia_seguinte_domingo,
                                partidas_entre_24h_e_03h_dia_seguinte_domingo,
                                quilometragem_entre_24h_e_03h_dia_seguinte_domingo
                            
                        
                        ,
                    
                        
                            
                                partidas_ida_entre_00h_e_03h_ponto_facultativo,
                                partidas_volta_entre_00h_e_03h_ponto_facultativo,
                                partidas_entre_00h_e_03h_ponto_facultativo,
                                quilometragem_entre_00h_e_03h_ponto_facultativo,
                            
                        
                            
                                partidas_ida_entre_03h_e_06h_ponto_facultativo,
                                partidas_volta_entre_03h_e_06h_ponto_facultativo,
                                partidas_entre_03h_e_06h_ponto_facultativo,
                                quilometragem_entre_03h_e_06h_ponto_facultativo,
                            
                        
                            
                                partidas_ida_entre_06h_e_09h_ponto_facultativo,
                                partidas_volta_entre_06h_e_09h_ponto_facultativo,
                                partidas_entre_06h_e_09h_ponto_facultativo,
                                quilometragem_entre_06h_e_09h_ponto_facultativo,
                            
                        
                            
                                partidas_ida_entre_09h_e_12h_ponto_facultativo,
                                partidas_volta_entre_09h_e_12h_ponto_facultativo,
                                partidas_entre_09h_e_12h_ponto_facultativo,
                                quilometragem_entre_09h_e_12h_ponto_facultativo,
                            
                        
                            
                                partidas_ida_entre_12h_e_15h_ponto_facultativo,
                                partidas_volta_entre_12h_e_15h_ponto_facultativo,
                                partidas_entre_12h_e_15h_ponto_facultativo,
                                quilometragem_entre_12h_e_15h_ponto_facultativo,
                            
                        
                            
                                partidas_ida_entre_15h_e_18h_ponto_facultativo,
                                partidas_volta_entre_15h_e_18h_ponto_facultativo,
                                partidas_entre_15h_e_18h_ponto_facultativo,
                                quilometragem_entre_15h_e_18h_ponto_facultativo,
                            
                        
                            
                                partidas_ida_entre_18h_e_21h_ponto_facultativo,
                                partidas_volta_entre_18h_e_21h_ponto_facultativo,
                                partidas_entre_18h_e_21h_ponto_facultativo,
                                quilometragem_entre_18h_e_21h_ponto_facultativo,
                            
                        
                            
                                partidas_ida_entre_21h_e_24h_ponto_facultativo,
                                partidas_volta_entre_21h_e_24h_ponto_facultativo,
                                partidas_entre_21h_e_24h_ponto_facultativo,
                                quilometragem_entre_21h_e_24h_ponto_facultativo,
                            
                        
                            
                                partidas_ida_entre_24h_e_03h_dia_seguinte_ponto_facultativo,
                                partidas_volta_entre_24h_e_03h_dia_seguinte_ponto_facultativo,
                                partidas_entre_24h_e_03h_dia_seguinte_ponto_facultativo,
                                quilometragem_entre_24h_e_03h_dia_seguinte_ponto_facultativo
                            
                        
                        
                    
                )
            )
        where column_name like '%entre%'
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),
    dados_agrupados as (
        select
            dd.*,
            df.faixa_horaria_inicio,
            df.faixa_horaria_fim,
            df.partidas_ida,
            df.partidas_volta,
            case
                when
                    date('2024-12-31')
                    < date('2025-04-30')
                then df.partidas
                else df.partidas_ida + df.partidas_volta
            end as partidas,
            df.quilometragem
        from dados_dia as dd
        inner join
            dados_faixa as df
            on dd.data_versao = df.data_versao
            and dd.tipo_os = df.tipo_os
            and dd.servico = df.servico
            and dd.tipo_dia = df.tipo_dia
    )
select
    fi.feed_version,
    fi.feed_start_date,
    fi.feed_end_date,
    d.* except (data_versao),
    '' as versao_modelo
from dados_agrupados as d
left join `rj-smtr`.`gtfs`.`feed_info` as fi on d.data_versao = fi.feed_start_date
where
        d.data_versao = '2024-12-31'
        and fi.feed_start_date = '2024-12-31'
