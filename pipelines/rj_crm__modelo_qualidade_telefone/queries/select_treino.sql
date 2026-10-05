-- só entra quem tem rótulo (INNER JOIN): telefone sem disparo na janela futura
-- sorteada some do dataset de treino, igual ao notebook original.
SELECT f.telefone, f.data_corte, r.high_delivery, f.* EXCEPT (telefone, data_corte)
FROM features f
JOIN rotulo r ON r.telefone = f.telefone
