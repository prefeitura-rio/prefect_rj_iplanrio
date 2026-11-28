-- Esvazia a tabela tmp da carga (tasks/load.py::carrega_classificacoes e
-- tasks/taxonomia.py::atualiza_tema_motivo), antes e depois do MERGE.
TRUNCATE TABLE `${tmp_full}`
