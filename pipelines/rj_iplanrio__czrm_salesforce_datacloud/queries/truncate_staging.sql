-- Limpa a staging depois de um MERGE bem-sucedido.
-- Se o MERGE falhar, a staging permanece (próximo tick a reutiliza e o
-- próprio MERGE deduplica).
TRUNCATE TABLE `$staging`
