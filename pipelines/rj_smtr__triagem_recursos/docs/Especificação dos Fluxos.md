# Especificação dos Fluxos

Para evitar duplicação de dados e garantir a conformidade com as diretrizes do projeto, o **SEI-Rio** atua como a única fonte de verdade para processos e documentos oficiais, enquanto o **Banco Local (Cloud SQL - Postgres)** armazena exclusivamente os dados gerados pela inteligência da aplicação e registros de auditoria.

**Responsabilidade de Dados**

|  |  |  |
| --- | --- | --- |
| Dado | Onde fica | Observação |
| Processo, documentos e PDF | SEI | Buscados sob demanda via API/`mod-wssei`. *Os arquivos PDF não são salvos em disco nem no banco de dados*; são mantidos apenas em memória temporária durante o processamento da triagem e descartados em seguida. |
| Responsável pelo recurso | SEI | Atribuição e marcador; o banco não copia |
| Parecer e bloco de assinatura | SEI | Gerados no SEI via MCP utilizando as credenciais e permissões do usuário logado. |
| Resultados e Métricas da IA | Banco | Grava a classe predita, score bruto de confiança, justificativa textual, evidências encontradas, páginas relevantes, dados extraídos e as versões do modelo de IA e do prompt utilizados. |
| Histórico de revisões | Banco | Registra a sugestão da IA vs. a decisão final tomada pelo Jarista (quem alterou, o quê e quando); só acrescenta, nunca altera |
| Eventos e tempos | Banco | Triado, distribuído, revisado, homologado, assinado; base das métricas |
| Papéis (jarista, gestor) | Em aberto (Banco Local) | Tabela de usuários que vincula o login do SEI aos perfis de acesso (Jarista ou Gestor) e à respetiva JARI/caixa correspondente. |

Guardar o score bruto, e não só a cor, permite recalcular tudo se o limiar mudar.

### Como "meus recursos" é montado

1. O MCP pede ao SEI os processos atribuídos ao jarista logado.
2. Busca no banco o resultado da IA para esses mesmos números de processo.
3. Junta os dois e aplica os filtros (confiança, hipótese, revisado).

Se alguém mudar a atribuição direto no SEI, a próxima consulta já sai certa, porque nada foi copiado.
