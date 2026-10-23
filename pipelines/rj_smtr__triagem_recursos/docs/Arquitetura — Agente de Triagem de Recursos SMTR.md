# Arquitetura — Agente de Triagem de Recursos SMTR

# 1. Visão Geral & Escopo

## **1.1. Contexto & Problema**

A versão 1 (v1) do sistema automatiza a triagem dos recursos intempestivos da Secretaria Municipal de Transportes (SMTR), integrando uma pipeline de IA já existente e validada ao SEI-Rio e ao Claude Desktop. A solução visa levar o trabalho de análise dos Jaristas (12 relatores) e de gestão de distribuição para uma interface conversacional no Claude Desktop.

- **Passivo acumulado:** \~25.000 recursos pendentes de analise
- **Entrada mensal:** \~4.500 recursos (4.000 a 5.000).
- **Capacidade atual:** 4.200/mês (12 jaristas × 350). 
- **Diagnóstico Operacional:** Sem o uso de IA, a capacidade da equipe apenas empata com a entrada mensal, tornando impossível a redução do passivo acumulado.
- **Acurácia do Protótipo:** 
    - Todos os casos: 96,5%
    - Verde (score ≥ 0,85): 98,9%
    - Vermelho (score \< 0,5) - **sem** recomendação da IA: cerca de 50%

Visao Geral

## **1.2. Escopo**

### **Dentro do Escopo**

| **Item** | **Observacao** |
| --- | --- |
| Triagem automática periódica do passivo e entradas mensais. | Rotina Periodica (dia ou semana) automatizada realizada antes da distribuição de recursos. |
| Tools do perfil Jaristas | - Listar recursos atribuídos (individual/lote) - Inspecionar justificativa (individual, verificar reasoning da classificação do modelo) - Visualizar PDF do recurso (individual) - Corrigir classificação (individual/lote) - Homologar encaminhamento (individual/lote) |
| Tools do perfil Gestor | - Listar todos os recursos e status - Listar recursos e status por jarista - Distribuir recursos pré-processados pela IA (lista de pares \[jarista,recurso\]) - Redistribuir recursos - Consultar métricas |
| Banco local | Persistência de resultados da IA, histórico de revisões, métricas e eventos. |

### Fora do Escopo 

| **Item** | **Observacao** |
| --- | --- |
| Assinatura automática de processos via MCP | Alternativa adotada: disponibilização do link direto para o bloco de assinatura no SEI). |
| Consulta automática ao sistema Radar | Sistema sem endpoint de imagem; triagens de NIC e Erro de Imagem permanecem com consulta manual |
| Reanálise sob demanda | Casos raros; deixar correção manual pelo Jarista |
| Interface web própria (telas/botões) | Substituída inteiramente pelo Claude Desktop. |
| Triagem de Recursos Intempestivos (PDFs) anexados diretamente no chat do Claude. | Não será analisado nem registrado pela ferramenta |

## **1.3 Premissas**

- **Disponibilidade do Módulo **`mod-wssei`**:** A arquitetura do MCP assume como premissa a liberação e homologação do módulo `mod-wssei` no SEI-Rio (previsão para 20/10). Caso ocorra atraso ou indisponibilidade, o **Plano B** prevê o uso da API padrão do SEI.
- **Integração sem Duplicação de Dados:** O banco local armazena estritamente os dados gerados pelo processamento do sistema (inteligência, raciocínio e logs). Processos, documentos, atribuições de responsáveis e marcadores permanecem sob gestão exclusiva do SEI-Rio.
- **Isonomia e Equilíbrio de Carga:** O pré-processamento pela pipeline de IA ocorre antes da distribuição pelo gestor para que o volume de análise manual de cada Jarista seja equivalente em complexidade e esforço.
- **Isolamento por JARI:** Cada recurso pertence a uma JARI específica vinculada à sua caixa no SEI. 

# 2. Solução & Componentes

## 2.1 Visão da Arquitetura

O Claude Desktop interage exclusivamente com o **MCP STMR**, enquanto a **Pipeline de Triagem** executa em lote de forma independente. O Claude não acessa diretamente o SEI nem o banco de dados local.

## 2.2 Componentes

| Componente | Responsabilidade Principal | Situação |
| --- | --- | --- |
| **Claude Desktop (Enterprise)** | - Interface única de interação do usuário. - Configurado via plugin da organização (composto por MCP, skills e instruções do agente), restrito e visível apenas para os grupos da STMR. | **Configurar** |
| **Plugin Jarista / Gestor SMTR** | - Único servidor que o Claude enxerga - Expõe as *tools* do Jarista e do Gestor e checa as regras de domínio antes de executar ações. | **Construir** |
| MCP (cliente) SEI | - *Fork* da biblioteca `mcp-seipro` contendo apenas as funções utilizadas. - Mesmo código compartilhado pelo MCP e pela Pipeline, porém com logins distintos (login do próprio usuário no MCP e usuário de serviço na Pipeline) | **Adaptar** |
| **Pipeline de Triagem** | - Execução em lote (já validada no protótipo) que lê as caixas do SEI via `mod-wssei` usando usuário de serviço dedicado. - Roda periodicamente (no passivo e no fluxo mensal), baixa PDFs temporários, calcula o score de confiança e grava as saídas no banco local | **Adaptar** |
| **Banco Local** | Base de dados que armazena o que não existe no SEI: resultados e raciocínio da IA, histórico aditivo de revisões, log de eventos/tempos e mapeamento de papéis de usuários/JARIs | **Construir** |
| **SEI-Rio (via mod-wssei)** | Fonte de verdade de todos os processos, PDFs, atribuições de responsáveis, marcadores e blocos de assinatura. | **Em Homologação** |

## 2.2 Justificativa da Triagem em Pipeline Separada

A opção por executar a triagem em uma pipeline em lote separada, em vez de realizar o processamento em tempo real durante a conversa com o Claude, baseia-se nas seguintes razões técnicas e operacionais:

- **Complexidade do Processamento (OCR e Confiança):** O cálculo do score de confiança exige baixar o PDF, aplicar OCR e gerar embeddings. Executar esse fluxo de forma síncrona dentro do chat tornaria a interação com o usuário lenta e travada.
- **Volume do Passivo (\~25.000 recursos):** Processar um passivo desse porte exige execução automatizada em lote e em segundo plano, algo inviável de ser acionado caso a caso via conversa.
- **Equilíbrio na Distribuição de Carga:** Como o pré-processamento ocorre antes da etapa do gestor, é possível distribuir os recursos considerando a complexidade calculada pela IA, garantindo uma carga de trabalho justa entre os jaristas.
- **Reaproveitamento da Solução Existente:** Permite reutilizar a pipeline de triagem já construída e validada no protótipo (que alcançou 96,5% de acerto), evitando recriar a lógica do zero apenas com prompts e skills.
- **Flexibilidade de Modelos e Custos:** A execução desacoplada (via Bifrost) permite alternar entre diferentes modelos (como Gemini Flash e Claude Haiku) conforme custo e desempenho.

# 3. Infraestrutura, Implantação & Segurança

## 3.1 Topologia

A solução será implantada no projeto no Google Cloud Platform (GCP): `rj-agent-smtr-triagem-recursos`. Apenas o serviço do MCP STMR fica exposto à internet para conexão com a Anthropic; os demais componentes rodam de forma privada e controlada.

## 3.2 Autenticação

- **Jarista ou Gestor -\> MCP:** Autenticação via OAuth do conector do Claude. Na tela de login, o utilizador insere o login e senha do SEI. No modo remoto do `mcp-seipro`, as credenciais trafegam encriptadas dentro do token e **não ficam gravadas no servidor MCP**.
- **MCP -\> SEI-Rio:** Autenticado através do login do próprio usuário, tirado diretamente do token do OAuth.
- **MCP-\> Papel do Usuário:** Consulta à tabela de papéis no banco local utilizando a identificação do usuário do SEI para definir se tem perfil de Jarista ou Gestor
- **Pipeline -\> SEI-Rio:** Autenticada via conta de serviço (usuário de sistem do SEI), com senha mantida no Secret Manager.
- **MCP e Pipeline → Banco Local:** Conexão e autenticação geridas por contas de serviço do GCP.

# 4. Riscos, Contingências & Decisões Pendentes

## 4.1 Matriz de Riscos Técnicos e Mitigações

|  |  |  |
| --- | --- | --- |
| **Risco** | **Impacto** | **Plano de Mitigação / Ação** |
| **Atraso ou falha no **`mod-wssei` *(v4.1.5 para v5.0.5)* | Bloqueio na integração do MCP para leitura de PDFs e ações de escrita no SEI. | Testar na homologação na terça (20/10); adotar API padrão do SEI como **Plano B** (sem leitura de PDF e sem assinatura). |
| **Negativa de conta de serviço no SEI** | Obrigatoriedade de usar conta nominal para a pipeline, prejudicando a auditoria. | Solicitar formalmente com antecedência à equipe responsável pelo SEI. |
| **MCP público na internet** | Aumento da superfície de exposição a ataques externos. | Proteção via OAuth, restrição de lista de hosts permitidos e submissão à aprovação de segurança do GCP. |
| **Assinatura realizada fora do sistema** | Métricas e marcador incompletos | Consulta diária ao SEI para sincronização |
| **Aprovação sem leitura (Jarista concordando com a IA nos casos vermelhos)** | Homologação de erros de classificação da IA. | Implementar alertas para casos de baixa confiança e acompanhar a taxa de concordância por jarista nas métricas. |
| **Elevação de custo do modelo** *(O Gemini Flash dobrará de preço em janeiro)* | Aumento do gasto mensal de operação. | Comparar custos e desempenho no teste *harness*; manter a chave de API em nome da SMTR se optar pelo Haiku. |

## 4.2  Pendências e Próximos Passos 

|  |  |  |
| --- | --- | --- |
| **Item / Pendência** | **Responsável** | **Status / Ação** |
| **Testar **`mod-wssei`** na Homologação**Validar listagem de caixas, download de anexos, criação de blocos e histórico de andamentos. | Grupo | Aguardando liberação (20/10) |
| **Pedidos ao Time do SEI**Solicitar usuário de teste, conta de serviço e suporte à consulta de status de assinatura. | Grupo | A fazer |
| **Console do Claude Enterprise**Obter acesso de administrador para cadastrar e restringir o plugin aos grupos da STMR. | Felipe | A fazer |
| **Validação com Usuários**Confirmar o fluxo de parecer, regra de marcadores (marcar "julgado" só após assinatura) e blocos. | Tatiane Carrilho | A fazer |
| **Mapeamento de Papéis e JARIs**Definir se a informação de qual JARI pertence cada jarista vem do SEI ou é guardada no banco local. | Grupo | Em aberto |
| **Data de Assinatura via SEI**Testar se o histórico de andamentos traz a data da assinatura para assinaturas feitas fora do sistema. | Grupo | Em aberto |
