"""Constantes da extração XML (documentos fiscais nacionais) da Nota Carioca."""

QUERIES_ANCHOR = __file__

# Webservice ASMX da intranet da Nota Carioca (RotinasAutomaticas.vb: WebService Namespace e SoapDocumentMethod).
SOAP_NAMESPACE = "http://notacarioca.rio.gov.br/intranet"
OPERATION = "ExtrairDocumentosFiscaisNacionaisAutomatico"
REQUEST_ELEMENT = f"{OPERATION}Request"
SOAP_ACTION = f"{SOAP_NAMESPACE}/{OPERATION}"
URL_PROD = "https://notacarioca.rio.gov.br/intranet/RotinasAutomaticas.asmx"
URL_STAGING = "https://notacariocahom.rio.gov.br/intranet/RotinasAutomaticas.asmx"
# Os pods do k3s levam "connection reset" ao chamar a intranet direto; pelo squid do cluster (Service proxy/proxy) o
# webservice responde, em produção e em homologação.
PROXY_URL = "http://proxy.proxy.svc.cluster.local:3128"

# Pasta do Infisical com o Oracle (chaves DB_ORACLE_NOTA_FISCAL__*); cada ambiente tem o seu valor (staging =
# homologação).
INFISICAL_SECRET_PATH = "/db-oracle-nota-fiscal"
# Token do webservice: chega ao pod com o nome literal da chave no Infisical (sem prefixo de pasta).
TOKEN_KEY = "NOTA_CARIOCA_ROTINAS_TOKEN"

# NFSE_OWNER.TAREFAS (Tarefa.vb: eTarefaTipo e eTarefaStatus).
TASK_EXTRACTION = 46
TASK_POST_PROCESSING = 48
STATUS_SUCCESS = 2
STATUS_FAILED = 3
STATUS_INTERRUPTED = 5
STATUS_DESCRIPTIONS = {
    0: "aguardando execução",
    1: "em execução",
    2: "concluída com sucesso",
    3: "concluída com erro",
    4: "interrupção solicitada",
    5: "interrompida",
}
TASK_KINDS = {
    TASK_EXTRACTION: "extração de documentos fiscais nacionais",
    TASK_POST_PROCESSING: "pós-processamento das notas nacionais",
}

LABEL = "Extração XML"
SECONDS_PER_HOUR = 3600
SECONDS_PER_MINUTE = 60
