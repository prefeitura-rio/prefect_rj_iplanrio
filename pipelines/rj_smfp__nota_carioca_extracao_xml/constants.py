"""Constants of rj_smfp__nota_carioca_extracao_xml."""

from typing import Final

QUERIES_ANCHOR: Final = __file__
"""A file of the pipeline folder, from which `render_query` finds `queries/`."""

SOAP_NAMESPACE: Final = "http://notacarioca.rio.gov.br/intranet"
"""The namespace of the ASMX web service of the intranet of the Nota Carioca."""

OPERATION: Final = "ExtrairDocumentosFiscaisNacionaisAutomatico"
REQUEST_ELEMENT: Final = f"{OPERATION}Request"
SOAP_ACTION: Final = f"{SOAP_NAMESPACE}/{OPERATION}"
SOAP_ENVELOPE_NAMESPACE: Final = "http://schemas.xmlsoap.org/soap/envelope/"

PROXY_URL: Final = "http://proxy.proxy.svc.cluster.local:3128"
"""The squid of the cluster: a pod that calls the intranet directly gets a reset."""

REQUEST_TIMEOUT_SECONDS: Final = 60.0
CALL_ATTEMPTS: Final = 3
CALL_BACKOFF_SECONDS: Final = 30.0
SNIPPET_LIMIT: Final = 300

TLS_CIPHERS: Final = (
    "@SECLEVEL=2:ECDH+AESGCM:ECDH+CHACHA20:ECDH+AES:DHE+AES:AES256-GCM-SHA384:AES128-GCM-SHA256"
    ":!aNULL:!eNULL:!aDSS:!SHA1:!AESCCM"
)
"""The default ciphers of Python, plus the two AES-GCM ones with an RSA key exchange.

The server only negotiates TLS 1.2 with `AES256-GCM-SHA384`, which the default excludes.
"""

TASK_EXTRACTION: Final = 46
TASK_POST_PROCESSING: Final = 48
TASK_RUNNING: Final = 1
"""The `STATUS` of a task that is running."""

STATUS_SUCCESS: Final = 2
STATUS_FAILED: Final = 3
STATUS_INTERRUPTED: Final = 5

STATUS_DESCRIPTIONS: Final = {
    0: "aguardando execução",
    1: "em execução",
    2: "concluída com sucesso",
    3: "concluída com erro",
    4: "interrupção solicitada",
    5: "interrompida",
}
"""The `STATUS` of `NFSE_OWNER.TAREFAS`, from `eTarefaStatus` of the application."""

TASK_KINDS: Final = {
    TASK_EXTRACTION: "extração de documentos fiscais nacionais",
    TASK_POST_PROCESSING: "pós-processamento das notas nacionais",
}

CONFLICT_ATTEMPTS: Final = 3
"""The calls when the service says `Conflict` but no extraction shows in the table."""

LABEL: Final = "Extração XML"
SECONDS_PER_HOUR: Final = 3600
SECONDS_PER_MINUTE: Final = 60
SUMMARY_LIMIT: Final = 500
