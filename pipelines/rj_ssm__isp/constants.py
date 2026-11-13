"""Constantes do pipeline rj_ssm__isp."""

from typing import Final
from zoneinfo import ZoneInfo

# Timezone
SP_TZ: Final = ZoneInfo("America/Sao_Paulo")

# Diretório base para arquivos temporários
TMP_BASE: Final = "/tmp/rj_ssm__isp"

# Fases de disponibilidade dos dados
FASE_LABEL: dict[str, str] = {
    "parcial": "parcial",
    "consolidados": "consolidado",
    "errata": "errata",
}

# Timeouts HTTP (segundos)
AUTH_TIMEOUT: Final = 30.0
DOMAIN_TIMEOUT: Final = 30.0
COUNT_TIMEOUT: Final = 30.0
QUERY_TIMEOUT: Final = 60.0

# Código IBGE do município padrão (Rio de Janeiro).
MUNICIPIO_RIO_DE_JANEIRO: Final = 3304557

# Paginação da API de features.
DEFAULT_PAGE_SIZE: Final = 2000
MAX_CONCURRENT_REQUESTS: Final = 5

MESES_ABREV: Final[dict[int, str]] = {
    1: "jan",
    2: "fev",
    3: "mar",
    4: "abr",
    5: "mai",
    6: "jun",
    7: "jul",
    8: "ago",
    9: "set",
    10: "out",
    11: "nov",
    12: "dez",
}

# Período do dia a partir do código bruto do campo ffaixa (domínio da camada:
# 1: 0h às 5h59, 2: 6h às 11h59, 3: 12h às 17h59, 4: 18h às 23h59).
PERIODO_DIA_POR_FFAIXA: Final[dict[int, str]] = {
    1: "madrugada",
    2: "manhã",
    3: "tarde",
    4: "noite",
}

# "Título do DO" selecionados por padrão no filtro (conforme configuração
# atual do widget no ISP-GEO). São resolvidos para códigos numéricos em
# tempo de execução contra o domínio real da camada.
DEFAULT_CRIME_TITLES: Final[list[str]] = [
    "Lesao corporal dolosa",
    "Estupro",
    "Homicidio culposo de transito",
    "Lesao corporal culposa de transito",
    "Encontro de cadaver",
    "Encontro de ossada",
    "Roubo a estabelecimento comercial",
    "Roubo a residencia",
    "Roubo de veiculo",
    "Roubo de carga",
    "Roubo a transeunte",
    "Roubo em coletivo",
    "Roubo a banco",
    "Roubo de caixa eletronico",
    "Roubo de aparelho celular",
    "Roubo com conducao da vitima para saque em instituicao financeira",
    "Furto de veiculos",
    "Extorsao mediante sequestro (sequestro classico)",
    "Extorsao",
    "Extorsao com momentanea privacao da liberdade (sequestro relampago)",
    "Estelionato",
    "Apreensao de drogas",
    "Recuperacao de veiculo",
    "Cumprimento de mandado de prisao",
    "Ameaca",
    "Pessoas desaparecidas",
    "Morte por intervencao de agente do Estado",
    "Roubo apos saque em instituicao financeira",
    "Roubo no interior de estabelecimento industrial",
    "Roubo a turista",
    "Roubo de bicicleta",
    "Furto de telefone celular",
    "Furto de carga",
    "Furto a transeunte",
    "Furto em coletivo",
    "Furto a turista",
    "Furto de bicicleta",
]

# Campos com data (epoch ms -> YYYY-MM-DD).
DATE_FIELDS: Final[list[str]] = ["datc", "datf"]

# Todos os campos da camada (48, sem contar "shape"), na ordem original da API,
# com as colunas derivadas inseridas logo após o campo de origem. Nomes em
# snake_case (sem espaço/acento) para poderem ser usados direto como nome de
# coluna no BigQuery.
CSV_COLUMNS: Final[list[tuple[str, str]]] = [
    ("fase_particao", "fase_particao"),
    ("objectid", "objectid"),
    ("distancia_focoespecial", "distancia_area_foco_especial"),
    ("target_fid", "target_fid"),
    ("chave", "chave"),
    ("rgocronu", "RO"),
    ("etit", "titulo_delito"),
    ("eseq", "sequencial_envolvido"),
    ("delito_do", "titulo_do"),
    ("sim", "indicador_estrategico"),
    ("total_rbft", "total_rbft"),
    ("fase", "fase_divulgacao"),
    ("ano", "ano_registro"),
    ("mes", "mes_registro"),
    ("datc", "data_registro"),
    ("datf", "data_fato"),
    ("ano_fato", "ano_fato"),
    ("id_mes_fato", "id_mes_fato"),
    ("mes_fato", "mes_fato"),
    ("horf", "hora_fato"),
    ("hora_faixa", "hora_fato_faixa"),
    ("fhora", "hora_fato_sem_minutos"),
    ("fdiasem", "dia_semana_fato"),
    ("id_dia_semana_fato", "id_dia_semana_fato"),
    ("dia_semana_fato", "dia_semana_fato_abrev"),
    ("cisp", "cisp"),
    ("aisp", "aisp"),
    ("risp", "risp"),
    ("ftlc", "tipo_local_fato"),
    ("fmun_cod", "municipio_fato_ibge"),
    ("municipio", "municipio_fato"),
    ("fcom", "complemento_endereco"),
    ("localidade", "localidade"),
    ("ftlo_recode", "tipo_logradouro"),
    ("flog_recode", "nome_logradouro"),
    ("locf_recode", "logradouro_completo"),
    ("fnum_recode", "num_porta"),
    ("fref_recode", "ponto_referencia"),
    ("fbai_recode", "bairro"),
    ("intersecao", "intersecao_esquina"),
    ("km", "km"),
    ("lat", "latitude"),
    ("long", "longitude"),
    ("endereco", "logradouro_numerica"),
    ("endereco_sem_tipo", "endereco_sem_tipo"),
    ("esquina", "esquina"),
    ("celula", "celula_ibge"),
    ("class_geocode", "class_geocode"),
    ("uuid", "uuid"),
    ("point_x", "point_x"),
    ("point_y", "point_y"),
    ("wkt", "wkt"),
    ("geography", "geography"),
    ("id_territorio", "id_territorio"),
    ("nome_focoespecial", "nome_area_foco_especial"),
    ("dominio_focoespecial", "dominio_focoespecial"),
    ("ffaixa", "faixa_horaria"),
    ("periodo_dia", "periodo_dia"),
]
