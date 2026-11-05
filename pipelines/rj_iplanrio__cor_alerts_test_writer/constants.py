# -*- coding: utf-8 -*-
"""
Constantes para pipeline de geracao de dados de teste COR
"""

from enum import Enum


class CORTestWriterConstants(Enum):
    """Constantes para o gerador de dados de teste"""

    # BigQuery (mesma tabela que a pipeline processadora usa)
    DATASET_ID = "brutos_eai_logs"
    QUEUE_TABLE_ID = "cor_alerts_queue"
    BILLING_PROJECT_ID = "rj-iplanrio"

    # User ID para dados de teste
    TEST_USER_ID = "test_user_001"

    # Prefixo para identificar alertas de teste
    TEST_ALERT_PREFIX = "TEST_"

    # Ambientes validos
    VALID_ENVIRONMENTS = ["staging", "prod"]

    # Diretorio temporario para arquivos CSV
    ROOT_FOLDER = "./data_cor_alerts_test/"


# Definicao de cenarios de teste
TEST_SCENARIOS = {
    "single": {
        "name": "Single Alert - Teste de Janela de Tempo",
        "description": "1 alerta isolado - deve enviar apos 7 minutos",
        "alerts": [
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8668,
                "lng": -43.2913,
                "address": "Rua Principal do Acari, 100, Acari",
                "bairro_raw": "Acari",
                "bairro_normalizado": "acari",
                "description": "Rua alagada na altura do numero 100, agua subindo",
            }
        ],
    },
    "small_cluster": {
        "name": "Small Cluster - Teste de Agregacao",
        "description": "3 alertas dentro de 500m - agrupa em 1 cluster, envia apos 7 min",
        "alerts": [
            {
                "alert_type": "enchente",
                "severity": "alta",
                "lat": -22.8668,
                "lng": -43.2913,
                "address": "Rua Principal do Acari, 100, Acari",
                "bairro_raw": "Acari",
                "bairro_normalizado": "acari",
                "description": "Rio transbordando, agua na rua",
            },
            {
                "alert_type": "enchente",
                "severity": "alta",
                "lat": -22.8650,
                "lng": -43.2913,  # ~200m ao norte
                "address": "Rua do Acari, 500, Acari",
                "bairro_raw": "Acari",
                "bairro_normalizado": "acari",
                "description": "Enchente forte, transito parado",
            },
            {
                "alert_type": "enchente",
                "severity": "critica",
                "lat": -22.8642,
                "lng": -43.2900,  # ~300m nordeste
                "address": "Rua Santa Rosa, 200, Acari",
                "bairro_raw": "Acari",
                "bairro_normalizado": "acari",
                "description": "Situacao critica, carros ilhados",
            },
        ],
    },
    "large_cluster": {
        "name": "Large Cluster - Teste de Threshold Imediato",
        "description": "6 alertas dentro de 500m - envia imediatamente (>= 5 alertas)",
        "alerts": [
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8932,
                "lng": -43.5512,
                "address": "Rua Principal de Guaratiba, 100, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": "Agua na altura do joelho, carros parados",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8920,
                "lng": -43.5512,
                "address": "Rua das Flores, 300, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": "Rua completamente alagada, nao consigo sair de casa",
            },
            {
                "alert_type": "alagamento",
                "severity": "critica",
                "lat": -22.8945,
                "lng": -43.5500,
                "address": "Estrada da Pedra, 50, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": "Bueiro entupido, agua subindo rapido",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8928,
                "lng": -43.5520,
                "address": "Rua do Comercio, 200, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": "Bolsao de agua na pista, transito parado",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8938,
                "lng": -43.5505,
                "address": "Avenida das Americas, 80, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": "Rio transbordou, rua inundada",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8942,
                "lng": -43.5515,
                "address": "Rua da Praia, 150, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": "Enchente levando carros, situacao critica",
            },
        ],
    },
    "mixed": {
        "name": "Mixed - Teste Abrangente com Filtragem",
        "description": "Multiplos clusters com bairros permitidos E nao-permitidos para testar filtragem",
        "alerts": [
            # Cluster 1: 2 enchentes em Acari (aguarda 7 min) - DEVE SER PROCESSADO
            {
                "alert_type": "enchente",
                "severity": "alta",
                "lat": -22.8668,
                "lng": -43.2913,
                "address": "Rua Principal do Acari, Acari",
                "bairro_raw": "Acari",
                "bairro_normalizado": "acari",
                "description": "Rio transbordando na rua principal",
            },
            {
                "alert_type": "enchente",
                "severity": "critica",
                "lat": -22.8650,
                "lng": -43.2913,
                "address": "Rua Santa Rosa, Acari",
                "bairro_raw": "Acari",
                "bairro_normalizado": "acari",
                "description": "Enchente severa, moradores ilhados",
            },
            # Cluster 2: 3 alagamentos em Copacabana (DEVE SER FILTRADO - bairro nao permitido)
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9711,
                "lng": -43.1822,
                "address": "Av. Atlantica, 100, Copacabana",
                "bairro_raw": "Copacabana",
                "bairro_normalizado": "copacabana",
                "description": "Alagamento na orla - NAO deve processar",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9693,
                "lng": -43.1822,
                "address": "Rua Barata Ribeiro, Copacabana",
                "bairro_raw": "Copacabana",
                "bairro_normalizado": "copacabana",
                "description": "Agua na rua - NAO deve processar",
            },
            {
                "alert_type": "alagamento",
                "severity": "critica",
                "lat": -22.9685,
                "lng": -43.1800,
                "address": "Rua Santa Clara, Copacabana",
                "bairro_raw": "Copacabana",
                "bairro_normalizado": "copacabana",
                "description": "Situacao critica - NAO deve processar",
            },
            # Cluster 3: 5 alagamentos em Guaratiba (envia imediato) - DEVE SER PROCESSADO
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8932,
                "lng": -43.5512,
                "address": "Rua Principal de Guaratiba, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": "Alagamento extenso na via principal",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8920,
                "lng": -43.5512,
                "address": "Rua das Flores, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": "Agua acumulada, impossivel passar",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8945,
                "lng": -43.5500,
                "address": "Estrada da Pedra, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": "Bueiros transbordando",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8928,
                "lng": -43.5520,
                "address": "Rua do Comercio, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": "Ponto de alagamento cronico",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8938,
                "lng": -43.5505,
                "address": "Avenida das Americas, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": "Comercio alagado, prejuizos",
            },
            # Cluster 4: 2 alertas em Ipanema (DEVE SER FILTRADO - bairro nao permitido)
            {
                "alert_type": "enchente",
                "severity": "alta",
                "lat": -22.9838,
                "lng": -43.2096,
                "address": "Rua Visconde de Piraja, Ipanema",
                "bairro_raw": "Ipanema",
                "bairro_normalizado": "ipanema",
                "description": "Enchente na rua - NAO deve processar",
            },
            {
                "alert_type": "enchente",
                "severity": "critica",
                "lat": -22.9820,
                "lng": -43.2096,
                "address": "Rua Garcia D'Avila, Ipanema",
                "bairro_raw": "Ipanema",
                "bairro_normalizado": "ipanema",
                "description": "Situacao critica - NAO deve processar",
            },
            # Cluster 5: 1 bolsao em Jardim America (aguarda 7 min) - DEVE SER PROCESSADO
            {
                "alert_type": "bolsao",
                "severity": "alta",
                "lat": -22.8668,
                "lng": -43.3670,
                "address": "Rua Principal do Jardim America, Jardim America",
                "bairro_raw": "Jardim América",
                "bairro_normalizado": "jardim america",
                "description": "Bolsao de agua sob viaduto, pista bloqueada",
            },
        ],
    },
    "neighborhood_filter": {
        "name": "Neighborhood Filter - Teste de Filtragem de Bairros",
        "description": "Testa se apenas bairros permitidos (Acari, Guaratiba, Jardim America) sao processados",
        "alerts": [
            # DEVE SER PROCESSADO - Acari
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8668,
                "lng": -43.2913,
                "address": "Rua do Acari, 100, Acari",
                "bairro_raw": "Acari",
                "bairro_normalizado": "acari",
                "description": "DEVE processar - Acari na whitelist",
            },
            # DEVE SER FILTRADO - Copacabana
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9711,
                "lng": -43.1822,
                "address": "Av. Atlantica, Copacabana",
                "bairro_raw": "Copacabana",
                "bairro_normalizado": "copacabana",
                "description": "NAO deve processar - Copacabana fora da whitelist",
            },
            # DEVE SER PROCESSADO - Guaratiba
            {
                "alert_type": "enchente",
                "severity": "critica",
                "lat": -22.8932,
                "lng": -43.5512,
                "address": "Rua de Guaratiba, 200, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": "DEVE processar - Guaratiba na whitelist",
            },
            # DEVE SER FILTRADO - Leblon
            {
                "alert_type": "bolsao",
                "severity": "alta",
                "lat": -22.9843,
                "lng": -43.2255,
                "address": "Av. Ataulfo de Paiva, Leblon",
                "bairro_raw": "Leblon",
                "bairro_normalizado": "leblon",
                "description": "NAO deve processar - Leblon fora da whitelist",
            },
            # DEVE SER PROCESSADO - Jardim America
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8668,
                "lng": -43.3670,
                "address": "Rua do Jardim America, 50, Jardim America",
                "bairro_raw": "Jardim América",
                "bairro_normalizado": "jardim america",
                "description": "DEVE processar - Jardim America na whitelist",
            },
            # DEVE SER FILTRADO - Centro
            {
                "alert_type": "enchente",
                "severity": "critica",
                "lat": -22.9035,
                "lng": -43.2096,
                "address": "Av. Presidente Vargas, Centro",
                "bairro_raw": "Centro",
                "bairro_normalizado": "centro",
                "description": "NAO deve processar - Centro fora da whitelist",
            },
            # DEVE SER FILTRADO - Tijuca
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9324,
                "lng": -43.2462,
                "address": "Rua Conde de Bonfim, Tijuca",
                "bairro_raw": "Tijuca",
                "bairro_normalizado": "tijuca",
                "description": "NAO deve processar - Tijuca fora da whitelist",
            },
            # DEVE SER FILTRADO - Barra da Tijuca
            {
                "alert_type": "bolsao",
                "severity": "alta",
                "lat": -23.0099,
                "lng": -43.3644,
                "address": "Av. das Americas, Barra da Tijuca",
                "bairro_raw": "Barra da Tijuca",
                "bairro_normalizado": "barra da tijuca",
                "description": "NAO deve processar - Barra fora da whitelist",
            },
        ],
    },
    "edge_cases": {
        "name": "Edge Cases - Testes de Casos Limites",
        "description": "Alertas em situacoes limite para testar robustez (com e sem filtragem)",
        "alerts": [
            # Alertas exatamente no limite de 500m em Acari (DEVE PROCESSAR - whitelist)
            {
                "alert_type": "enchente",
                "severity": "alta",
                "lat": -22.8668,
                "lng": -43.2913,
                "address": "Ponto A - Acari",
                "bairro_raw": "Acari",
                "bairro_normalizado": "acari",
                "description": "Alerta no ponto A",
            },
            {
                "alert_type": "enchente",
                "severity": "alta",
                "lat": -22.8713,  # ~500m ao sul
                "lng": -43.2913,
                "address": "Ponto B - 500m de A, Acari",
                "bairro_raw": "Acari",
                "bairro_normalizado": "acari",
                "description": "Alerta exatamente a 500m do ponto A",
            },
            # Severidades mistas no mesmo cluster - Jardim America (DEVE PROCESSAR - whitelist)
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.8668,
                "lng": -43.3670,
                "address": "Jardim America - ponto 1",
                "bairro_raw": "Jardim América",
                "bairro_normalizado": "jardim america",
                "description": "Alagamento severidade alta",
            },
            {
                "alert_type": "alagamento",
                "severity": "critica",
                "lat": -22.8674,
                "lng": -43.3670,
                "address": "Jardim America - ponto 2",
                "bairro_raw": "Jardim América",
                "bairro_normalizado": "jardim america",
                "description": "Alagamento severidade critica",
            },
            # Alerta com descricao longa - Guaratiba (DEVE PROCESSAR - whitelist)
            {
                "alert_type": "bolsao",
                "severity": "alta",
                "lat": -22.8932,
                "lng": -43.5512,
                "address": "Rua Larga, Guaratiba",
                "bairro_raw": "Guaratiba",
                "bairro_normalizado": "guaratiba",
                "description": (
                    "Bolsao de agua muito grande formado apos chuva intensa. "
                    "A agua esta acumulada ha mais de 2 horas e nao consegue escoar. "
                    "Veiculos estao passando com dificuldade e alguns ja ficaram parados. "
                    "Pedestres precisam desviar pela calcada do outro lado."
                ),
            },
            # Alerta sem bairro (campo vazio) - DEVE SER FILTRADO
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9711,
                "lng": -43.1822,
                "address": "Endereco sem bairro identificado",
                "bairro_raw": "",
                "bairro_normalizado": "",
                "description": "NAO deve processar - sem bairro identificado",
            },
            # Alerta em Botafogo - DEVE SER FILTRADO (fora da whitelist)
            {
                "alert_type": "enchente",
                "severity": "critica",
                "lat": -22.9519,
                "lng": -43.1828,
                "address": "Rua Voluntarios da Patria, Botafogo",
                "bairro_raw": "Botafogo",
                "bairro_normalizado": "botafogo",
                "description": "NAO deve processar - Botafogo fora da whitelist",
            },
        ],
    },
}
