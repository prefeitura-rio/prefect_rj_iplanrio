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
                "lat": -22.9711,
                "lng": -43.1822,
                "address": "Avenida Atlantica, 100, Copacabana",
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
                "lat": -22.9711,
                "lng": -43.1822,
                "address": "Av. Atlantica, 100, Copacabana",
                "description": "Rio transbordando, agua na rua",
            },
            {
                "alert_type": "enchente",
                "severity": "alta",
                "lat": -22.9693,
                "lng": -43.1822,  # ~200m ao norte
                "address": "Av. Atlantica, 500, Copacabana",
                "description": "Enchente forte, transito parado",
            },
            {
                "alert_type": "enchente",
                "severity": "critica",
                "lat": -22.9685,
                "lng": -43.1800,  # ~300m nordeste
                "address": "Rua Barata Ribeiro, 200, Copacabana",
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
                "lat": -22.9838,
                "lng": -43.2096,
                "address": "Rua Visconde de Piraja, 100, Ipanema",
                "description": "Agua na altura do joelho, carros parados",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9820,
                "lng": -43.2096,
                "address": "Rua Visconde de Piraja, 300, Ipanema",
                "description": "Rua completamente alagada, nao consigo sair de casa",
            },
            {
                "alert_type": "alagamento",
                "severity": "critica",
                "lat": -22.9850,
                "lng": -43.2080,
                "address": "Rua Garcia D'Avila, 50, Ipanema",
                "description": "Bueiro entupido, agua subindo rapido",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9825,
                "lng": -43.2110,
                "address": "Rua Prudente de Morais, 200, Ipanema",
                "description": "Bolsao de agua na pista, transito parado",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9845,
                "lng": -43.2070,
                "address": "Rua Farme de Amoedo, 80, Ipanema",
                "description": "Rio transbordou, rua inundada",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9855,
                "lng": -43.2090,
                "address": "Rua Barao da Torre, 150, Ipanema",
                "description": "Enchente levando carros, situacao critica",
            },
        ],
    },
    "mixed": {
        "name": "Mixed - Teste Abrangente",
        "description": "Multiplos clusters independentes de tipos diferentes",
        "alerts": [
            # Cluster 1: 2 enchentes em Copacabana (aguarda 7 min)
            {
                "alert_type": "enchente",
                "severity": "alta",
                "lat": -22.9711,
                "lng": -43.1822,
                "address": "Av. Atlantica, Copacabana",
                "description": "Rio transbordando na orla",
            },
            {
                "alert_type": "enchente",
                "severity": "critica",
                "lat": -22.9693,
                "lng": -43.1822,
                "address": "Rua Santa Clara, Copacabana",
                "description": "Enchente severa, moradores ilhados",
            },
            # Cluster 2: 5 alagamentos em Ipanema (envia imediato)
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9838,
                "lng": -43.2096,
                "address": "Rua Visconde de Piraja, Ipanema",
                "description": "Alagamento extenso na via principal",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9820,
                "lng": -43.2096,
                "address": "Rua Paul Redfern, Ipanema",
                "description": "Agua acumulada, impossivel passar",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9850,
                "lng": -43.2080,
                "address": "Rua Garcia D'Avila, Ipanema",
                "description": "Bueiros transbordando",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9825,
                "lng": -43.2110,
                "address": "Rua Prudente de Morais, Ipanema",
                "description": "Ponto de alagamento cronico",
            },
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9845,
                "lng": -43.2070,
                "address": "Rua Farme de Amoedo, Ipanema",
                "description": "Comercio alagado, prejuizos",
            },
            # Cluster 3: 1 bolsao no Centro (aguarda 7 min)
            {
                "alert_type": "bolsao",
                "severity": "alta",
                "lat": -22.9035,
                "lng": -43.2096,
                "address": "Avenida Presidente Vargas, Centro",
                "description": "Bolsao de agua sob viaduto, pista bloqueada",
            },
        ],
    },
    "edge_cases": {
        "name": "Edge Cases - Testes de Casos Limites",
        "description": "Alertas em situacoes limite para testar robustez",
        "alerts": [
            # Alertas exatamente no limite de 500m (devem formar clusters separados)
            {
                "alert_type": "enchente",
                "severity": "alta",
                "lat": -22.9711,
                "lng": -43.1822,
                "address": "Ponto A - Copacabana",
                "description": "Alerta no ponto A",
            },
            {
                "alert_type": "enchente",
                "severity": "alta",
                "lat": -22.9756,  # ~500m ao sul
                "lng": -43.1822,
                "address": "Ponto B - 500m de A",
                "description": "Alerta exatamente a 500m do ponto A",
            },
            # Severidades mistas no mesmo cluster
            {
                "alert_type": "alagamento",
                "severity": "alta",
                "lat": -22.9324,
                "lng": -43.2462,
                "address": "Tijuca - ponto 1",
                "description": "Alagamento severidade alta",
            },
            {
                "alert_type": "alagamento",
                "severity": "critica",
                "lat": -22.9330,
                "lng": -43.2462,
                "address": "Tijuca - ponto 2",
                "description": "Alagamento severidade critica",
            },
            # Alerta com descricao longa
            {
                "alert_type": "bolsao",
                "severity": "alta",
                "lat": -22.9100,
                "lng": -43.1800,
                "address": "Rua Larga, Centro",
                "description": (
                    "Bolsao de agua muito grande formado apos chuva intensa. "
                    "A agua esta acumulada ha mais de 2 horas e nao consegue escoar. "
                    "Veiculos estao passando com dificuldade e alguns ja ficaram parados. "
                    "Pedestres precisam desviar pela calcada do outro lado."
                ),
            },
        ],
    },
}
