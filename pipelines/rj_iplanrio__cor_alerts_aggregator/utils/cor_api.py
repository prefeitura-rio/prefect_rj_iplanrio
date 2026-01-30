# -*- coding: utf-8 -*-
"""
Cliente para COR OnCall API (versao para Prefect)
"""

import os
from datetime import datetime
from typing import List, Dict, Any

import httpx

from iplanrio.pipelines_utils.logging import log


# Mapeamento de tipos de alerta
ALERT_TYPE_MAPPING = {
    "alagamento": "ALAGAMENTO",
    "enchente": "ENCHENTE",
    "bolsao": "BOLSAO_DAGUA",
}

SEVERITY_PRIORITY_MAPPING = {
    "alta": "02",
    "critica": "01",
}


class COROnCallClient:
    """Cliente para COR OnCall API"""

    def __init__(self, environment: str = "staging"):
        self.environment = environment

        # Credenciais via variaveis de ambiente (configuradas no secret do k8s)
        self.base_url = os.getenv("CHATBOT_COR_EVENTS_API_BASE_URL")
        self.username = os.getenv("CHATBOT_COR_EVENTS_API_USERNAME")
        self.password = os.getenv("CHATBOT_COR_EVENTS_API_PASSWORD")

        self._access_token = None

    def _authenticate(self) -> str:
        """Obtem token de acesso da API COR"""
        if not self.base_url:
            raise ValueError("CHATBOT_COR_EVENTS_API_BASE_URL nao configurado")

        login_url = f"{self.base_url}/hxgnEvents/api/Events/Login"

        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                login_url,
                json={"Username": self.username, "Password": self.password},
            )

            if response.status_code != 200:
                raise Exception(f"Autenticacao falhou: {response.status_code}")

            data = response.json()
            if data.get("Error"):
                raise Exception(f"Erro de autenticacao: {data['Error']}")

            return data["access_token"]

    def submit_aggregated_alert(
        self,
        aggregation_group_id: str,
        alert_type: str,
        severity: str,
        descriptions: List[str],
        address: str,
        latitude: float,
        longitude: float,
        alert_count: int,
        alert_ids: List[str],
    ) -> Dict[str, Any]:
        """
        Envia alerta agregado para COR API.

        Args:
            aggregation_group_id: ID unico do grupo de agregacao
            alert_type: Tipo do alerta
            severity: Severidade (alta, critica)
            descriptions: Lista de descricoes/relatos dos usuarios
            address: Endereco representativo
            latitude: Latitude do centroide
            longitude: Longitude do centroide
            alert_count: Numero de alertas no grupo (usado internamente)
            alert_ids: Lista de IDs originais (usado internamente)

        Returns:
            Resultado da API
        """
        if not self.base_url:
            log("COR API nao configurada - simulando envio")
            return {"success": True, "simulated": True}

        # Obtem token
        if not self._access_token:
            self._access_token = self._authenticate()

        # Construir AgencyEventTypeCode com tipo + relatos
        type_code = ALERT_TYPE_MAPPING.get(alert_type, alert_type.upper())

        if len(descriptions) == 1:
            # Alerta unico: "ALAGAMENTO: <relato>"
            agency_event_type = f"{type_code}: {descriptions[0][:200]}"
        else:
            # Alerta agregado: "ALAGAMENTO: (1) <relato1> | (2) <relato2> | ..."
            relatos_formatados = " | ".join(
                f"({i+1}) {desc[:100]}" for i, desc in enumerate(descriptions[:5])
            )
            if len(descriptions) > 5:
                relatos_formatados += f" | (+{len(descriptions) - 5} relatos)"
            agency_event_type = f"{type_code}: {relatos_formatados}"

        # Limitar tamanho total (API pode ter limite)
        agency_event_type = agency_event_type[:500]

        # Monta payload (apenas campos aceitos pela API COR)
        payload = {
            "EventId": aggregation_group_id,
            "Location": address,
            "Priority": SEVERITY_PRIORITY_MAPPING.get(severity, "02"),
            "AgencyEventTypeCode": agency_event_type,
            "CreatedDate": datetime.now().strftime("%Y-%m-%d %H:%M:%Sh"),
            "Latitude": latitude,
            "Longitude": longitude,
        }

        events_url = f"{self.base_url}/hxgnEvents/api/Events/OpenedEvents"

        def _send_request(token: str) -> httpx.Response:
            """Envia requisicao com o token fornecido."""
            with httpx.Client(timeout=30.0) as client:
                return client.post(
                    events_url,
                    json=payload,
                    params={"Token": token},
                )

        response = _send_request(self._access_token)

        if response.status_code == 401:
            # Token expirado, renova e tenta novamente com novo cliente
            log("Token expirado, renovando...")
            self._access_token = self._authenticate()
            response = _send_request(self._access_token)

        if response.status_code != 200:
            raise Exception(f"Erro ao enviar alerta: {response.status_code} - {response.text}")

        return {
            "success": True,
            "status_code": response.status_code,
            "response": response.json() if response.text else None,
        }
