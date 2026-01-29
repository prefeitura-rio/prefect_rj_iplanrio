# -*- coding: utf-8 -*-
"""
Cliente para COR OnCall API (versao para Prefect)
"""

import os
from datetime import datetime
from typing import List, Dict, Any

import httpx

from iplanrio.pipelines_utils.logging import log


# Mapeamento de tipos de alerta para codigo COR
ALERT_TYPE_MAPPING = {
    "alagamento": "ALAGAMENTO",
    "enchente": "ENCHENTE",
    "bolsao": "BOLSAO_DAGUA",
}

# Mapeamento de severidade para prioridade COR
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

        if not self.username or not self.password:
            raise ValueError("Credenciais COR API nao configuradas")

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

            if not data.get("access_token"):
                raise Exception("Token de acesso nao retornado")

            return data["access_token"]

    def submit_aggregated_alert(
        self,
        aggregation_group_id: str,
        alert_type: str,
        severity: str,
        description: str,
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
            alert_type: Tipo do alerta (alagamento, enchente, bolsao)
            severity: Severidade (alta, critica)
            description: Descricao agregada
            address: Endereco representativo
            latitude: Latitude do centroide
            longitude: Longitude do centroide
            alert_count: Numero de alertas no grupo
            alert_ids: Lista de IDs originais

        Returns:
            Resultado da API
        """
        if not self.base_url:
            log("COR API nao configurada - simulando envio")
            return {"success": True, "simulated": True}

        # Obtem token se necessario
        if not self._access_token:
            self._access_token = self._authenticate()

        # Monta payload
        payload = {
            "EventId": aggregation_group_id,
            "Location": address,
            "Priority": SEVERITY_PRIORITY_MAPPING.get(severity, "02"),
            "AgencyEventTypeCode": ALERT_TYPE_MAPPING.get(
                alert_type, alert_type.upper()
            ),
            "CreatedDate": datetime.now().strftime("%Y-%m-%d %H:%M:%Sh"),
            "Latitude": latitude,
            "Longitude": longitude,
            # Campos customizados para agregacao
            "AlertCount": alert_count,
            "OriginalAlertIds": ",".join(alert_ids[:10]),  # Limita a 10 IDs
            "Description": description[:500],  # Limita descricao
        }

        events_url = f"{self.base_url}/hxgnEvents/api/Events/OpenedEvents"

        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                events_url,
                json=payload,
                params={"Token": self._access_token},
            )

            if response.status_code == 401:
                # Token expirado, tenta renovar
                log("Token expirado, renovando...")
                self._access_token = self._authenticate()
                response = client.post(
                    events_url,
                    json=payload,
                    params={"Token": self._access_token},
                )

            if response.status_code != 200:
                raise Exception(
                    f"Erro ao enviar alerta: {response.status_code} - {response.text}"
                )

            log(f"Alerta {aggregation_group_id} enviado com sucesso para COR API")

            return {
                "success": True,
                "status_code": response.status_code,
                "response": response.json() if response.text else None,
            }
