# -*- coding: utf-8 -*-
"""Módulo de envio de e-mails via Data Relay API."""
from pathlib import Path
from datetime import datetime

import requests
import time
import logging
from jinja2 import Environment, FileSystemLoader, TemplateNotFound
from typing import Optional, List

from pipelines.rj_pic__disparos_email.env import (
    DATA_RELAY_URL,
    DATA_RELAY_API_KEY,
    DATA_RELAY_FROM_ADDRESS,
    DATA_RELAY_USE_GMAIL_API,
    DATA_RELAY_IS_HTML_BODY,
    MAX_RETRIES,
    RETRY_DELAY,
    THROTTLE_DELAY,
)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],  # Terminal
)

success_logger = logging.getLogger("success")
success_logger.setLevel(logging.INFO)
success_logger.propagate = False


class EmailSender:
    """Classe para envio de e-mails via Data Relay API com retry e throttling."""

    def __init__(self):
        """Inicializa o sender de e-mails."""
        self.last_send_time = 0
        self._validate_credentials()

    def _validate_credentials(self):
        """Valida se as credenciais estão configuradas."""
        if not DATA_RELAY_API_KEY:
            raise ValueError(
                "Credenciais Data Relay não configuradas. "
                "Configure PIC_DISPAROS_EMAIL_DATA_RELAY_API_KEY no arquivo .env"
            )
        if not DATA_RELAY_URL:
            raise ValueError(
                "URL do Data Relay não configurada. "
                "Configure PIC_DISPAROS_EMAIL_DATA_RELAY_URL no arquivo .env"
            )

    def _throttle(self):
        """Aplica throttling entre envios."""
        current_time = time.time()
        time_since_last_send = current_time - self.last_send_time

        if time_since_last_send < THROTTLE_DELAY:
            sleep_time = THROTTLE_DELAY - time_since_last_send
            time.sleep(sleep_time)

        self.last_send_time = time.time()

    def _send_email_internal(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        from_email: Optional[str] = None,
        cc_addresses: Optional[List[str]] = None,
        bcc_addresses: Optional[List[str]] = None,
        reply_to: Optional[List[str]] = None,
    ) -> bool:
        """
        Envia um e-mail via Data Relay API (sem retry).

        Args:
            to_email: E-mail do destinatário
            subject: Assunto do e-mail
            html_body: Corpo HTML do e-mail
            from_email: E-mail do remetente (opcional)
            cc_addresses: Lista de e-mails para CC (opcional)
            bcc_addresses: Lista de e-mails para BCC (opcional)
            reply_to: Lista de e-mails para reply-to (opcional)

        Returns:
            True se enviado com sucesso, False caso contrário
        """
        try:
            # Aplica throttling
            self._throttle()

            # Prepara o payload para a API do Data Relay
            payload = {
                "to_addresses": [to_email],
                "subject": subject,
                "body": html_body,
                "is_html_body": DATA_RELAY_IS_HTML_BODY,
                "use_gmail_api": DATA_RELAY_USE_GMAIL_API,
            }

            # Adiciona campos opcionais se fornecidos
            if from_email or DATA_RELAY_FROM_ADDRESS:
                payload["from_address"] = from_email or DATA_RELAY_FROM_ADDRESS

            if cc_addresses:
                payload["cc_addresses"] = cc_addresses

            if bcc_addresses:
                payload["bcc_addresses"] = bcc_addresses

            if reply_to:
                payload["reply_to"] = reply_to

            # Configura headers
            headers = {
                "accept": "application/json",
                "x-api-key": DATA_RELAY_API_KEY,
                "Content-Type": "application/json",
            }

            # Envia requisição para a API
            response = requests.post(
                DATA_RELAY_URL,
                headers=headers,
                json=payload,
                timeout=30,
            )

            # Verifica se foi bem sucedido
            response.raise_for_status()

            return True

        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [400, 401, 403]:
                # Erros de autenticação ou validação - não devem ser retentados
                logging.error(f"Erro de autenticação/validação para {to_email}: {e}")
                logging.error(f"Response: {e.response.text}")
                raise
            else:
                # Outros erros HTTP
                logging.error(f"Erro HTTP ao enviar para {to_email}: {e}")
                logging.error(f"Response: {e.response.text}")
                raise

        except requests.exceptions.Timeout as e:
            logging.error(f"Timeout ao enviar para {to_email}: {e}")
            raise

        except requests.exceptions.RequestException as e:
            logging.error(f"Erro de requisição ao enviar para {to_email}: {e}")
            raise

        except Exception as e:
            logging.error(f"Erro inesperado ao enviar para {to_email}: {e}")
            raise

    def send_email(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        from_email: Optional[str] = None,
        recipient_name: Optional[str] = None,
        cc_addresses: Optional[List[str]] = None,
        bcc_addresses: Optional[List[str]] = None,
        reply_to: Optional[List[str]] = None,
    ) -> bool:
        """
        Envia um e-mail com retry automático via Data Relay API.

        Args:
            to_email: E-mail do destinatário
            subject: Assunto do e-mail
            html_body: Corpo HTML do e-mail
            from_email: E-mail do remetente (opcional)
            recipient_name: Nome do destinatário (para logs)
            cc_addresses: Lista de e-mails para CC (opcional)
            bcc_addresses: Lista de e-mails para BCC (opcional)
            reply_to: Lista de e-mails para reply-to (opcional)

        Returns:
            True se enviado com sucesso, False caso contrário
        """
        display_name = recipient_name or to_email

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._send_email_internal(
                    to_email=to_email,
                    subject=subject,
                    html_body=html_body,
                    from_email=from_email,
                    cc_addresses=cc_addresses,
                    bcc_addresses=bcc_addresses,
                    reply_to=reply_to,
                )

                # Sucesso
                success_msg = (
                    f"E-mail enviado com sucesso para {display_name} ({to_email})"
                )
                logging.info(success_msg)
                success_logger.info(f"{to_email} | {display_name}")
                return True

            except requests.exceptions.HTTPError as e:
                if e.response and e.response.status_code in [400, 401, 403]:
                    # Erros que não devem ser retentados
                    error_msg = (
                        f"Falha permanente ao enviar para {display_name} ({to_email}): {e}"
                    )
                    logging.error(error_msg)
                    return False
                # Outros erros HTTP podem ser retentados
                if attempt < MAX_RETRIES:
                    error_msg = (
                        f"Tentativa {attempt}/{MAX_RETRIES} falhou para {display_name} "
                        f"({to_email}): {e}. Tentando novamente em {RETRY_DELAY}s..."
                    )
                    logging.warning(error_msg)
                    time.sleep(RETRY_DELAY)
                else:
                    error_msg = (
                        f"Falha ao enviar para {display_name} ({to_email}) após "
                        f"{MAX_RETRIES} tentativas: {e}"
                    )
                    logging.error(error_msg)
                    return False

            except Exception as e:
                # Erros temporários - tenta novamente
                if attempt < MAX_RETRIES:
                    error_msg = (
                        f"Tentativa {attempt}/{MAX_RETRIES} falhou para {display_name} "
                        f"({to_email}): {e}. Tentando novamente em {RETRY_DELAY}s..."
                    )
                    logging.warning(error_msg)
                    time.sleep(RETRY_DELAY)
                else:
                    error_msg = (
                        f"Falha ao enviar para {display_name} ({to_email}) após "
                        f"{MAX_RETRIES} tentativas: {e}"
                    )
                    logging.error(error_msg)
                    return False

        return False


class TemplateEngine:
    """Processa templates HTML substituindo variáveis dinamicamente."""

    def __init__(self, template_path: str):
        """
        Inicializa o processador de templates.

        Args:
            template_path: Caminho para o arquivo de template HTML
        """
        self.template_path = Path(template_path)
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template não encontrado: {template_path}")

        # Configura Jinja2 para carregar templates do diretório
        template_dir = self.template_path.parent
        self.env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=True,  # Escapa HTML automaticamente para segurança
        )

        # Carrega o template
        template_name = self.template_path.name
        try:
            self.template = self.env.get_template(template_name)
        except TemplateNotFound:
            raise FileNotFoundError(f"Template não encontrado: {template_name}")

    def render(self, **variables) -> str:
        """
        Renderiza o template com as variáveis fornecidas.

        Args:
            **variables: Variáveis para substituir no template (ex: nome, data, etc.)

        Returns:
            HTML renderizado como string
        """
        # Adiciona data/hora atual se não fornecida
        if "data" not in variables:
            variables["data"] = datetime.now().strftime("%d/%m/%Y")
        if "hora" not in variables:
            variables["hora"] = datetime.now().strftime("%H:%M:%S")

        return self.template.render(**variables)
