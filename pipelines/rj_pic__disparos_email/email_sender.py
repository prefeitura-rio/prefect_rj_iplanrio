"""Módulo de envio de e-mails via SMTP."""
import smtplib
import time
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from config import (
    SMTP_SERVER,
    SMTP_PORT,
    SMTP_USERNAME,
    SMTP_PASSWORD,
    EMAIL_FROM,
)


MAX_RETRIES=3
RETRY_DELAY=2.0
THROTTLE_DELAY=2.0

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Terminal
    ]
)

success_logger = logging.getLogger('success')
success_logger.setLevel(logging.INFO)
success_logger.propagate = False


class EmailSender:
    """Classe para envio de e-mails com retry e throttling."""
    
    def __init__(self):
        """Inicializa o sender de e-mails."""
        self.last_send_time = 0
        self._validate_credentials()
    
    def _validate_credentials(self):
        """Valida se as credenciais estão configuradas."""
        if not SMTP_USERNAME or not SMTP_PASSWORD:
            raise ValueError(
                "Credenciais SMTP não configuradas. "
                "Configure SMTP_USERNAME e SMTP_PASSWORD no arquivo .env"
            )
    
    def _throttle(self):
        """Aplica throttling entre envios."""
        current_time = time.time()
        time_since_last_send = current_time - self.last_send_time
        
        if time_since_last_send < THROTTLE_DELAY:
            sleep_time = THROTTLE_DELAY - time_since_last_send
            time.sleep(sleep_time)
        
        self.last_send_time = time.time()
    
    def _create_message(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        from_email: Optional[str] = None
    ) -> MIMEMultipart:
        """
        Cria a mensagem de e-mail.
        
        Args:
            to_email: E-mail do destinatário
            subject: Assunto do e-mail
            html_body: Corpo HTML do e-mail
            from_email: E-mail do remetente (opcional)
        
        Returns:
            Mensagem MIME pronta para envio
        """
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = from_email or EMAIL_FROM
        msg['To'] = to_email
        
        # Adiciona corpo HTML
        html_part = MIMEText(html_body, 'html', 'utf-8')
        msg.attach(html_part)
        
        return msg
    
    def _send_email_internal(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        from_email: Optional[str] = None
    ) -> bool:
        """
        Envia um e-mail (sem retry).
        
        Args:
            to_email: E-mail do destinatário
            subject: Assunto do e-mail
            html_body: Corpo HTML do e-mail
            from_email: E-mail do remetente (opcional)
        
        Returns:
            True se enviado com sucesso, False caso contrário
        """
        try:
            # Aplica throttling
            self._throttle()
            
            # Cria mensagem
            msg = self._create_message(to_email, subject, html_body, from_email)
            
            # Conecta e envia
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.send_message(msg)
            
            return True
            
        except smtplib.SMTPAuthenticationError as e:
            logging.error(f"Erro de autenticação para {to_email}: {e}")
            raise
        except smtplib.SMTPRecipientsRefused as e:
            logging.error(f"Destinatário recusado {to_email}: {e}")
            raise
        except smtplib.SMTPServerDisconnected as e:
            logging.error(f"Servidor desconectado ao enviar para {to_email}: {e}")
            raise
        except smtplib.SMTPException as e:
            logging.error(f"Erro SMTP ao enviar para {to_email}: {e}")
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
        recipient_name: Optional[str] = None
    ) -> bool:
        """
        Envia um e-mail com retry automático.
        
        Args:
            to_email: E-mail do destinatário
            subject: Assunto do e-mail
            html_body: Corpo HTML do e-mail
            from_email: E-mail do remetente (opcional)
            recipient_name: Nome do destinatário (para logs)
        
        Returns:
            True se enviado com sucesso, False caso contrário
        """
        display_name = recipient_name or to_email
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._send_email_internal(to_email, subject, html_body, from_email)
                
                # Sucesso
                success_msg = f"E-mail enviado com sucesso para {display_name} ({to_email})"
                logging.info(success_msg)
                success_logger.info(f"{to_email} | {display_name}")
                return True
                
            except (smtplib.SMTPAuthenticationError, smtplib.SMTPRecipientsRefused) as e:
                # Erros que não devem ser retentados
                error_msg = f"Falha permanente ao enviar para {display_name} ({to_email}): {e}"
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

