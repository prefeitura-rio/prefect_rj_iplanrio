# -*- coding: utf-8 -*-
import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from langchain_core.load.dump import dumpd
from langchain_core.messages import BaseMessage

from pipelines.rj_iplanrio__eai_history.md_to_wpp import markdown_to_whatsapp


class LangGraphMessageFormatter:
    """
    Formatador especializado para mensagens do LangGraph Agent Engine.

    Converte mensagens do formato LangChain/LangGraph para o formato padronizado
    do gateway, incluindo funcionalidades como:
    - Session ID determinístico baseado em tempo
    - Timestamps e tempo entre mensagens
    - Serialização automática de objetos BaseMessage
    - Suporte opcional para formato WhatsApp
    - Estatísticas de uso agregadas
    """

    def __init__(self, thread_id: Optional[str] = None):
        self.thread_id = thread_id
        self.reset_state()

    def reset_state(self):
        """Reseta o estado interno do formatador"""
        self.current_session_id = None
        self.current_session_start_time = None
        self.last_message_timestamp = None
        self.tool_call_to_name = {}
        self.current_step_id = f"step-{uuid.uuid4()}"

    def serialize_message(self, message: BaseMessage) -> Dict[str, Any]:
        """
        Serializa mensagem BaseMessage usando dumpd do langchain.

        Args:
            message: Objeto BaseMessage do LangChain

        Returns:
            Dict com dados serializados da mensagem
        """
        raw = dumpd(message)

        # Adicionar usage_metadata se existir no response_metadata
        response_metadata = getattr(message, "response_metadata", None)
        if response_metadata and "usage_metadata" in response_metadata:
            raw["usage_metadata"] = response_metadata["usage_metadata"]

        return raw

    def generate_deterministic_session_id(
        self, timestamp_str: str, thread_id: Optional[str] = None
    ) -> str:
        """
        Gera um session_id determinístico baseado no timestamp e thread_id.

        Args:
            timestamp_str: String do timestamp da primeira mensagem da sessão
            thread_id: ID do thread (opcional)

        Returns:
            Session ID determinístico
        """
        base_string = f"{timestamp_str}_{thread_id or self.thread_id or 'unknown'}"
        hash_object = hashlib.md5(base_string.encode())
        hash_hex = hash_object.hexdigest()
        return f"{hash_hex[:16]}"

    def should_create_new_session(
        self, time_since_last_message: Optional[float], timeout_seconds: Optional[int]
    ) -> bool:
        """
        Determina se deve criar uma nova sessão baseado no tempo desde a última mensagem.

        Args:
            time_since_last_message: Tempo em segundos desde a última mensagem
            timeout_seconds: Timeout da sessão em segundos

        Returns:
            True se deve criar nova sessão, False caso contrário
        """
        if time_since_last_message is None or timeout_seconds is None:
            return False

        return time_since_last_message > timeout_seconds

    def parse_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """
        Converte string de timestamp para datetime object.

        Args:
            timestamp_str: String do timestamp

        Returns:
            Objeto datetime ou None se não for possível parsear
        """
        if not timestamp_str:
            return None
        try:
            # Remove 'Z' se presente e substitui por '+00:00'
            if timestamp_str.endswith("Z"):
                timestamp_str = timestamp_str[:-1] + "+00:00"
            return datetime.fromisoformat(timestamp_str)
        except:
            return None

    def calculate_time_since_last_message(
        self, current_timestamp: Optional[str]
    ) -> Optional[float]:
        """
        Calcula o tempo em segundos desde a última mensagem.

        Args:
            current_timestamp: Timestamp da mensagem atual

        Returns:
            Tempo em segundos ou None se não for possível calcular
        """
        if not current_timestamp or not self.last_message_timestamp:
            return None

        current_dt = self.parse_timestamp(current_timestamp)
        last_dt = self.parse_timestamp(self.last_message_timestamp)

        if current_dt and last_dt:
            return (current_dt - last_dt).total_seconds()

        return None

    def update_session_state(
        self,
        message_timestamp: Optional[str],
        time_since_last_message: Optional[float],
        session_timeout_seconds: Optional[int],
    ):
        """
        Atualiza o estado da sessão baseado no timestamp e timeout.

        Args:
            message_timestamp: Timestamp da mensagem atual
            time_since_last_message: Tempo desde a última mensagem
            session_timeout_seconds: Timeout da sessão
        """
        if session_timeout_seconds is None:
            # Para API: session_id sempre None
            self.current_session_id = None
        else:
            # Para histórico completo: verificar se deve criar nova sessão
            if self.current_session_id is None or self.should_create_new_session(
                time_since_last_message, session_timeout_seconds
            ):
                # Nova sessão: usar o timestamp atual como base para gerar o ID determinístico
                self.current_session_start_time = message_timestamp
                self.current_session_id = self.generate_deterministic_session_id(
                    self.current_session_start_time, self.thread_id
                )

        # Atualizar o último timestamp de mensagem
        if message_timestamp:
            self.last_message_timestamp = message_timestamp

    def extract_message_metadata(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrai metadados da mensagem baseado no tipo.

        Args:
            kwargs: Dados kwargs da mensagem serializada

        Returns:
            Dict com metadados extraídos
        """
        msg_type = kwargs.get("type")
        response_metadata = kwargs.get("response_metadata", {})
        usage_md = response_metadata.get("usage_metadata", {})

        if msg_type in ["human", "tool"]:
            # user_message e tool_return_message: campos null
            return {
                "model_name": None,
                "finish_reason": None,
                "avg_logprobs": None,
                "usage_metadata": None,
            }
        else:
            # assistant_message e tool_call_message: campos com dados
            return {
                "model_name": response_metadata.get("model_name", ""),
                "finish_reason": response_metadata.get("finish_reason", ""),
                "avg_logprobs": response_metadata.get("avg_logprobs"),
                "usage_metadata": {
                    "prompt_token_count": usage_md.get("prompt_token_count", 0),
                    "candidates_token_count": usage_md.get("candidates_token_count", 0),
                    "total_token_count": usage_md.get("total_token_count", 0),
                    "thoughts_token_count": usage_md.get("thoughts_token_count", 0),
                    "cached_content_token_count": usage_md.get(
                        "cached_content_token_count", 0
                    ),
                },
            }

    def create_base_message_dict(
        self,
        kwargs: Dict[str, Any],
        message_timestamp: Optional[str],
        time_since_last_message: Optional[float],
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Cria o dicionário base comum a todos os tipos de mensagem.

        Args:
            kwargs: Dados kwargs da mensagem
            message_timestamp: Timestamp da mensagem
            time_since_last_message: Tempo desde a última mensagem
            metadata: Metadados extraídos

        Returns:
            Dict com campos base da mensagem
        """
        original_id = kwargs.get("id", "").replace("run--", "")

        return {
            "id": original_id or f"message-{uuid.uuid4()}",
            "date": message_timestamp,
            "session_id": self.current_session_id,
            "time_since_last_message": time_since_last_message,
            "name": None,
            "otid": str(uuid.uuid4()),
            "sender_id": None,
            "step_id": self.current_step_id,
            "is_err": None,
            **metadata,
        }

    def process_human_message(
        self, kwargs: Dict[str, Any], base_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Processa mensagem do tipo human."""
        return {
            **base_dict,
            "message_type": "user_message",
            "content": kwargs.get("content", ""),
        }

    def process_ai_message(
        self,
        kwargs: Dict[str, Any],
        base_dict: Dict[str, Any],
        use_whatsapp_format: bool,
    ) -> List[Dict[str, Any]]:
        """Processa mensagem do tipo AI, podendo gerar múltiplas mensagens."""
        messages = []
        raw_content = kwargs.get("content", "")
        tool_calls = kwargs.get("tool_calls", [])

        response_metadata = kwargs.get("response_metadata", {})
        usage_md = response_metadata.get("usage_metadata", {})
        output_details = usage_md.get("output_token_details") or {}
        # reasoning_tokens agora pode vir do metadata OU ser calculado do conteúdo thinking
        reasoning_tokens = output_details.get("reasoning", 0) or 0

        final_content = ""
        thinking_content = ""

        # Processar conteúdo (pode ser string ou lista)
        if isinstance(raw_content, list):
            for item in raw_content:
                if isinstance(item, dict):
                    if item.get("type") == "thinking":
                        thinking_content += item.get("thinking", "")
                    elif item.get("type") == "text":
                        final_content += item.get("text", "")
                elif isinstance(item, str):
                    final_content += item
        elif isinstance(raw_content, str):
            final_content = raw_content

        # Adicionar mensagem de pensamento se houver conteúdo explícito ou tokens de reasoning
        if thinking_content:
            messages.append(
                {
                    **base_dict,
                    "id": f"thinking-{base_dict['id'] or uuid.uuid4()}",
                    "message_type": "reasoning_message",
                    "source": "reasoner_model",
                    "reasoning": thinking_content,
                    "signature": None,
                }
            )
        elif reasoning_tokens > 0:
            # Fallback para reasoning implícito (sem conteúdo textual exposto)
            reasoning_text = "Processando..."
            if tool_calls:
                reasoning_text = f"Processando chamada para ferramenta {tool_calls[0].get('name', 'unknown')}"
            elif final_content:
                reasoning_text = "Processando resposta para o usuário"

            messages.append(
                {
                    **base_dict,
                    "id": f"reasoning-{base_dict['id'] or uuid.uuid4()}",
                    "message_type": "reasoning_message",
                    "source": "reasoner_model",
                    "reasoning": reasoning_text,
                    "signature": None,
                }
            )

        if tool_calls:
            # Construir mapeamento de tool_call_id para nome da ferramenta
            for tc in tool_calls:
                tool_call_id = tc.get("id")
                tool_name = tc.get("name", "unknown")
                if tool_call_id:
                    self.tool_call_to_name[tool_call_id] = tool_name

            # Processar cada tool call
            for tc in tool_calls:
                messages.append(self._process_tool_call(tc, base_dict))

        # Se houver conteúdo final de texto (mesmo com tool calls, às vezes o modelo explica)
        if final_content:
            # Adicionar assistant message
            messages.append(
                {
                    **base_dict,
                    "message_type": "assistant_message",
                    "content": (
                        markdown_to_whatsapp(final_content)
                        if use_whatsapp_format
                        else final_content
                    ),
                }
            )

        return messages

    def _process_tool_call(
        self, tool_call: Dict[str, Any], base_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Processa uma tool call individual."""
        tool_call_id = tool_call.get("id", str(uuid.uuid4()))

        # Tentar parsear arguments como JSON
        args = tool_call.get("args", {})
        try:
            if isinstance(args, str):
                parsed_args = json.loads(args)
            else:
                parsed_args = args
        except (json.JSONDecodeError, TypeError):
            parsed_args = str(args)

        return {
            **base_dict,
            "id": f"{tool_call_id}",
            "message_type": "tool_call_message",
            "tool_call": {
                "name": tool_call.get("name", "unknown"),
                "arguments": parsed_args,
                "tool_call_id": tool_call_id,
            },
        }

    def process_tool_message(
        self, kwargs: Dict[str, Any], base_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Processa mensagem do tipo tool."""
        status = "error" if (kwargs.get("status") == "error") else "success"
        tool_call_id = kwargs.get("tool_call_id", "")

        # Usar o mapeamento para obter o nome da ferramenta
        tool_name = (
            self.tool_call_to_name.get(tool_call_id)
            or kwargs.get("name")
            or "unknown_tool"
        )

        # Tentar parsear tool_return como JSON
        tool_content = kwargs.get("content", "")
        try:
            if isinstance(tool_content, str) and tool_content.strip().startswith(
                ("{", "[")
            ):
                parsed_tool_return = json.loads(tool_content)
            else:
                parsed_tool_return = tool_content
        except (json.JSONDecodeError, TypeError):
            parsed_tool_return = tool_content

        # Atualizar step_id após tool return
        self.current_step_id = f"step-{uuid.uuid4()}"

        return {
            **base_dict,
            "id": base_dict["id"] or f"tool-return-{tool_call_id}",
            "name": tool_name,
            "message_type": "tool_return_message",
            "is_err": status == "error",
            "tool_return": parsed_tool_return,
            "status": status,
            "tool_call_id": tool_call_id,
            "stdout": None,
            "stderr": parsed_tool_return if status == "error" else None,
        }

    def calculate_usage_statistics(
        self, messages_to_process: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Calcula estatísticas de uso agregadas.

        Args:
            messages_to_process: Lista de mensagens processadas

        Returns:
            Dict com estatísticas de uso
        """
        input_tokens = 0
        output_tokens = 0
        total_tokens = 0
        thoughts_tokens = 0
        model_names = set()

        for msg in messages_to_process:
            kwargs = msg.get("kwargs", {})
            response_metadata = kwargs.get("response_metadata", {})
            usage_md = response_metadata.get("usage_metadata", {})

            # Mapear campos corretos do Google AI
            # input
            input_tokens += int(usage_md.get("prompt_token_count", 0) or 0)

            # output (candidates)
            output_tokens += int(usage_md.get("candidates_token_count", 0) or 0)

            # thoughts (se houver)
            thoughts_tokens += int(usage_md.get("thoughts_token_count", 0) or 0)

            # total (geralmente a soma, mas usamos o valor retornado se existir)
            msg_total = int(usage_md.get("total_token_count", 0) or 0)
            if msg_total == 0:
                msg_total = (
                    int(usage_md.get("prompt_token_count", 0) or 0)
                    + int(usage_md.get("candidates_token_count", 0) or 0)
                    + int(usage_md.get("thoughts_token_count", 0) or 0)
                )
            total_tokens += msg_total

            # Coletar model_names
            model_name = response_metadata.get("model_name")
            if model_name:
                model_names.add(model_name)

        # Se thoughts não estiverem incluídos em output_tokens (candidates),
        # podemos querer expor isso ou somar.
        # No Gemini, thoughts são cobrados como output, então faz sentido somar para ter uma noção de "tokens gerados".
        total_output_tokens = output_tokens + thoughts_tokens

        return {
            "message_type": "usage_statistics",
            "completion_tokens": total_output_tokens,  # Inclui pensamentos para refletir geração total
            "thoughts_tokens": thoughts_tokens,  # Novo campo para expor o total de tokens de pensamento
            "prompt_tokens": input_tokens,
            "total_tokens": total_tokens,
            "step_count": len(
                {m.get("step_id") for m in self.processed_messages if m.get("step_id")}
            ),
            "steps_messages": None,
            "run_ids": None,
            "agent_id": self.thread_id,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "status": "done",
            "model_names": list(model_names),
        }

    def format_messages(
        self,
        messages: List[Union[BaseMessage, Dict[str, Any]]],
        thread_id: Optional[str] = None,
        session_timeout_seconds: Optional[int] = None,
        use_whatsapp_format: bool = True,
    ) -> Dict[str, Any]:
        """
        Converte uma lista de mensagens para o formato Gateway.

        Args:
            messages: Lista de mensagens (BaseMessage ou dict já serializados)
            thread_id: ID do thread/agente (opcional, sobrescreve o da instância)
            session_timeout_seconds: Tempo limite em segundos para nova sessão
            use_whatsapp_format: Define se deve usar o markdown_to_whatsapp

        Returns:
            Dict no formato Gateway com status, data, mensagens e estatísticas de uso
        """
        # Usar thread_id fornecido ou o da instância
        if thread_id:
            self.thread_id = thread_id

        # Resetar estado para nova formatação
        self.reset_state()
        self.processed_messages = []

        # Serializar mensagens se necessário
        messages_to_process = []
        for msg in messages:
            if isinstance(msg, BaseMessage):
                serialized_msg = self.serialize_message(msg)
                messages_to_process.append(serialized_msg)
            else:
                messages_to_process.append(msg)

        # Processar cada mensagem
        for msg in messages_to_process:
            kwargs = msg.get("kwargs", {})
            msg_type = kwargs.get("type")

            # Extrair timestamp
            additional_kwargs = kwargs.get("additional_kwargs", {})
            message_timestamp = additional_kwargs.get("timestamp", None)

            # Calcular tempo entre mensagens
            time_since_last_message = self.calculate_time_since_last_message(
                message_timestamp
            )

            # Atualizar estado da sessão
            self.update_session_state(
                message_timestamp, time_since_last_message, session_timeout_seconds
            )

            # Extrair metadados
            metadata = self.extract_message_metadata(kwargs)

            # Criar base da mensagem
            base_dict = self.create_base_message_dict(
                kwargs, message_timestamp, time_since_last_message, metadata
            )

            # Processar baseado no tipo
            if msg_type == "human":
                processed_msg = self.process_human_message(kwargs, base_dict)
                self.processed_messages.append(processed_msg)

            elif msg_type == "ai":
                processed_msgs = self.process_ai_message(
                    kwargs, base_dict, use_whatsapp_format
                )
                self.processed_messages.extend(processed_msgs)

            elif msg_type == "tool":
                processed_msg = self.process_tool_message(kwargs, base_dict)
                self.processed_messages.append(processed_msg)

        # Calcular estatísticas de uso
        usage_stats = self.calculate_usage_statistics(messages_to_process)
        self.processed_messages.append(usage_stats)

        return {
            "status": "completed",
            "data": {
                "messages": self.processed_messages,
                # "messages": messages_to_process,
            },
        }


# Função de conveniência para manter compatibilidade
def to_gateway_format(
    messages: List[Union[BaseMessage, Dict[str, Any]]],
    thread_id: Optional[str] = None,
    session_timeout_seconds: Optional[int] = None,
    use_whatsapp_format: bool = True,
) -> Dict[str, Any]:
    """
    Função de conveniência que mantém a interface anterior.

    Args:
        messages: Lista de mensagens (BaseMessage ou dict já serializados)
        thread_id: ID do thread/agente (opcional)
        session_timeout_seconds: Tempo limite em segundos para nova sessão
        use_whatsapp_format: Define se deve usar o markdown_to_whatsapp

    Returns:
        Dict no formato Gateway com status, data, mensagens e estatísticas de uso
    """
    formatter = LangGraphMessageFormatter(thread_id=thread_id)
    return formatter.format_messages(
        messages=messages,
        thread_id=thread_id,
        session_timeout_seconds=session_timeout_seconds,
        use_whatsapp_format=use_whatsapp_format,
    )
