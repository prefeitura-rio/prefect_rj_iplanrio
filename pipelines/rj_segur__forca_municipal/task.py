# -*- coding: utf-8 -*-
"""
Script para transformar todos os endpoints GET da API em DataFrames.
Baseado na API CIVITAS/CORIO com autenticação, proxy e paginação.

Sistema: HxGN OnCall - Gestão de Ocorrências e Unidades da Guarda Municipal RJ
Documentação: api.pdf v1.1 (29/04/2025)
"""
import json
import logging
import warnings
from typing import Dict, List, Optional

import httpx
import pandas as pd
from prefect import task

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class APIToDataFrame:
    """Classe para converter endpoints da API em DataFrames com suporte a paginação."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        # proxy: Optional[str] = None,
        verify_ssl: bool = False,
        timeout: int = 300
    ):
        """
        Inicializa o conversor de API para DataFrames.

        Args:
            base_url: URL base da API
            username: Nome de usuário para autenticação
            password: Senha para autenticação
            proxy: URL do proxy (opcional)
            verify_ssl: Se True, verifica certificados SSL
            timeout: Timeout das requisições em segundos
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        # self.proxy = proxy
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.access_token = None
        self.expiration_time = None

    def authenticate(self) -> bool:
        """
        Realiza autenticação na API e obtém o access token.

        Returns:
            True se autenticação foi bem-sucedida, False caso contrário
        """
        login_url = f"{self.base_url}/login"
        credentials = {
            "userName": self.username,
            "password": self.password
        }

        try:
            with httpx.Client(
                # proxy=self.proxy,
                verify=self.verify_ssl,
                timeout=self.timeout
            ) as client:
                print(f"Autenticando em {login_url} ...")
                response = client.post(login_url, json=credentials)
                response.raise_for_status()

                data = response.json()
                self.access_token = data.get('accessToken')
                self.expiration_time = data.get('expirationTime')

                if self.access_token:
                    print(f"✓ Autenticação bem-sucedida")
                    print(f"  Token expira em: {self.expiration_time}")
                    return True
                else:
                    print("Token não encontrado na resposta")
                    return False

        except httpx.HTTPStatusError as e:
            print(f"Erro HTTP ao autenticar: {e.response.status_code} - {e.response.text}")
            return False
        except Exception as e:
            print(f"Erro ao autenticar: {e}")
            return False

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        client: Optional[httpx.Client] = None
    ) -> Optional[Dict]:
        """
        Faz requisição GET para o endpoint.

        Args:
            endpoint: Endpoint da API
            params: Parâmetros da query (opcional)
            client: Cliente httpx reutilizável (opcional)

        Returns:
            Resposta JSON ou None em caso de erro
        """
        if not self.access_token:
            print("Não autenticado. Execute authenticate() primeiro.")
            return None

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        headers = {"Authorization": f"Bearer {self.access_token}"}
        try:
            if client:
                response = client.get(url, params=params, headers=headers)
            else:
                with httpx.Client(
                    # proxy=self.proxy,
                    verify=self.verify_ssl,
                    timeout=self.timeout
                ) as temp_client:
                    response = temp_client.get(url, params=params, headers=headers)
            print(f"Requisição GET {url} com params {params} - status: {response.status_code}")
            response.raise_for_status()
            return response.json()

        except httpx.HTTPStatusError as e:
            print(f"Erro HTTP {e.response.status_code} ao requisitar {url}: {e.response.text[:200]}")
            return None
        except Exception as e:
            print(f"Erro ao requisitar {url} : {e}")
            return None

    def _extract_data_from_response(self, response_data: Dict) -> Optional[List]:
        """
        Extrai lista de dados da resposta JSON.

        Args:
            response_data: Dados da resposta JSON

        Returns:
            Lista de registros ou None
        """
        if isinstance(response_data, list):
            return response_data

        if isinstance(response_data, dict):
            # Possíveis chaves que contêm os dados
            data_keys = ['data', 'results', 'items', 'records', 'content']
            for key in data_keys:
                if key in response_data:
                    return response_data[key]

        return None

    def _get_total_pages(self, response_data: Dict) -> int:
        """
        Obtém o número total de páginas da resposta.

        Args:
            response_data: Dados da resposta JSON

        Returns:
            Número total de páginas (padrão: 1)
        """
        if not isinstance(response_data, dict):
            return 1

        # Possíveis estruturas de paginação
        if 'totalPages' in response_data:
            return response_data['totalPages']
        if 'pagination' in response_data:
            return response_data['pagination'].get('totalPages', 1)
        if 'total_pages' in response_data:
            return response_data['total_pages']

        return 1

    def _expand_json_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Expande colunas que contêm strings JSON em colunas separadas.

        Args:
            df: DataFrame com possíveis colunas JSON

        Returns:
            DataFrame com colunas JSON expandidas
        """
        if df.empty:
            return df

        df_expanded = df.copy()

        for col in df.columns:
            # Verifica se a coluna contém strings que parecem ser JSON
            sample = df[col].dropna().head(100)
            if sample.empty:
                continue

            # Verifica se pelo menos alguns valores são strings que começam com { ou [
            json_like = sample.astype(str).str.match(r'^\s*[\{\[]', na=False)

            if json_like.sum() > len(sample) * 0.3:  # Se >30% parecem JSON
                print(f"  Expandindo coluna JSON: {col}")

                try:
                    # Tenta fazer parse do JSON
                    parsed_data = []
                    for value in df[col]:
                        if pd.isna(value):
                            parsed_data.append({})  # Dicionário vazio para NaN
                        else:
                            try:
                                parsed = json.loads(value)
                                # Se não for dict, converte para dict vazio
                                if isinstance(parsed, dict):
                                    parsed_data.append(parsed)
                                else:
                                    parsed_data.append({})
                            except (json.JSONDecodeError, TypeError):
                                parsed_data.append({})  # Dicionário vazio para erros

                    # Cria DataFrame com os dados parseados
                    json_df = pd.json_normalize(parsed_data)

                    # Se conseguiu criar colunas
                    if not json_df.empty and len(json_df.columns) > 0:
                        # Renomeia colunas com prefixo da coluna original
                        json_df.columns = [f"{col}_{subcol}" for subcol in json_df.columns]

                        # Garante que o índice seja o mesmo
                        json_df.index = df_expanded.index

                        # Remove a coluna original e adiciona as novas
                        df_expanded = df_expanded.drop(columns=[col])
                        df_expanded = pd.concat([df_expanded, json_df], axis=1)

                        print(f"    ✓ {len(json_df.columns)} sub-colunas criadas")
                    else:
                        print(f"    ✗ Nenhuma sub-coluna criada para {col}")

                except Exception as e:
                    print(f"    ✗ Falha ao expandir {col}: {e}")

        return df_expanded

    def endpoint_to_dataframe(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        normalize: bool = True,
        paginated: bool = True,
        page_size: int = 100
    ) -> Optional[pd.DataFrame]:
        """
        Converte um endpoint GET em DataFrame com suporte a paginação.

        Args:
            endpoint: Endpoint da API
            params: Parâmetros da query (opcional)
            normalize: Se True, normaliza JSON aninhado
            paginated: Se True, busca todas as páginas automaticamente
            page_size: Tamanho da página para endpoints paginados

        Returns:
            DataFrame com os dados ou None em caso de erro
        """
        if not self.access_token:
            print("Não autenticado. Execute authenticate() primeiro.")
            return None

        all_records = []
        params = params or {}

        try:
            with httpx.Client(
                # proxy=self.proxy,
                verify=self.verify_ssl,
                timeout=self.timeout
            ) as client:

                if paginated:
                    # Primeira requisição para descobrir total de páginas
                    initial_params = {**params, 'page': 1, 'pageSize': page_size}
                    print(f"Buscando página 1 de {endpoint}...")

                    first_response = self._make_request(endpoint, initial_params, client)
                    if first_response is None:
                        return None

                    # Extrai dados da primeira página
                    first_page_data = self._extract_data_from_response(first_response)
                    if first_page_data:
                        all_records.extend(first_page_data)

                    # Descobre total de páginas
                    total_pages = self._get_total_pages(first_response)
                    print(f"  Total de páginas: {total_pages}")

                    # Busca páginas restantes
                    for page in range(2, total_pages + 1):
                        print(f"Buscando página {page}/{total_pages}...")
                        page_params = {**params, 'page': page, 'pageSize': page_size}

                        page_response = self._make_request(endpoint, page_params, client)
                        if page_response is None:
                            print(f"Falha ao obter página {page}, continuando...")
                            continue

                        page_data = self._extract_data_from_response(page_response)
                        if page_data:
                            all_records.extend(page_data)

                else:
                    # Sem paginação - única requisição
                    response = self._make_request(endpoint, params, client)
                    if response is None:
                        return None

                    data = self._extract_data_from_response(response)
                    if data:
                        all_records.extend(data)

            # Converte para DataFrame
            if not all_records:
                print(f"Nenhum registro encontrado em {endpoint}")
                return None

            df = pd.json_normalize(all_records) if normalize else pd.DataFrame(all_records)
            print(f"✓ DataFrame inicial criado: {len(df)} linhas × {len(df.columns)} colunas")

            # Expande colunas que contêm JSON
            df = self._expand_json_columns(df)
            print(f"✓ DataFrame final: {len(df)} linhas × {len(df.columns)} colunas")

            return df

        except Exception as e:
            print(f"Erro ao converter para DataFrame: {e}")
            return None

@task
def get_single_endpoint(endpoint: str, **kwargs) -> Optional[str]:
    """
    Task do Prefect para obter um endpoint da API e salvar como CSV.

    Args:
        endpoint: Endpoint da API (ex: "/unidades/ativas")
        **kwargs: Argumentos adicionais (params, paginated, page_size)

    Returns:
        Caminho do arquivo CSV salvo ou None em caso de erro

    Exemplo:
        path = get_single_endpoint("/unidades/ativas", paginated=True, page_size=100)
    """
    from pathlib import Path  # noqa: PLC0415

    from pipelines.rj_segur__forca_municipal.constants import API_CONFIG  # noqa: PLC0415

    # Usa configurações do constants.py
    converter = APIToDataFrame(
        base_url=API_CONFIG["base_url"],
        username=API_CONFIG["username"],
        password=API_CONFIG["password"],
        # proxy=API_CONFIG["proxy"],
        verify_ssl=API_CONFIG["verify_ssl"],
    )

    # Autentica
    if not converter.authenticate():
        print("Falha na autenticação")
        return None

    # Extrai dados do endpoint
    df = converter.endpoint_to_dataframe(endpoint, **kwargs)
    if df is None or df.empty:
        print(f"Nenhum dado obtido do endpoint {endpoint}")
        return None

    # Prepara nome do arquivo
    filename = endpoint.strip('/').replace('/', '_')
    output_dir = Path("./output/single_endpoint")
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / f"{filename}.csv"

    # Salva CSV
    df.to_csv(filepath, index=False, encoding='utf-8')
    print(f"✓ Arquivo salvo: {filepath}")
    print(f"✓ Total de linhas: {len(df)}")
    print(f"✓ Total de colunas: {len(df.columns)}")


    return str(filepath)
