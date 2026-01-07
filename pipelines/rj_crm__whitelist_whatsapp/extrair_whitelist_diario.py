import requests
import csv
import math
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# --- Configurações da API ---
API_URL = "https://services.pref.rio/rmi/v1/admin/beta/whitelist"
AUTH_URL = "https://auth-idrio.apps.rio.gov.br/auth/realms/idrio_cidadao/protocol/openid-connect/token"

# Credenciais para geração do Token
CLIENT_ID = "superapp.apps.rio.gov.br"
CLIENT_SECRET = "0a502c7d-7672-4ae4-b70d-d22ba0c9bcaa"

PER_PAGE = 100 # Máximo recomendado para paginação
CSV_DIARIO_FILENAME = "whitelist_diario.csv"
MAX_WORKERS = 5 # Número de threads para execução paralela
BR_TZ = ZoneInfo("America/Sao_Paulo")

# --- Headers (Cabeçalhos) da Requisição ---
headers = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def get_access_token():
    """
    Obtém um novo access_token usando as credenciais do cliente.
    """
    print("Obtendo novo token de acesso...")
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    
    try:
        response = requests.post(AUTH_URL, data=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        token = data.get("access_token")
        if token:
            print("✅ Token obtido com sucesso.")
            return token
        else:
            print("❌ Erro: Token não encontrado na resposta da autenticação.")
            return None
    except Exception as e:
        print(f"❌ Erro ao obter token: {e}")
        return None

def fetch_page(page):
    """
    Função para buscar uma única página da API.
    Retorna uma lista de registros ou uma lista vazia em caso de erro.
    """
    while True:
        try:
            params = {"page": page, "per_page": PER_PAGE}
            response = requests.get(API_URL, headers=headers, params=params, timeout=30)
            
            if response.status_code == 429:
                print(f"⚠️ Erro 429 (Too Many Requests) na página {page}. Aguardando 20s para retentar...")
                time.sleep(20)
                continue
            
            if response.status_code in [401, 403]:
                print(f"ERRO: Falha de Autenticação na página {page}. Token expirado?")
                return []

            response.raise_for_status()
            data = response.json()
            return data.get("whitelisted", [])
            
        except Exception as e:
            print(f"Erro ao buscar página {page}: {e}")
            return []

def varrer_api_para_csv():
    # 0. Obter Token de Acesso
    token = get_access_token()
    if not token:
        print("Abortando execução devido a falha na autenticação.")
        return
        
    # Atualizar headers com o token válido
    headers["Authorization"] = f"Bearer {token}"

    print("Iniciando a varredura da API...")
    
    # 1. Obter o total de registros para calcular páginas
    try:
        initial_params = {"page": 1, "per_page": PER_PAGE}
        
        while True:
            response = requests.get(API_URL, headers=headers, params=initial_params)
            
            if response.status_code == 429:
                print("⚠️ Erro 429 na requisição inicial. Aguardando 20s...")
                time.sleep(20)
                continue
            
            if response.status_code in [401, 403]:
                print(f"ERRO: Falha de Autenticação ({response.status_code}). Verifique o token.")
                return

            response.raise_for_status()
            break
            
        data = response.json()
        total_count = data.get("total_count", 0)
        
        if total_count == 0:
            print("Nenhum registro encontrado.")
            return

        total_pages = math.ceil(total_count / PER_PAGE)
        print(f"Total de registros: {total_count}")
        print(f"Total de páginas: {total_pages}")
        print(f"Iniciando download com {MAX_WORKERS} workers...")
        
    except Exception as e:
        print(f"Erro na requisição inicial: {e}")
        return

    # 2. Buscar todas as páginas em paralelo
    all_records = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_page = {executor.submit(fetch_page, page): page for page in range(1, total_pages + 1)}
        
        completed_count = 0
        for future in as_completed(future_to_page):
            page_records = future.result()
            all_records.extend(page_records)
            completed_count += 1
            if completed_count % 10 == 0 or completed_count == total_pages:
                print(f"Progresso: {completed_count}/{total_pages} páginas processadas.")

    print(f"Download concluído. Total de registros coletados: {len(all_records)}")

    # 3. Processar Dados
    print("Processando dados...")
    
    # Campos da API que queremos manter
    api_fields = ["phone_number", "group_id", "group_name", "added_at"]
    
    # Converter added_at para datetime e encontrar a data mínima
    processed_records = []
    # Inicializa min_date com timezone BR para evitar erro de comparação
    min_date = datetime.now(BR_TZ)
    
    for record in all_records:
        # Extrair apenas os campos desejados
        clean_record = {k: record.get(k, "") for k in api_fields}
        
        # Parse da data
        added_at_str = clean_record.get("added_at")
        if added_at_str:
            try:
                dt_utc = datetime.fromisoformat(added_at_str.replace('Z', '+00:00'))
                # Converte para o fuso horário do Brasil
                dt_br = dt_utc.astimezone(BR_TZ)
                
                clean_record["added_at_dt"] = dt_br
                if dt_br < min_date:
                    min_date = dt_br
            except ValueError:
                # Se falhar o parse, usa hoje como fallback
                clean_record["added_at_dt"] = datetime.now(BR_TZ)
        else:
             clean_record["added_at_dt"] = datetime.now(BR_TZ)
             
        processed_records.append(clean_record)

    # --- ARQUIVO: Snapshot Diário (Incremental) ---
    print(f"Gerando snapshot diário '{CSV_DIARIO_FILENAME}'...")
    
    # Definir intervalo de datas: da data mínima até hoje
    start_date = min_date.date()
    end_date = datetime.now(BR_TZ).date()
    
    print(f"Gerando dados de {start_date} até {end_date}...")
    
    snapshot_rows = []
    current_date = start_date
    
    while current_date <= end_date:
        day_str = current_date.strftime("%Y-%m-%d")
        
        for record in processed_records:
            if record["added_at_dt"].date() <= current_date:
                row = {
                    "snapshot_date": day_str,
                    "phone_number": record["phone_number"],
                    "group_id": record["group_id"]
                }
                snapshot_rows.append(row)
        
        current_date += timedelta(days=1)

    # Gravar CSV Diário
    output_fields_diario = ["snapshot_date", "phone_number", "group_id"]
    
    try:
        with open(CSV_DIARIO_FILENAME, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=output_fields_diario)
            # Sem cabeçalho conforme solicitado
            writer.writerows(snapshot_rows)
            
        print(f"✅ Sucesso! '{CSV_DIARIO_FILENAME}' gerado com {len(snapshot_rows)} linhas.")
        
    except Exception as e:
        print(f"❌ Erro ao escrever '{CSV_DIARIO_FILENAME}': {e}")

if __name__ == "__main__":
    varrer_api_para_csv()
