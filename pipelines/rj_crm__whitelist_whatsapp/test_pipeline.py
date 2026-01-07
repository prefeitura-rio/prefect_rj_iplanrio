# -*- coding: utf-8 -*-
"""
Local test script for the rj_crm__whitelist_whatsapp pipeline.
Run this after setting the RMI_ environment variables.
"""
import os
import sys
from pathlib import Path

# Add project root to sys.path to allow imports from "pipelines"
root_path = str(Path(__file__).parents[2])
if root_path not in sys.path:
    sys.path.append(root_path)

import pandas as pd
from pipelines.rj_crm__whitelist_whatsapp.tasks import (
    get_whitelist_credentials,
    fetch_whitelist_data,
    transform_whitelist_data,
    generate_backfill_data
)
from iplanrio.pipelines_utils.logging import log

from prefect import flow

@flow(name="Test Whitelist Extraction")
def test_extraction_flow():
    print("🚀 Iniciando teste de extração...")
    
    # 2. Run tasks (inside flow context)
    try:
        creds = get_whitelist_credentials()
        raw_data = fetch_whitelist_data(creds)
        
        print(f"✅ Extração concluída! {len(raw_data)} registros recuperados.")
        
        if raw_data:
            df = transform_whitelist_data(raw_data)
            print("✅ Transformação padrão concluída!")
            
            # 3. Test Backfill (with small volume)
            print("\n🔄 Testando lógica de Backfill...")
            df_backfill = generate_backfill_data(raw_data[:10]) # Apenas 10 registros para não demorar
            print(f"✅ Backfill de amostra concluído! {len(df_backfill)} linhas geradas.")
            
            print("\nPrimeiros 5 registros do Backfill:")
            print(df_backfill.head())
            
            # Save a sample for checking
            df_backfill.to_csv("test_backfill_sample.csv", index=False)
            print("\n💾 Amostra de Backfill salva em 'test_backfill_sample.csv'")
            
    except Exception as e:
        print(f"❌ Ocorreu um erro durante o teste: {e}")

if __name__ == "__main__":
    # Check if env vars are set
    required_vars = [
        "RMI_AUTH_URL", 
        "RMI_CLIENT_ID", 
        "RMI_CLIENT_SECRET", 
        "RMI_WHITELIST_API_URL"
    ]
    
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        print(f"❌ Erro: As seguintes variáveis de ambiente estão faltando: {missing}")
        print("Sete-as antes de rodar o teste.")
    else:
        test_extraction_flow()
