# -*- coding: utf-8 -*-
import requests
import json

TOKEN = "6bTVQZZQJ4YcWA5cEtgLLCnQ"
MONITOR_ID = "3839268"
START_DATE = "2025-11-17"
END_DATE = "2025-12-17"

def fetch_sla():
    url = f"https://uptime.betterstack.com/api/v2/monitors/{MONITOR_ID}/sla"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    params = {
        "from": START_DATE,
        "to": END_DATE
    }
    
    print(f"Fetching SLA data from {START_DATE} to {END_DATE}...")
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        print("\n--- Resultado da API de SLA ---")
        print(json.dumps(data, indent=2))
        
        # Salva em um arquivo para facilitar a visualização
        with open("exemplo_sla_betterstack.json", "w") as f:
            json.dump(data, f, indent=2)
        print("\nExemplo salvo em: exemplo_sla_betterstack.json")
    else:
        print(f"Erro na API: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    fetch_sla()
