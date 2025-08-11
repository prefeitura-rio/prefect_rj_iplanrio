# Migração Prefect 1.4 → 3.0: SMAS Disparo CADUNICO

## 📋 Resumo da Migração

Pipeline **`disparo_automatico`** migrada de **`pipelines_rj_crm_registry`** (Prefect 1.4) para **`rj_smas__disparo_cadunico`** (Prefect 3.0).

---

## ✅ Arquivos Migrados

### Estrutura Original (Prefect 1.4)
```
pipelines_rj_crm_registry/pipelines/disparo_automatico/
├── schedules.py
├── flows.py  
├── tasks.py
└── templates/disparo/
    ├── processors.py
    ├── tasks.py
    └── disparo_template.py
```

### Estrutura Migrada (Prefect 3.0)
```
prefect_rj_iplanrio/pipelines/rj_smas__disparo_cadunico/
├── prefect.yaml
├── flow.py
├── tasks.py (inclui funções do template)
├── processors.py (migrado do template)
└── MIGRATION_NOTES.md (este arquivo)
```

---

## 🔄 Principais Mudanças Aplicadas

### 1. **Schedule → YAML**
| Prefect 1.4 | Prefect 3.0 |
|-------------|-------------|
| `Schedule(clocks=[IntervalClock(...)])` | Seção `schedules:` no YAML |
| `interval=timedelta(minutes=10)` | `interval: 600` (segundos) |
| `interval=timedelta(minutes=60)` | `interval: 3600` (segundos) |
| `start_date=datetime(...)` | `anchor_date: "2021-01-01T00:04:00"` |
| `labels=[constants.CRM_AGENT_LABEL.value]` | Removido (não existe no 3.0) |

### 2. **Flow Architecture**
| Prefect 1.4 | Prefect 3.0 |
|-------------|-------------|
| `with Flow(name="...", state_handlers=[...])` | `@flow(log_prints=True)` |
| `Parameter("dataset_id", default="...")` | Parâmetros de função |
| `case(condition, True):` | `if condition:` |
| `LocalDaskExecutor(num_workers=1)` | Removido (gerenciado pelo Prefect) |
| `KubernetesRun(...)` | Configurado no `prefect.yaml` |
| `GCS(...)` | Configurado no `prefect.yaml` |

### 3. **Imports & Libraries**
| Prefect 1.4 | Prefect 3.0 |
|-------------|-------------|
| `prefeitura_rio.pipelines_utils.logging.log` | `iplanrio.pipelines_utils.logging.log` |
| `prefeitura_rio.pipelines_utils.tasks.create_table_and_upload_to_gcs` | `iplanrio.pipelines_utils.bd.create_table_and_upload_to_gcs_task` |
| `prefeitura_rio.pipelines_utils.state_handlers.handler_inject_bd_credentials` | `iplanrio.pipelines_utils.env.inject_bd_credentials_task` |

---

## ⚠️ Alertas de Incompatibilidade

### 🔴 **Funções SEM Equivalente no iplanrio**

1. **`handler_initialize_sentry`**
   - ❌ **SEM EQUIVALENTE** 
   - **Impacto**: Monitoramento de erros Sentry removido
   - **Ação**: Implementar manualmente ou aguardar adição ao iplanrio

2. **`task_run_dbt_model_task`**
   - ❌ **SEM EQUIVALENTE**
   - **Impacto**: Execução automática de modelos DBT removida
   - **Ação**: Configurar pipeline DBT separado ou aguardar implementação

3. **Prefect 1.4 Engine Features**
   - ❌ `prefect.engine.signals.ENDRUN` → Substituído por `raise Exception`
   - ❌ `prefect.engine.state.Failed/Skipped` → Removidos no Prefect 3.0
   - ❌ `Parameter` objects → Substituídos por parâmetros de função

### 🔶 **Mudanças que Requerem Atenção**

1. **Constantes Hardcoded**
   ```python
   # ⚠️ ORIGINAL (dependia de constants.py)
   constants.WETALKIE_PATH.value
   constants.WETALKIE_URL.value
   
   # ✅ MIGRADO (hardcoded temporário)
   infisical_path = "/wetalkie"
   infisical_url = "wetalkie_url"
   ```
   **Ação**: Configurar adequadamente no sistema de secrets

2. **Error Handling**
   ```python
   # ⚠️ ORIGINAL
   raise ENDRUN(state=Skipped("message"))
   
   # ✅ MIGRADO  
   raise Exception("message")
   ```

3. **Task Execution Control**
   - Prefect 3.0 usa controle nativo ao invés de `case()` conditions
   - Flow logic movida para Python padrão `if/else`

---

## 🧪 Testes Recomendados

1. **Verificar credenciais API Wetalkie**
2. **Testar conexão BigQuery**
3. **Validar processamento de áudios** 
4. **Confirmar schedules funcionando**
5. **Verificar uploads GCS/BigQuery**

---

## 📊 Status da Migração

| Componente | Status | Observações |
|------------|---------|-------------|
| Schedule | ✅ Completo | Convertido para YAML |
| Flow Logic | ✅ Completo | Adaptado para Prefect 3.0 |
| Tasks | ✅ Completo | Imports adaptados + funções do template |
| Template Functions | ✅ Completo | Migradas: chunks, deduplicação, query processors |
| Query Processors | ✅ Completo | CADUNICO processor migrado |
| Error Handling | ⚠️ Parcial | ENDRUN/states removidos |
| DBT Integration | ❌ Pendente | Sem equivalente disponível |
| Sentry | ❌ Pendente | Sem equivalente disponível |
| Constants/Secrets | ⚠️ Temporário | Hardcoded, requer configuração |

### ✅ **Funções do Template Migradas:**
- `printar()` - Debug utility
- `remove_duplicate_phones()` - Remove telefones duplicados
- `dispatch_with_chunks()` - Disparo em lotes
- `get_destinations_with_processor()` - Query com processadores
- `get_query_processor()` - Registry de processadores
- `process_cadunico_query()` - Processador CADUNICO

---

## 🚀 Próximos Passos

1. **Configurar secrets** no sistema Infisical para credenciais Wetalkie
2. **Implementar integração DBT** separadamente  
3. **Adicionar monitoramento** (substituto do Sentry)
4. **Testar pipeline** completo em ambiente de staging
5. **Validar agendamento** e execução automática