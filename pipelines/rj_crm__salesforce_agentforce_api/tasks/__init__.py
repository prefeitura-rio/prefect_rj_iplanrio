# -*- coding: utf-8 -*-
# Re-exporta as tasks do módulo legado (tasks.py) para manter compatibilidade
# com flow.py e test_local.py, que importam de `.tasks` diretamente.
from pipelines.rj_crm__salesforce_agentforce_api.tasks_legacy import (  # noqa: F401
    build_dataframe,
    fetch_crm_bulk_query,
    list_available_objects,
)
