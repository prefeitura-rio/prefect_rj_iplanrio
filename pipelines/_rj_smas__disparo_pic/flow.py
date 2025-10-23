# -*- coding: utf-8 -*-
"""
Main flow entry point for rj_smas__disparo_pic pipeline

Executa pipeline completa de disparo PIC SMAS com todos os parâmetros
usando valores padrão definidos nas constantes.
"""

from pipelines.rj_smas__disparo_pic.flows.whatsapp_flow import whatsapp_flow

# Execute flow with all defaults from PicConstants
whatsapp_flow()
