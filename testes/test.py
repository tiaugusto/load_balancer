from configure_logs import configure_logs

logger = configure_logs("aloha")

logger.debug("Mensagem de depuração")
logger.info("Aplicação iniciada")
logger.warning("Aviso de teste")
logger.error("Erro de teste")
logger.critical("Erro crítico de teste")

print("Teste concluído! Verifique a pasta logs.")