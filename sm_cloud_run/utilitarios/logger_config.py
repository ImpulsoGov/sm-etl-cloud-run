import logging

def logger_config():
    # Configuração do formato da mensagem de log
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M')    
    # Criação do manipulador de fluxo para exibir mensagens de log no console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)    
    # Adiciona o manipulador de fluxo ao logger raiz
    logging.getLogger('').addHandler(console)
