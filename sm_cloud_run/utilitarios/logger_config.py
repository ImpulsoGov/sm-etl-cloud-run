import sys
import logging

def logger_config():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        handlers=[logging.StreamHandler(sys.stdout)])  # Ensure it prints to terminal

