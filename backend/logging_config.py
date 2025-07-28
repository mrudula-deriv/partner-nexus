import logging
from logging.handlers import RotatingFileHandler
import os

class LoggingConfig:
    def __init__(self, name):
        self.name = name

    def setup_logger(self):
        # Create logs directory if it doesn't exist
        if not os.path.exists('logs'):
            os.makedirs('logs')

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            f'logs/{self.name}.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        # Create logger
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.INFO)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
