import logging

def get_logger(name: str):
    logger = logging.getLogger(name)
    
    # Prevent duplicate logs if the logger is called multiple times
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        
        # Create console handler and set format
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
        
    return logger