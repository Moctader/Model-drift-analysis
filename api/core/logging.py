import logging
from core.settings import settings


def setup_logging():
    logging.basicConfig(level=logging.INFO)

    if settings.LOGGER_CONSOLE:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logging.getLogger().addHandler(console_handler)


logger = logging.getLogger(settings.PROJECT_NAME)
