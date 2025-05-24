from datetime import datetime
import logging

from .constants import LOG_SINGLE_PASS, WORKING_FOLDER

def init_logging() -> None:
    """
    Initialises logging
    """

    class PaddedProcessFormatter(logging.Formatter):
        def format(self, record):
            # Pad process ID to 4 digits with leading zeros
            record.process_padded = f"PID:{record.process:08d}"
            return super().format(record)

    log_format = '%(asctime)s,%(msecs)03d [%(process_padded)s] [%(levelname)-2s] %(message)s'
    formatter = PaddedProcessFormatter(log_format, "%Y-%m-%d %H:%M:%S")
    handler_1 = logging.StreamHandler()
    handler_2 = logging.FileHandler(LOG_SINGLE_PASS)
    handler_3 = logging.FileHandler("{0}/{1}.log".format(WORKING_FOLDER, datetime.today().strftime('%Y-%m-%d')))

    handler_1.setFormatter(formatter)
    handler_2.setFormatter(formatter)
    handler_3.setFormatter(formatter)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(process_padded)s] [%(levelname)-2s] %(message)s',
        handlers=[handler_1, handler_2, handler_3]
    )