"""
Create the logging object to use in the different components of the simulator.

The logging object is used to log different events in the scheduler in a
detailed and uniform manner for analysis.
"""
import logging
from pathlib import Path
from datetime import datetime


class SimulatorLogger:
    """This class is to define and create instances of the logging class."""

    LOG_FILE_PATH = Path("logs")
    LOG_FORMAT = ("%(asctime)s : %(module)s : %(filename)s : %(funcName)s : "
                  "%(lineno)d : %(levelname)s : %(message)s")

    is_setup: bool = False

    def __init__(self, module_name: str):
        """
        Initialise the object with the name of the caller code's module name.

        Args:
            module_name (str): Name of the caller code's module
        """
        self.module_name = module_name
        if SimulatorLogger.is_setup is False:
            # print(f'{datetime.now().strftime("record-%Y-%m-%d-%H-%M-%S.log")}'
            # ' Hello')
            self.LOG_FILE_NAME = (self.LOG_FILE_PATH /
                                  datetime.now().strftime("record-%Y-%m-%d-"
                                                          "%H-%M-%S.log"))
            with open(self.LOG_FILE_NAME, "a"):
                ...
            logging.basicConfig(filename=self.LOG_FILE_NAME,
                                format=self.LOG_FORMAT,
                                # encoding='utf-8',  # Only for Python >= 3.9
                                level=logging.INFO)

            # Makes sure that the root logger is setup only once
            SimulatorLogger.is_setup = True

    def get_logger(self) -> logging.Logger:
        """
        Return a configured instance of the logging class to the caller.

        Returns:
            logging.Logger: Object used for logging runtime information.
        """
        return logging.getLogger(self.module_name)
