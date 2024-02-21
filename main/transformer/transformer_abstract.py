from abc import ABC, abstractmethod

import logging


class Transformer_Abstract(ABC):
    """
    This interface is used to define transformers.

    Attributes
    ----------
    _input_file : str
        the full path for the file where the data is serialized.
    _output_file : str
        the full path for the file where the data, when transformed, will be serialized.

    Methods
    -------
    getInputFile() : str
        returns the input file.
    
    getOutputFile() : str
        returns the output file.

    csv_to_json() : None
        transforms the data serialized in the input file and saves into the output file.
    """

    def __init__(self, input_file, output_file):
        """
        Basic constructor

        Parameters
        ----------
        _input_file : str
            the full path for the file where the data is serialized.
        _output_file : str
            the full path for the file where the data, when transformed, will be serialized.
        """
        logging.debug(f"[{self.__class__.__name__}] About to create CSV_to_JSON_Transformer --> input_file: {input_file}, output_file: {output_file}.")
        self._input_file = input_file
        self._output_file = output_file

    def getInputFile(self):
        """
        Getter for attribute _input_file.

        Returns the full path for the file where the data is serialized.
        """
        logging.info(f"[{self.__class__.__name__}.getInputFile] input_file: {self._input_file}")
        return self._input_file

    def getOutputFile(self):
        """
        Getter for attribute _output_file.

        Returns the full path for the file where the data will be serialized after tramsfpr,ed.
        """
        logging.info(f"[{self.__class__.__name__}.getOutputFile] output_file: {self._output_file}")
        return self._output_file

    @abstractmethod
    def transform(self):
        """
        This function reads data from one format (_input_file) and save it into the new format (_output_file).
        """
        pass