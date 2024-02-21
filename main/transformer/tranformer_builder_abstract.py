from abc import ABC, abstractmethod

import logging


class Transformer_Builder_Abstract(ABC):
    """
    This interface defines the basics for Transformer Builders, it includes previous validations for input parameters.

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

    build() : Transformer
        returns a single Transformer
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
        logging.debug(
            f"[{self.__class__.__name__}] About to create CSV_to_JSON_TransformerBuilder --> input_file: {input_file}, output_file: {output_file}")
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

        Returns the full path for the file where the data will be serialized after transformed.
        """
        logging.info(f"[{self.__class__.__name__}.getOutputFile] output_file: {self._output_file}")
        return self._output_file

    @abstractmethod
    def build(self):
        """
        This abstract method should return a single Transformer.
        """
        pass