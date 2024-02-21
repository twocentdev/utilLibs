from tranformer_builder_abstract import Transformer_Builder_Abstract
from transformer import CSV_to_JSON_Transformer
from transformer import Delimiter_Transformer

import logging


class CSV_to_JSON_Transformer_Builder (Transformer_Builder_Abstract):
    """
    This class is used to build CSV_to_JSON_Transformers, it includes previous validations for input parameters.
    """

    def build(self):
        """
        This method is used to create a single CSV_to_JSON_Transformers. For this, both input and ouput file MUST be files.

        Returns a single CSV_to_JSON_Transformer for the given file.
        """
        logging.debug(f"[{self.__class__.__name__}.build] About to create CSV_to_JSON_Transformer.")
        return CSV_to_JSON_Transformer(self._input_file, self._output_file)


class Delimiter_Transformer_Builder (Transformer_Builder_Abstract):
    """
    This class is used to build Delimiter_Transformer(s).
    """

    def __init__(self, input_file, output_file="_temp", delimiter=","):
        """
        Basic constructor

        Parameters
        ----------
        _input_file : str
            the full path for the file where the data is serialized.
        _output_file : str
            the full path for the file where the data, when transformed, will be serialized.
        _delimiter : str
            this string contains the char(s) used to separate the values inside the CSV file.
        """
        logging.debug(
            f"[{self.__class__.__name__}] About to create CSV_to_JSON_TransformerBuilder --> input_file: {input_file}, output_file: {output_file}.")
        self._input_file = input_file
        self._output_file = f"{input_file[:input_file.rfind('.')]}{output_file}{input_file[input_file.rfind('.'):]}"
        self._delimiter = delimiter

    def build(self):
        """
        This method returns a Delimiter_Transformer with the params given to the builder.
        """
        logging.debug(f"[{self.__class__.__name__}.build] About to create Delimiter_TransformerBuilder.")
        return Delimiter_Transformer(self._input_file, self._output_file, self._delimiter)