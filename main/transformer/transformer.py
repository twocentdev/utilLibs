from abc import ABC, abstractmethod
from transformer_abstract import Transformer_Abstract
from tranformer_builder_abstract import Transformer_Builder_Abstract

import csv
import logging
import json


class CSV_to_JSON_Transformer(Transformer_Abstract):
    """
    This transformer receives a CSV file and saves its content into a JSON file.
    """

    def transform(self):
        """
        Transforms the data serialized in the input file (CSV) and saves into the output file (JSON).
        """
        logging.debug(f"[{self.__class__.__name__}.csv_to_json] About to transform csv to json.")
        json_list = []

        with open(self._input_file, encoding="utf-8-sig") as csv_file_handler:
            logging.debug(f"[{self.__class__.__name__}.csv_to_json] input_file has just been opened.")
            csv_reader = csv.DictReader(csv_file_handler, delimiter=",")

            for row in csv_reader:
                logging.debug(f"[{self.__class__.__name__}.csv_to_json] New row just read --> {row}")
                json_list.append(row)

        with open(self._output_file, "w", encoding="utf-8-sig") as json_file_handler:
            logging.debug(f"[{self.__class__.__name__}.csv_to_json] output_file has just been opened.")
            json_file_handler.write(json.dumps(json_list, indent=4))

        logging.debug(f"[CSV_to_JSON_Transformer.csv_to_json] csv has been transformed to json correctly.")
        return 0


class Delimiter_Transformer(Transformer_Abstract):
    """
    This transformer receives a CSV file replaces its delimiter to ','. This is required because CSV_to_JSON_Transformer ONLY allows ',' as delimiter.
    """

    def __init__(self, input_file, output_file, delimiter = ","):
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
        self._delimiter = delimiter 

    def transform(self):
        """
        This function is used to replace the delimiter char inside a CSV file.
        """
        logging.debug(f"[{self.__class__.__name__}.resetOutputFile] About to replace delimiter")
        with open(self._input_file, encoding="utf-8-sig") as input_file_handler:
            with open(self._output_file, "w", encoding="utf-8-sig") as output_file_handler:
                for row in input_file_handler:
                    output_file_handler.write(row.replace(self._delimiter, ","))
        return


class Transformer_Selector:
    """
    This class is used to check file extensions (csv, txt, json, xml, etc.) and select the suitable transformer.
    """

    def __init__ (self, input_file, output_file):
        """
        Basic constructor

        Parameters
        ----------
        _input_file : str
            the file to be transformed from.
        _output_file : str
            the file to be transformed to.
        """
        self._input_file = input_file[input_file.rfind('.')+1:].lower()
        self._output_file = output_file[output_file.rfind('.')+1:].lower()
    
    def select(self):
        """
        This methods checks both file extensions in order to select the appropiate transformer. You should implement the appropiate transformer and builder for each.

        Returns a string that represents the required transformer. It should be like %FORMAT%_to_%FORMAT%, ej.: csv_to_json or json_to_xml
        """
        logging.debug(f"[{self.__class__.__name__}.select] About to select transformer for {self._input_file} to {self._output_file} files.")
        if self._input_file == "csv":
            if self._output_file == "json":
                transformer = "csv_to_json"
                logging.debug(f"[{self.__class__.__name__}.select] The selected transformer is {transformer}.")
                return transformer
        logging.error(f"[{self.__class__.__name__}.select] There is no transformer for this files.")
        return "unknown"
