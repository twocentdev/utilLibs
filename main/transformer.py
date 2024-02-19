from abc import ABC, abstractmethod

import argparse
import csv
import logging
import json
import os


class Transformer(ABC):
    """
    This interface is used to define transformers.

    Attributes
    ----------
    _input_file : str
        the full path for the file where the data is serialized.
    _output_file : str
        the full path for the file where the data, when transformed, will be serialized.
    _overwrite : boolean
        if true, the output file can be overwrited with new data.

    Methods
    -------
    getInputFile() : str
        returns the input file.
    
    getOutputFile() : str
        returns the output file.

    getOverwrite() : str
        returns true if overwrite is allowed, false in other case.

    csv_to_json() : None
        transforms the data serialized in the input file and saves into the output file.
    """

    def __init__(self, input_file, output_file, overwrite=False):
        """
        Basic constructor

        Parameters
        ----------
        _input_file : str
            the full path for the file where the data is serialized.
        _output_file : str
            the full path for the file where the data, when transformed, will be serialized.
        _overwrite : boolean
            if true, the output file can be overwrited with new data.
        """
        logging.debug(f"[{self.__class__.__name__}] About to create CSV_to_JSON_Transformer --> input_file: {input_file}, output_file: {output_file}, overwrite: {overwrite}.")
        self._input_file = input_file
        self._output_file = output_file
        self._overwrite = overwrite

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

    def getOverwrite(self):
        """
        Getter for attribute _overwrite.

        Returns true if overwriting output file is allowed, false in other case.
        """
        logging.info(f"[{self.__class__.__name__}.getOverwrite] overwrite: {self._overwrite}")
        return self._overwrite

    @abstractmethod
    def transform(self):
        """
        This function reads data from one format (_input_file) and save it into the new format (_output_file).
        """
        pass


class CSV_to_JSON_Transformer(Transformer):
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


class Delimiter_Transformer(Transformer):
    """
    This transformer receives a CSV file replaces its delimiter to ','. This is required because CSV_to_JSON_Transformer ONLY allows ',' as delimiter.
    """

    def __init__(self, input_file, output_file, overwrite=False, delimiter = ","):
        """
        Basic constructor

        Parameters
        ----------
        _input_file : str
            the full path for the file where the data is serialized.
        _output_file : str
            the full path for the file where the data, when transformed, will be serialized.
        _overwrite : boolean
            if true, the output file can be overwrited with new data.
        """
        logging.debug(f"[{self.__class__.__name__}] About to create CSV_to_JSON_Transformer --> input_file: {input_file}, output_file: {output_file}, overwrite: {overwrite}.")
        self._input_file = input_file
        self._output_file = output_file
        self._overwrite = overwrite
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


class TransformerBuilder(ABC):
    """
    This interface defines the basics for Transformer Builders, it includes previous validations for input parameters.

    Attributes
    ----------
    _input_file : str
        the full path for the file where the data is serialized.
    _output_file : str
        the full path for the file where the data, when transformed, will be serialized.
    _overwrite : boolean
        if true, the output file can be overwrited with new data.

    Methods
    -------
    getInputFile() : str
        returns the input file.
    
    getOutputFile() : str
        returns the output file.

    getOverwrite() : str
        returns true if overwrite is allowed, false in other case.

    build() : Transformer
        returns a single Transformer
    """

    def __init__(self, input_file, output_file, overwrite=False):
        """
        Basic constructor

        Parameters
        ----------
        _input_file : str
            the full path for the file where the data is serialized.
        _output_file : str
            the full path for the file where the data, when transformed, will be serialized.
        _overwrite : boolean
            if true, the output file can be overwrited with new data.
        """
        logging.debug(
            f"[{self.__class__.__name__}] About to create CSV_to_JSON_TransformerBuilder --> input_file: {input_file}, output_file: {output_file}, overwrite: {overwrite == True}.")
        self._input_file = input_file
        self._output_file = output_file
        if overwrite:
            self._overwrite = overwrite
        else:
            self._overwrite = False

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

    def getOverwrite(self):
        """
        Getter for attribute _overwrite.

        Returns true if overwriting output file is allowed, false in other case.
        """
        logging.info(f"[{self.__class__.__name__}.getOverwrite] overwrite: {self._overwrite}")
        return self._overwrite

    @abstractmethod
    def build(self):
        """
        This abstract method should return a single Transformer.
        """
        pass

class CSV_to_JSON_TransformerBuilder (TransformerBuilder):
    """
    This class is used to build CSV_to_JSON_Transformers, it includes previous validations for input parameters.
    """

    def build(self):
        """
        This method is used to create a single CSV_to_JSON_Transformers. For this, both input and ouput file MUST be files.

        Returns a single CSV_to_JSON_Transformer for the given file.
        """
        logging.debug(f"[{self.__class__.__name__}.build] About to create CSV_to_JSON_Transformer.")
        return CSV_to_JSON_Transformer(self._input_file, self._output_file, self._overwrite)


class Delimiter_TransformerBuilder (TransformerBuilder):
    """
    This class is used to build Delimiter_Transformer(s).
    """

    def __init__(self, input_file, output_file="_temp", overwrite=False, delimiter=","):
        """
        Basic constructor

        Parameters
        ----------
        _input_file : str
            the full path for the file where the data is serialized.
        _output_file : str
            the full path for the file where the data, when transformed, will be serialized.
        _overwrite : boolean
            if true, the output file can be overwrited with new data.
        _delimiter : str
            this string contains the char(s) used to separate the values inside the CSV file.
        """
        logging.debug(
            f"[{self.__class__.__name__}] About to create CSV_to_JSON_TransformerBuilder --> input_file: {input_file}, output_file: {output_file}, overwrite: {overwrite == True}.")
        self._input_file = input_file
        self._output_file = f"{input_file[:input_file.rfind('.')]}{output_file}{input_file[input_file.rfind('.'):]}"
        if overwrite:
            self._overwrite = overwrite
        else:
            self._overwrite = False
        self._delimiter = delimiter

    def build(self):
        """
        This method returns a Delimiter_Transformer with the params given to the builder.
        """
        logging.debug(f"[{self.__class__.__name__}.build] About to create Delimiter_TransformerBuilder.")
        return Delimiter_Transformer(self._input_file, self._output_file, self._overwrite, self._delimiter)


class Transformer_selector:
    """
    This class is used to check file extensions (csv, txt, json, xml, etc.) and select the suitable transformer.
    """

    def __init__ (self, input_file_extension, output_file_extension):
        """
        Basic constructor

         Parameters
        ----------
        _input_file_extension : str
            the extension of the file to be transformed from.
        _output_file_extension : str
            the extension of the file to be transformed to.
        """
        self._input_file_extension = input_file_extension.lower()
        self._output_file_extension = output_file_extension.lower()
    
    def select(self):
        """
        This methods checks both file extensions in order to select the appropiate transformer. You should implement the appropiate transformer and builder for each.

        Returns a string that represents the required transformer. It should be like %FORMAT%_to_%FORMAT%, ej.: csv_to_json or json_to_xml
        """
        logging.debug(f"[{self.__class__.__name__}.select] About to select transformer for {self._input_file_extension} to {self._output_file_extension} files.")
        if self._input_file_extension == "csv":
            if self._output_file_extension == "json":
                transformer = "csv_to_json"
                logging.debug(f"[{self.__class__.__name__}.select] The selected transformer is {transformer}.")
                return transformer
        logging.error(f"[{self.__class__.__name__}.select] There is no transformer for this files.")
        return "unknown"

def main():
    # About to parse params
    parser = argparse.ArgumentParser(description="This commands allows to easily transforms data from one format files to another. The allowed transformations include: CSV --> JSON.")
    parser.add_argument("input_file", help="the file or directory where the data is stored.")
    parser.add_argument("output_file", help="the file or directory where the data will be stored after transforming it.")
    parser.add_argument("-b", "--batch", help="use this param if wanna transform all files inside a directory to the choosen format (csv, txt, json, etc.).")
    parser.add_argument("-d", "--delimiter", help="use this param if wanna set a delimiter")
    parser.add_argument("-o", "--overwrite", action="store_true", help="use this param if wanna allow file overwrite.")
    parser.add_argument("-v", "--verbose", action="store_true", help="enables detailed logs.")
    args = parser.parse_args()

    # About to set logger
    _logging_level = None
    if args.verbose:
        _logging_level=logging.DEBUG
    else:
        _logging_level=logging.WARN
    logging.basicConfig(format="%(asctime)s [%(levelname)-5.5s] %(message)s", level=_logging_level)

    logging.debug(f"[Main] Is batch mode enabled?")
    files = {}
    if args.batch:
        if os.path.isdir(args.input_file) and os.path.isdir(args.output_file):
            logging.debug(f"[Main] Batch mode enabled and both dirs are valid.")
            for file in os.listdir(args.input_file):
                from_file = f"{args.input_file}\\{file}"
                to_file = f"{args.output_file}\\{file[:file.rfind('.')]}.{args.batch}"
                if not(args.overwrite) and os.path.isfile(to_file):
                    logging.warning(f"[Main] Overwrite mode is not enabled and {to_file} already exists.")
                    continue
                files[from_file] = to_file
                logging.debug(f"[Main] File {from_file} should be transformed into {files[from_file]} file.")
        else:
            logging.critical(f"[Main] Batch mode is enabled but input or output directories do not exist or are not directories.")
            exit(-1)
    else: # Batch mode is not enabled
        if os.path.isfile(args.input_file):
            logging.debug(f"[Main] Single file mode enabled and input file exists.")
            files[args.input_file] = args.output_file
            logging.debug(f"[Main] File {args.input_file} should be transformed into {files[args.input_file]} file.")
        else:
            logging.critical(f"[Main] Single file mode is enabled but input file does not exist or is not a file.")
            exit(-1)
    
    logging.debug(f"[Main] About to process file(s).")
    for file in files:
        logging.debug(f"[Main] About to transform file {file} into {files[file]}.")
        input_file_extension = file[file.rfind(".")+1:]
        output_file_extension = (files[file])[files[file].rfind(".")+1:]

        logging.debug(f"[Main] About to select transformer ({input_file_extension} --> {output_file_extension}).")
        selector = Transformer_selector(input_file_extension, output_file_extension)
        transformer_selected = selector.select()
        logging.debug(f"[Main] The selected transformer is {transformer_selected}.")

        logging.debug(f"[Main] About to build transformer_builder.")
        transformer_builder = None
        if transformer_selected == "csv_to_json":
            if args.delimiter:
                logging.debug(f"[Main] About to process delimiter.")
                transformer_builder = Delimiter_TransformerBuilder(file, "_temp", args.overwrite, args.delimiter)
                delimiter_transformer = transformer_builder.build()
                delimiter_transformer.transform()
                logging.debug(f"[Main] About to build transformer_builder.")
                transformer_builder = CSV_to_JSON_TransformerBuilder(delimiter_transformer.getOutputFile(), files[file], args.overwrite)
            else:
                logging.debug(f"[Main] About to build transformer_builder.")
                transformer_builder = CSV_to_JSON_TransformerBuilder(file, files[file], args.overwrite)
        else:
            logging.warning(f"[Main] There is no transformer available.")
            continue
            
        logging.debug(f"[Main] About to build transformer.")
        transformer = transformer_builder.build()
        transformer.transform()


if __name__ == "__main__":
    main()
