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


class TransformerBuilder(ABC):
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
        return CSV_to_JSON_Transformer(self._input_file, self._output_file)


class Delimiter_TransformerBuilder (TransformerBuilder):
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


class Transformer_selector:
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


class Main:

    def get_args(self):
        """
        Sets command line args.
        """
        parser = argparse.ArgumentParser(
            description = "This commands allows to easily transforms data from one format files to another. The allowed transformations include: CSV --> JSON.",
            formatter_class= argparse.ArgumentDefaultsHelpFormatter
        )
        parser.add_argument(
            "input_file",
            help="the file or directory where the data is stored."
        )
        parser.add_argument(
            "output_file",
            help="the file or directory where the data will be stored after transforming it."
        )
        parser.add_argument(
            "-b", "--batch",
            help="use this param if wanna transform all files inside a directory to the choosen format (csv, txt, json, etc.)."
        )
        parser.add_argument(
            "-d", "--delimiter",
            help="use this param if wanna set a delimiter"
        )
        parser.add_argument(
            "-o", "--overwrite",
            action="store_true",
            help="use this param if wanna allow file overwrite."
        )
        parser.add_argument(
            "-v", "--verbose",
            action="store_true",
            help="enables detailed logs."
        )
        args = parser.parse_args()
        return args
    
    def get_batch_files(self, input_path, output_path, format):
        """
        Creates a dictionary that contais every input file related to its output file as k, v.

        Parameters
        ----------
        input_path : str
            the full directory path where files are placed.
        output_path : str
            the full directory path where files will be placed when transformed.
        format : str
            the extension that files will have when transformed.

        Returns a dictionary that contains every file in the input path related to the output file as k, v.
        """
        files = {}
        for file in self.get_files_in_directory(input_path):
            logging.debug(f"[Main] About to get output file for {file}.")
            from_file = f"{input_path}/{file}"
            to_file = self.get_output_file_from_input_file(output_path, file, format)
            files[from_file] = to_file
        return files
    
    def get_files_in_directory(self, path):
        """
        Gets all filenames inside a directory.
        """
        return os.listdir(path)
    
    def get_output_file_from_input_file(self, path, file, format):
        """
        Creates a full filepath with the given path, filename and extension.

        Parameters
        ----------
        path : str
            the full directory path to validate.
        file : str
            the filename.
        format : str
            the file extension.

        Returns the full filepath (path/file.extension)
        """
        filename = file[:file.rfind('.')]
        return f"{path}/{filename}.{format}"
    
    def set_logging(self, verbose=False):
        """
        Sets the basic config for logging, including verbose mode.
        """
        if verbose:
            logging.basicConfig(format="%(asctime)s [%(levelname)-5.5s] %(message)s", level=logging.DEBUG)
        else:
            logging.basicConfig(format="%(asctime)s [%(levelname)-5.5s] %(message)s", level=logging.WARN)

    def validate_directory_path(self, path):
        """
        Check if the directory path exists and is a directory.

        Parameters
        ----------
        path : str
            the full directory path to validate.

        Returns the path if it's valid. It raises an error if not.
        """
        if not os.path.exists(path):
            raise argparse.ArgumentTypeError(f"The directory {path} does not exist.")
        if not os.path.isdir(path):
            raise argparse.ArgumentTypeError(f"{path} is not a directory.")
        return path

    def validate_file_path(self, path):
        """
        Check if the file path exists and is a file.

        Parameters
        ----------
        path : str
            the full file path to validate.

        Returns the path if it's valid. It raises an error if not.
        """
        if not os.path.exists(path):
            raise argparse.ArgumentTypeError(f"The file {path} does not exists.")
        if not os.path.isfile(path):
            raise argparse.ArgumentTypeError(f"{path} is not a file.")
        return path
    
    def validate_file_path_not_exists(self, path):
        """
        Check if the file path does NOT exists. This is used when overwrite mode is disabled.

        Parameters
        ----------
        path : str
            the full file path to validate.

        Returns the path if it's valid (the file does NOT exists). It raises an error if not.
        """
        if os.path.exists(path):
            raise argparse.ArgumentTypeError(f"The file {path} exists.")
        return path

    def run(self):
        """
        The main.
        """
        args = self.get_args()
        self.set_logging(args.verbose)

        files = {}
        logging.debug(f"[Main] About to list all files.")
        if args.batch:
            logging.debug(f"[Main] Batch mode is enabled.")
            try:
                self.validate_directory_path(args.input_file)
                self.validate_directory_path(args.output_file)
            except argparse.ArgumentTypeError:
                logging.error(f"[Main] Input or output directory does not exists.")
                exit(-1)
            files = self.get_batch_files(args.input_file, args.output_file, args.batch)
        else: # Batch mode is disabled
            logging.debug(f"[Main] Single file mode is enabled.")
            try:
                self.validate_file_path(args.input_file)
            except argparse.ArgumentTypeError:
                logging.error(f"[Main] Input file {args.input_file} does not exists.")
                exit(-1)
            files[args.input_file] = args.output_file
        logging.debug(f"[Main] All file(s) have been listed.")

        for file in files:
            print(f"{file} --> {files[file]}")
            if (not args.overwrite):
                try:
                    self.validate_file_path_not_exists(files[file])
                except argparse.ArgumentTypeError:
                    logging.warning(f"[Main] Cannot overwrite file {files[file]}.")
                    continue
            logging.debug(f"[Main] About to select Transformer.")
            transformer_selector = Transformer_selector(file, files[file])
            transformer_selected = transformer_selector.select()
            if transformer_selected == "csv_to_json":
                if args.delimiter:
                    logging.debug(f"[Main] There is a delimiter to process before CSV_to_JSON_Transformer.")
                    builder = Delimiter_TransformerBuilder(file, "_temp", args.delimiter)
                    transformer = builder.build()
                    transformer.transform()
                    builder = CSV_to_JSON_TransformerBuilder(transformer.getOutputFile(), files[file])
                    transformer = builder.build()
                    transformer.transform()
                else: # There is no delimiter to process.
                    builder = CSV_to_JSON_TransformerBuilder(file, files[file])
                    transformer = builder.build()
                    transformer.transform()
            else: # There is no transformer available for this.
                logging.warning(f"[Main] Oh no...!!! There is no transformer for this {transformer_selected}.")


if __name__ == "__main__":
    app = Main()
    app.run()
