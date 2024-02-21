from transformer_builder import CSV_to_JSON_Transformer_Builder
from transformer_builder import Delimiter_Transformer_Builder
from transformer import Transformer_Selector

import argparse
import logging
import os


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
            transformer_selector = Transformer_Selector(file, files[file])
            transformer_selected = transformer_selector.select()
            if transformer_selected == "csv_to_json":
                if args.delimiter:
                    logging.debug(f"[Main] There is a delimiter to process before CSV_to_JSON_Transformer.")
                    builder = Delimiter_Transformer_Builder(file, "_temp", args.delimiter)
                    transformer = builder.build()
                    transformer.transform()
                    builder = CSV_to_JSON_Transformer_Builder(transformer.getOutputFile(), files[file])
                    transformer = builder.build()
                    transformer.transform()
                else: # There is no delimiter to process.
                    builder = CSV_to_JSON_Transformer_Builder(file, files[file])
                    transformer = builder.build()
                    transformer.transform()
            else: # There is no transformer available for this.
                logging.warning(f"[Main] Oh no...!!! There is no transformer for this {transformer_selected}.")


if __name__ == "__main__":
    app = Main()
    app.run()
