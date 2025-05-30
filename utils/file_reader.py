import polars as pl
import os
from enum import Enum
from typing import Callable, Dict
from utils.path_util import getFullInputPath 

class FileExtension(Enum):
    """
    Enumerates supported file extensions for data reading.
    """
    CSV = ".csv"
    JSON = ".json"
    EXCEL_XLS = ".xls"
    EXCEL_XLSX = ".xlsx"
    PARQUET = ".parquet"
    TSV = ".tsv"
    IPC = ".ipc"
    FEATHER = ".feather"

# Define a type hint for the Polars reader functions
PolarsReaderFunction = Callable[[str], pl.DataFrame]

# Map file extensions to their corresponding Polars reading functions
# This acts as our "switch-case" for data loading.
FILE_READER_MAP: Dict[FileExtension, PolarsReaderFunction] = {
    FileExtension.CSV: pl.read_csv,
    FileExtension.JSON: pl.read_json,
    FileExtension.EXCEL_XLS: pl.read_excel,  # .xls and .xlsx use the same reader
    FileExtension.EXCEL_XLSX: pl.read_excel,
    FileExtension.PARQUET: pl.read_parquet,
    FileExtension.TSV: lambda path: pl.read_csv(path, separator='\t'), # Custom reader for TSV
    FileExtension.IPC: pl.read_ipc,
    FileExtension.FEATHER: pl.read_ipc, # .ipc and .feather use the same reader
}

def load_data_from_file(file_path: str) -> pl.DataFrame:
    """
    Loads data from a specified file path into a Polars DataFrame.

    Supports CSV, JSON, Excel (.xls, .xlsx), Parquet, TSV, IPC, and Feather formats.

    Args:
        file_path (str): The relative path to the file within the configured input directory.

    Returns:
        pl.DataFrame: The loaded data as a Polars DataFrame.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        ValueError: If the file type is unsupported or an error occurs during reading.
        pl.PolarsError: If Polars encounters an issue during data parsing (e.g., malformed file).
    """
    # Step 1: Get the full absolute path
    absolute_file_path = getFullInputPath(file_path)

    # Step 2: Validate file existence
    if not os.path.exists(absolute_file_path):
        raise FileNotFoundError(f"File not found: {absolute_file_path}")
    if not os.path.isfile(absolute_file_path):
        raise ValueError(f"Path is not a file: {absolute_file_path}")

    # Step 3: Determine file extension
    file_extension_str = os.path.splitext(absolute_file_path)[-1].lower()

    # Step 4: Map extension to FileExtension enum and get reader function
    try:
        # Find the enum member that matches the file extension string
        # This handles cases where multiple extensions map to the same reader (e.g., .xls/.xlsx)
        matching_enum_member = next(
            (ext_enum for ext_enum in FileExtension if ext_enum.value == file_extension_str),
            None
        )

        if matching_enum_member is None:
            raise ValueError(f"Unsupported file type: '{file_extension_str}'. "
                             f"Supported types are: {[ext.value for ext in FileExtension]}.")

        data_reader_function = FILE_READER_MAP.get(matching_enum_member)

        if data_reader_function is None:
            # This should ideally not happen if FILE_READER_MAP is comprehensive
            raise ValueError(f"No reader function mapped for extension: '{file_extension_str}'.")

    except Exception as e:
        raise ValueError(f"Error determining reader for file '{file_path}': {e}")


    # Step 5: Read the data using try-except for robust error handling
    try:
        print(f"Loading data from: {absolute_file_path} (Type: {file_extension_str})")
        data_frame = data_reader_function(absolute_file_path)
        print(f"Data loaded successfully! Shape: {data_frame.shape}")
        return data_frame
    except pl.PolarsError as e:
        raise pl.PolarsError(f"Polars error reading file '{file_path}': {e}")
    except Exception as e:
        raise ValueError(f"An unexpected error occurred while reading file '{file_path}': {e}")