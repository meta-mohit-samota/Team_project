import os
from pathlib import Path
# def get_input_path(file_name: str) -> str: 
#     return os.path.join("data/input", file_name)

# def get_output_path(file_name: str) -> str: 
#     return os.path.join("data/output", file_name)

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input_files"
OUTPUT_DIR = BASE_DIR / "output_files"
OUTPUT_FILENAME = "processed_output.csv"


# Ensure directories exist
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)