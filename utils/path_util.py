import os
from pathlib import Path
from dotenv import load_dotenv
# def get_input_path(file_name: str) -> str: 
#     return os.path.join("data/input", file_name)

# def get_output_path(file_name: str) -> str: 
#     return os.path.join("data/output", file_name)

load_dotenv() 

# BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = os.environ.get("INPUT_DIR")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR")
OUTPUT_FILENAME = "processed_output.csv"

INPUT_DIR = Path(INPUT_DIR)
OUTPUT_DIR = Path(OUTPUT_DIR)
# print(INPUT_DIR,OUTPUT_DIR)

# Ensure directories exist
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)