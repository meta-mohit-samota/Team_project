from fastapi import APIRouter, HTTPException 
from models.schemas import FileProcessingRequest 
from services.file_joiner import join_files 
from services.filter_process import apply_filters 
from utils.file_reader import read_file 
# from utils.path_util import get_output_path 
from utils.path_util import INPUT_DIR, OUTPUT_DIR
from utils.logger import logger
from utils.fileName_generator import generate_filename

router = APIRouter()

@router.post("/process") 
def process_files(request: FileProcessingRequest): 
    try: 
        join_result = join_files(request.files_and_join_info.primary_file, request.files_and_join_info.secondary_files)

        for condition in request.filter:
            df = read_file(condition.fileName)
            filtered_df = apply_filters(df, condition.conditions)
            # output_path = get_output_path(f"filtered_{condition.fileName}")
            output_path = OUTPUT_DIR / generate_filename()
            filtered_df.write_csv(output_path)

        return {"message": "Files processed successfully"}
    except Exception as e:
        logger.error(f"API error: {e}")
        raise HTTPException(status_code=500, detail=str(e))