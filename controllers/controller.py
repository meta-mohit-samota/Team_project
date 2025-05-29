# from fastapi import APIRouter, HTTPException 
# from models.schemas import InputModel
# # from models.schemas import APIRequest
# from services.file_joiner import join_files 
# from services.filter_process import apply_filters 
# from utils.file_reader import read_file 
# # from utils.path_util import get_output_path 
# from utils.path_util import INPUT_DIR, OUTPUT_DIR
# from utils.logger import logger
# from utils.fileName_generator import generate_filename
# import json

# router = APIRouter()

# @router.post("/process") 
# def process_files(request: InputModel): 
#     try: 
#         join_result = join_files(request.files_and_join_info.primary_file, request.files_and_join_info.secondary_files)

#         for condition in request.filter:
#             df = read_file(condition.file_name)
#             filtered_df = apply_filters(df, condition.conditions)
#             # output_path = get_output_path(f"filtered_{condition.fileName}")
#             output_path = OUTPUT_DIR / generate_filename()
#             filtered_df.write_csv(output_path)

#         # logger.info(json.dumps(request,indent=2))
#         # # for key,val in request.json.items():
#         # #     logger.info(key+"\t"+val+"\n")
#         # return {json.dumps(request,indent=2)}

#         return {"message": "Files processed successfully"}
#     except Exception as e:
#         logger.error(f"API error: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


from fastapi import APIRouter, HTTPException
from models.schemas import InputModel
from services.file_joiner import join_files
from services.filter_process import apply_filters
from utils.file_reader import read_file
from utils.path_util import OUTPUT_DIR
from utils.logger import logger
from utils.fileName_generator import generate_filename

router = APIRouter()

@router.post("/process")
def process_files(request: InputModel):
    try:
        # Join files
        # join_result = join_files(
        #     request.files_and_join_info.primary_file,
        #     request.files_and_join_info.secondary_files
        # )
        # df_map = {request.files_and_join_info}
        # if request.files_and_join_info.secondary_files:
        #     for 
        # # Apply filters if provided

        df = read_file(condition.file_name)
        logger.info(df)

        # if request.filter:
        #     for condition in request.filter:
                # filtered_df = apply_filters(df, condition.conditions)
                # output_path = OUTPUT_DIR / generate_filename()
                # filtered_df.write_csv(output_path)

        logger.info(request)
        return {"message": request}
    except Exception as e:
        logger.error(f"API error: {e}", exc_info=True)
        return {"error came":"yes"}
        # raise HTTPException(status_code=500, detail=str(e))