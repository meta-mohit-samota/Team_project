from fastapi import FastAPI,HTTPException
from controllers.controller import router as processor_router
from pydantic import ValidationError
from models import schemas

app = FastAPI(title="File Processing API")

# @app.post("/",tags = ["Root"])
# def upload_data(payload: schemas):
#     try:
#         return {"message":"Payload is valid","data":payload.model_dump()}
#     except ValidationError as e:
#         raise HTTPException(status_code = 400,detail = str(e))

app.include_router(processor_router)