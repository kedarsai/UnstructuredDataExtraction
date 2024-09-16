# main.py

from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel
from typing import Optional
import os

from extractor import PDFExtractor
from database import DatabaseManager
from utils import load_yaml_config
import config

app = FastAPI()

class ExtractionRequest(BaseModel):
    folder_path: str
    yaml_path: str
    table_name: Optional[str] = config.TABLE_NAME

@app.post("/extract")
async def extract_data(request: ExtractionRequest):
    folder_path = request.folder_path
    yaml_path = request.yaml_path
    table_name = request.table_name

    # Validate folder path
    if not os.path.isdir(folder_path):
        raise HTTPException(status_code=400, detail="Invalid folder path.")
    # Validate YAML path
    if not os.path.isfile(yaml_path):
        raise HTTPException(status_code=400, detail="Invalid YAML file path.")

    # Load YAML configuration
    fields_config = load_yaml_config(yaml_path)

    # Initialize PDFExtractor
    pdf_extractor = PDFExtractor(
        poppler_path=config.POPPLER_PATH,
        tesseract_cmd=config.TESSERACT_CMD
    )

    # Initialize DatabaseManager
    db_manager = DatabaseManager(
        server=config.SERVER,
        database=config.DATABASE
    )

    # Ensure the table exists
    if not db_manager.table_exists(table_name):
        db_manager.create_table(table_name, fields_config)
        print(f"Table '{table_name}' created.")

    # Process PDFs in the folder
    for filename in os.listdir(folder_path):
        if filename.lower().endswith('.pdf'):
            pdf_file_path = os.path.join(folder_path, filename)
            print(f"Processing file: {filename}")
            try:
                extracted_data = pdf_extractor.extract_data_from_pdf(pdf_file_path, fields_config)
                extracted_data['file_name'] = filename
                db_manager.insert_data(table_name, extracted_data, fields_config)
            except Exception as e:
                print(f"Error processing file {filename}: {e}")
                continue

    db_manager.close()
    return {"status": "success", "message": "Processing completed."}
