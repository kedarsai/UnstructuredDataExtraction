# config.py

import os

# Paths to external dependencies
POPPLER_PATH = os.getenv('POPPLER_PATH', r'C:\Users\KedarsaiPadigala\OneDrive - Syniti\Desktop\LLM\Projects\poppler-24.07.0\Library\bin')  # Update as needed
TESSERACT_CMD = os.getenv('TESSERACT_CMD', r'C:\Users\KedarsaiPadigala\AppData\Local\Programs\Tesseract-OCR\tesseract.exe')  # Update as needed

# Database configuration
SERVER = os.getenv('SQL_SERVER', 'AECSP-KPADIGALA\KEDAR')
DATABASE = os.getenv('SQL_DATABASE', 'practice')  # Update as needed

# Table name
TABLE_NAME = 'contracts5'  # Update as needed
