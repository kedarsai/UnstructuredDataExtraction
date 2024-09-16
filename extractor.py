# extractor.py

import os
import re
import pdfplumber
import pytesseract
from pdf2image import convert_from_path
from PIL import Image

class PDFExtractor:
    def __init__(self, poppler_path, tesseract_cmd):
        self.poppler_path = poppler_path
        pytesseract.pytesseract.tesseract_cmd = tesseract_cmd

    def is_scanned_pdf(self, pdf_path, page_num=0):
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[page_num]
            text = page.extract_text()
            return not text or not text.strip()

    def preprocess_image(self, image):
        image = image.convert('L')
        return image

    def extract_text_from_text_based_pdf(self, pdf_path):
        pages_text = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                pages_text.append(page_text)
        return pages_text

    def extract_text_from_scanned_pdf(self, pdf_path):
        pages = convert_from_path(pdf_path, poppler_path=self.poppler_path)
        pages_text = []
        for page_image in pages:
            page_image = self.preprocess_image(page_image)
            page_text = pytesseract.image_to_string(page_image)
            pages_text.append(page_text)
        return pages_text

    def process_fields(self, fields_config, pages_text, pdf_path, is_scanned):
        extracted_data = {}
        for field_name, field_info in fields_config.items():
            parser_type = field_info.get('parser')
            if parser_type == 'regex':
                all_text = "\n".join(pages_text)
                regex_pattern = field_info.get('regex')
                matches = re.search(regex_pattern, all_text)
                extracted_value = matches.group(1) if matches else None
                extracted_data[field_name] = extracted_value
            elif parser_type == 'area':
                area = field_info.get('area')
                if isinstance(area, str):
                    area = eval(area)
                extracted_text = self.extract_text_from_area(pdf_path, area, is_scanned)
                extracted_data[field_name] = extracted_text
            else:
                extracted_data[field_name] = None
        return extracted_data

    def extract_text_from_area(self, pdf_path, area, is_scanned):
        extracted_text = None
        if is_scanned:
            pages = convert_from_path(pdf_path, poppler_path=self.poppler_path)
            for page_image in pages:
                x0, y0, x1, y1 = area
                width, height = page_image.size
                pil_bbox = (x0, height - y1, x1, height - y0)
                cropped_image = page_image.crop(pil_bbox)
                cropped_image = self.preprocess_image(cropped_image)
                area_text = pytesseract.image_to_string(cropped_image)
                if area_text.strip():
                    extracted_text = area_text.strip()
                    break
        else:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    cropped_page = page.crop(bbox=area)
                    area_text = cropped_page.extract_text()
                    if area_text:
                        extracted_text = area_text.strip()
                        break
        return extracted_text

    def extract_data_from_pdf(self, pdf_path, fields_config):
        is_scanned = self.is_scanned_pdf(pdf_path)
        if is_scanned:
            pages_text = self.extract_text_from_scanned_pdf(pdf_path)
        else:
            pages_text = self.extract_text_from_text_based_pdf(pdf_path)
        extracted_data = self.process_fields(fields_config, pages_text, pdf_path, is_scanned)
        return extracted_data
