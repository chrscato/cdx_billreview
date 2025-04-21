#!/usr/bin/env python3
"""
pdf_preview_sections.py

Improved PDF preview generator that cleanly crops header, service lines, and footer
based on relative height ranges. Uses PyMuPDF and Pillow.
"""

import os
import sys
import logging
import tempfile
from pathlib import Path
import fitz  # PyMuPDF
import boto3
from PIL import Image
from dotenv import load_dotenv

# Load project and env
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))
load_dotenv(project_root / ".env")

from utils.s3_utils import download  # Update this if using different helpers

def generate_pdf_sections(pdf_filename: str):
    logger = logging.getLogger("PDF Section Generator")
    s3_client = boto3.client('s3')
    bucket = os.getenv("S3_BUCKET", "bill-review-prod")
    input_prefix = "data/hcfa_pdf/"
    output_prefix = "data/hcfa_pdf/preview/"

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_path = Path(tmp_dir)
            pdf_path = temp_path / pdf_filename

            # Download PDF
            s3_client.download_file(bucket, f"{input_prefix}{pdf_filename}", str(pdf_path))
            logger.info(f"Downloaded {pdf_filename}")

            # Open PDF
            pdf = fitz.open(str(pdf_path))
            page = pdf[0]

            # Render at 300 DPI
            zoom = 300 / 72
            mat = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            width, height = image.size

            # Set crop boundaries
            header_end = int(height * 0.23)
            footer_start = int(height * 0.91)

            # Crop sections
            header = image.crop((0, 0, width, header_end))
            service_lines = image.crop((0, header_end, width, footer_start))
            footer = image.crop((0, footer_start, width, height))

            base_name = Path(pdf_filename).stem
            output_files = {
                "header.png": header,
                "service_lines.png": service_lines,
                "footer.png": footer
            }

            for name, img in output_files.items():
                out_path = temp_path / name
                img.save(out_path, format="PNG")
                s3_key = f"{output_prefix}{base_name}/{name}"
                s3_client.upload_file(str(out_path), bucket, s3_key, ExtraArgs={"ContentType": "image/png"})
                logger.info(f"Uploaded: {s3_key}")

            pdf.close()

    except Exception as e:
        logger.error(f"Failed to process {pdf_filename}: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    # Example use
    generate_pdf_sections("example_form.pdf")
