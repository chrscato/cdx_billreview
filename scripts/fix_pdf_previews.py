#!/usr/bin/env python3
"""
repair_invalid_previews.py

Finds all invalid JSONs and regenerates PDF preview crops from archived PDFs.
Overwrites any existing preview images.
"""

import os
import sys
import tempfile
import logging
from pathlib import Path
import boto3
import fitz
from PIL import Image
from dotenv import load_dotenv

# Load env
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))
load_dotenv(project_root / ".env")

logger = logging.getLogger("RepairInvalidPreviews")
s3 = boto3.client("s3")

BUCKET = os.getenv("S3_BUCKET", "bill-review-prod")
INVALID_PREFIX = "data/hcfa_json/invalid/"
ARCHIVE_PDF_PREFIX = "data/hcfa_pdf/archived/"
PREVIEW_PREFIX = "data/hcfa_pdf/preview/"

def generate_and_upload_previews(pdf_filename: str):
    """Uses archived PDF to regenerate clean preview crops and upload to preview folder."""
    base_name = Path(pdf_filename).stem

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_pdf_path = Path(tmp_dir) / pdf_filename

        # Download archived PDF
        s3.download_file(BUCKET, f"{ARCHIVE_PDF_PREFIX}{pdf_filename}", str(local_pdf_path))
        pdf = fitz.open(str(local_pdf_path))
        page = pdf[0]

        # Render page at 300 DPI
        zoom = 300 / 72
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        width, height = image.size

        # Define crop ranges
        header_end = int(height * 0.23)
        footer_start = int(height * 0.91)

        # Crop sections
        header = image.crop((0, 0, width, header_end))
        service_lines = image.crop((0, header_end, width, footer_start))
        footer = image.crop((0, footer_start, width, height))

        # Save & upload each
        for label, img in {
            "header.png": header,
            "service_lines.png": service_lines,
            "footer.png": footer
        }.items():
            local_img_path = Path(tmp_dir) / label
            img.save(local_img_path, format="PNG")

            s3_key = f"{PREVIEW_PREFIX}{base_name}/{label}"
            s3.upload_file(str(local_img_path), BUCKET, s3_key, ExtraArgs={"ContentType": "image/png"})
            logger.info(f"✔️ Uploaded {s3_key}")

        pdf.close()

def repair_all():
    # List all invalid JSONs
    logger.info("🔍 Scanning for invalid JSONs...")
    result = s3.list_objects_v2(Bucket=BUCKET, Prefix=INVALID_PREFIX)
    if "Contents" not in result:
        logger.info("✅ No files found in invalid/")
        return

    json_keys = [obj["Key"] for obj in result["Contents"] if obj["Key"].endswith(".json")]

    logger.info(f"🔧 Found {len(json_keys)} invalid JSONs")

    for json_key in json_keys:
        try:
            base_filename = Path(json_key).stem
            pdf_filename = f"{base_filename}.pdf"

            # Check if the archived PDF exists first
            s3.head_object(Bucket=BUCKET, Key=f"{ARCHIVE_PDF_PREFIX}{pdf_filename}")
            logger.info(f"🛠️  Reprocessing {pdf_filename}")
            generate_and_upload_previews(pdf_filename)
        except s3.exceptions.ClientError as e:
            logger.warning(f"⚠️ PDF not found for {json_key}, skipping. Error: {str(e)}")
        except Exception as ex:
            logger.error(f"❌ Failed to process {json_key}: {str(ex)}", exc_info=True)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    repair_all()
