import boto3
import os
from pdf2image import convert_from_bytes
from PIL import Image
import io
import tempfile

def get_s3_client():
    return boto3.client('s3')

def download_pdf_from_s3(bucket, key):
    """Download PDF from S3 and return as bytes"""
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()

def upload_image_to_s3(image, bucket, key):
    """Upload PIL Image to S3 and return the full URL"""
    s3 = get_s3_client()
    
    # Save image to bytes
    img_byte_arr = io.BytesIO()
    image.save(img_byte_arr, format='PNG')
    img_byte_arr = img_byte_arr.getvalue()
    
    # Upload to S3
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=img_byte_arr,
        ContentType='image/png'
    )
    
    # Generate the URL
    url = f"https://{bucket}.s3.amazonaws.com/{key}"
    return url

def process_pdf(filename, bucket="bill-review-prod"):
    """Process a PDF file from S3 and create cropped preview images"""
    
    # Construct S3 paths
    source_key = f"data/hcfa_pdf/archived/{filename}"
    base_filename = os.path.splitext(filename)[0]
    preview_prefix = f"data/hcfa_pdf/previews/{base_filename}"
    
    print(f"Processing PDF: {filename}")
    
    try:
        # Download PDF from S3
        pdf_bytes = download_pdf_from_s3(bucket, source_key)
        
        # Convert first page to image
        with tempfile.TemporaryDirectory() as path:
            images = convert_from_bytes(pdf_bytes, first_page=1, last_page=1, dpi=200, path=path)
            if not images:
                raise Exception("Failed to convert PDF to image")
            
            page = images[0]
            width, height = page.size
            
            # Calculate crop dimensions
            header_height = int(height * 0.25)  # top 25%
            service_lines_height = int(height * 0.40)  # middle 40%
            footer_start = int(height * 0.75)  # bottom 25%
            
            # Crop sections
            header = page.crop((0, 0, width, header_height))
            service_lines = page.crop((0, header_height, width, header_height + service_lines_height))
            footer = page.crop((0, footer_start, width, height))
            
            # Upload cropped images to S3
            sections = {
                'header': header,
                'service_lines': service_lines,
                'footer': footer
            }
            
            urls = {}
            for section_name, image in sections.items():
                key = f"{preview_prefix}/{section_name}.png"
                url = upload_image_to_s3(image, bucket, key)
                urls[section_name] = url
                print(f"{section_name.title()} image URL: {url}")
            
            return urls
            
    except Exception as e:
        print(f"Error processing {filename}: {str(e)}")
        raise

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python process_pdf_preview.py <filename>")
        sys.exit(1)
    
    filename = sys.argv[1]
    process_pdf(filename) 