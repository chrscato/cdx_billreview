# Bill Review Portal Documentation

## Overview
The Bill Review Portal is a web application designed to manage and process HCFA (Health Care Financing Administration) forms. It provides a streamlined workflow for uploading, reviewing, and processing medical billing documents.

## System Architecture

### Frontend
- Built with Flask (Python web framework)
- Uses Bootstrap for UI components
- Implements responsive design for all screen sizes
- Uses Font Awesome and Bootstrap Icons for icons

### Backend
- Python-based processing pipeline
- AWS S3 for file storage
- FileMaker integration for data mapping

### Key Components
1. **Preprocessing Module**
   - Handles initial file upload and processing
   - OCR correction workflow
   - Unmapped file management
   - PDF preview generation

2. **File Storage**
   - S3 bucket structure:
     - `data/hcfa_pdf/archived/`: Original PDF files
     - `data/hcfa_pdf/preview/`: Generated preview images
     - `data/hcfa_json/valid/`: Processed JSON files
     - `data/hcfa_json/valid/mapped/`: Successfully mapped files
     - `data/hcfa_json/valid/unmapped/`: Files requiring mapping
     - `data/hcfa_json/invalid/`: Files with OCR errors

## Workflow

### 1. File Upload (Dropoff)
- Accepts PDF files
- Validates file format
- Uploads to S3 archived directory
- Triggers preprocessing pipeline

### 2. Preprocessing Pipeline
- Converts PDF to JSON
- Extracts key information:
  - Patient Information
  - Billing Information
  - Service Lines
- Generates preview images
- Classifies files as valid/invalid

### 3. OCR Correction (Invalid Files)
- Files with OCR errors are moved to invalid directory
- Users can:
  - View original PDF
  - Edit extracted data
  - Save corrections
  - Approve and move to valid directory

### 4. Mapping Review (Unmapped Files)
- Files requiring mapping are moved to unmapped directory
- Users can:
  - View original PDF
  - Edit extracted data
  - Save changes
  - Approve and move to mapped directory

## User Interface

### Navigation
- Main Dashboard
  - HCFA Dropoff
  - OCR Corrections
  - Unmapped Files

### File Management
- List Views
  - Shows file count badges
  - Provides quick access to files
  - Supports pagination

### File Editor
- Patient Information Section
  - Patient Name
  - Date of Birth
  - ZIP Code

- Billing Information Section
  - Provider Name
  - Provider NPI
  - Provider Address
  - Provider TIN
  - Patient Account No
  - Total Charge

- Service Lines Section
  - Dynamic line addition/removal
  - CPT Code
  - Charge Amount
  - Date of Service
  - Units
  - Place of Service
  - Diagnosis
  - Modifiers

### PDF Viewer
- Presigned URL generation
- 1-hour expiration
- Direct S3 access

## API Endpoints

### Preprocessing Routes
- `/preprocessing/`
  - Main dashboard
  - Shows file counts

- `/preprocessing/dropoff`
  - File upload interface
  - PDF validation

- `/preprocessing/invalid`
  - Lists files with OCR errors
  - Shows invalid count

- `/preprocessing/invalid/<filename>`
  - File editor for OCR corrections
  - PDF preview

- `/preprocessing/invalid/<filename>/pdf`
  - Generates presigned URL for PDF
  - 1-hour expiration

- `/preprocessing/invalid/<filename>/preview/<section>`
  - Generates presigned URL for preview images
  - 1-hour expiration

- `/preprocessing/unmapped`
  - Lists unmapped files
  - Shows unmapped count

- `/preprocessing/unmapped/<filename>`
  - File editor for mapping
  - PDF preview

- `/preprocessing/unmapped/<filename>/pdf`
  - Generates presigned URL for PDF
  - 1-hour expiration

## File Formats

### JSON Structure
```json
{
  "patient_info": {
    "patient_name": "string",
    "patient_dob": "string (MM/DD/YYYY)",
    "patient_zip": "string"
  },
  "billing_info": {
    "billing_provider_name": "string",
    "billing_provider_npi": "string",
    "billing_provider_address": "string",
    "billing_provider_tin": "string",
    "patient_account_no": "string",
    "total_charge": "string ($0.00)"
  },
  "service_lines": [
    {
      "cpt_code": "string",
      "charge_amount": "string ($0.00)",
      "date_of_service": "string (MM/DD/YY - MM/DD/YY)",
      "units": "string",
      "place_of_service": "string",
      "diagnosis_pointer": "string",
      "modifiers": ["string"]
    }
  ]
}
```

## Security

### Authentication
- User authentication required
- Role-based access control

### File Access
- Presigned URLs for S3 access
- 1-hour expiration
- Content-type restrictions

### Data Validation
- Input sanitization
- File type validation
- Data format validation

## Error Handling

### User Interface
- Error messages for:
  - File upload failures
  - PDF access errors
  - Data validation errors
  - Network errors

### Backend
- Logging system
- Error tracking
- Exception handling

## Performance Considerations

### File Processing
- Asynchronous processing
- Batch operations
- Progress tracking

### UI Responsiveness
- Lazy loading
- Pagination
- Caching

## Integration Points

### FileMaker
- Data mapping
- Record matching
- Fuzzy search

### AWS S3
- File storage
- Presigned URLs
- Bucket management

## Development Guidelines

### Code Structure
- Modular design
- Separation of concerns
- Clear naming conventions

### Testing
- Unit tests
- Integration tests
- UI tests

### Documentation
- Code comments
- API documentation
- User guides

## Deployment

### Requirements
- Python 3.x
- Flask
- AWS credentials
- FileMaker access

### Environment Variables
- `S3_BUCKET`: AWS S3 bucket name
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `FILEMAKER_*`: FileMaker connection details

### Configuration
- Development environment
- Production environment
- Testing environment

## Maintenance

### Regular Tasks
- Log rotation
- File cleanup
- Performance monitoring

### Updates
- Version control
- Change management
- Rollback procedures

## Troubleshooting

### Common Issues
- File upload failures
- PDF access errors
- Mapping failures
- Performance issues

### Debugging
- Log analysis
- Error tracking
- Performance profiling

## Future Enhancements

### Planned Features
- Advanced search
- Batch operations
- Reporting
- Analytics

### Technical Improvements
- Performance optimization
- UI enhancements
- Integration expansion 