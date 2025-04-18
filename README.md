# Bill Review System

A comprehensive system for processing and managing medical bills.

## Project Structure

- `portal/` - Web interface for the bill review system
- `preprocess/` - Data preprocessing component
- `processs/` - Main processing component
- `outreach/` - Outreach and communication component
- `utils/` - Shared utility functions and tools
- `scripts/` - Utility scripts and automation tools

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
- Copy `.env.example` to `.env`
- Update the values in `.env` with your configuration

## Development

Each component has its own documentation and setup instructions in its respective directory.

## Security

- Never commit sensitive files (`.env`, `googlecloud.json`, etc.)
- Keep database backups secure
- Follow security best practices for handling medical data 