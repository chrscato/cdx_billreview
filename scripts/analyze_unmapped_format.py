#!/usr/bin/env python3
"""
analyze_unmapped_format.py

Analyzes the format of JSON files in the unmapped folder to identify any anomalies
or deviations from the expected structure.
"""

import os
import sys
import json
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Any
from dotenv import load_dotenv

# Add project root to Python path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

# Import S3 utilities
from utils.s3_utils import list_objects, download, get_s3_json

# Load environment variables
load_dotenv()
S3_BUCKET = os.getenv('S3_BUCKET')
MAPPED_PREFIX = 'data/hcfa_json/valid/mapped/'

def get_all_keys(obj: Dict[str, Any], prefix: str = '') -> Set[str]:
    """Recursively get all keys in a nested dictionary."""
    keys = set()
    for key, value in obj.items():
        current_key = f"{prefix}.{key}" if prefix else key
        keys.add(current_key)
        if isinstance(value, dict):
            keys.update(get_all_keys(value, current_key))
        elif isinstance(value, list) and value and isinstance(value[0], dict):
            for item in value:
                keys.update(get_all_keys(item, current_key))
    return keys

def analyze_file_structure(key: str) -> Dict[str, Any]:
    """Analyze the structure of a single JSON file."""
    try:
        # Get the JSON data
        json_data = get_s3_json(key)
        
        # Get all keys in the structure
        all_keys = get_all_keys(json_data)
        
        # Check for required sections
        required_sections = {'patient_info', 'billing_info', 'service_lines'}
        missing_sections = required_sections - {k.split('.')[0] for k in all_keys}
        
        # Analyze service lines structure
        service_lines = json_data.get('service_lines', [])
        service_line_keys = set()
        if service_lines:
            service_line_keys = get_all_keys(service_lines[0])
        
        return {
            'filename': os.path.basename(key),
            'all_keys': all_keys,
            'missing_sections': missing_sections,
            'service_line_keys': service_line_keys,
            'num_service_lines': len(service_lines),
            'error': None
        }
    except Exception as e:
        return {
            'filename': os.path.basename(key),
            'error': str(e)
        }

def main():
    print("🔍 Analyzing files in mapped folder...")
    
    # Get all JSON files in mapped folder
    all_files = list_objects(MAPPED_PREFIX)
    print(f"Total files found: {len(all_files)}")
    
    # Count files at different depths
    depth_counts = defaultdict(int)
    for f in all_files:
        depth = f.count('/')
        depth_counts[depth] += 1
    
    print("\nFiles by depth:")
    for depth, count in sorted(depth_counts.items()):
        print(f"Depth {depth}: {count} files")
    
    # Filter for JSON files at depths 5 and 6
    json_files = [f for f in all_files if f.endswith('.json') and f.count('/') in [5, 6]]
    
    print(f"\nJSON files at depths 5 and 6: {len(json_files)}")
    
    # Analyze each file
    results = []
    for key in json_files:
        results.append(analyze_file_structure(key))
    
    # Group files by structure
    structure_groups = defaultdict(list)
    for result in results:
        if result['error']:
            structure_groups['ERROR'].append(result)
        else:
            # Create a structure signature based on keys
            signature = (
                frozenset(result['all_keys']),
                frozenset(result['service_line_keys']),
                result['num_service_lines']
            )
            structure_groups[signature].append(result)
    
    # Print analysis results
    print("\n📊 Analysis Results:")
    print(f"Total files analyzed: {len(json_files)}")
    print(f"Number of unique structures: {len(structure_groups)}")
    
    if 'ERROR' in structure_groups:
        print("\n❌ Files with errors:")
        for result in structure_groups['ERROR']:
            print(f"  - {result['filename']}: {result['error']}")
    
    print("\n📋 Structure Groups:")
    for i, (signature, files) in enumerate(structure_groups.items(), 1):
        if signature == 'ERROR':
            continue
            
        print(f"\n=== Group {i} ({len(files)} files) ===")
        if isinstance(signature, tuple):  # Normal structure group
            print("\nCommon keys:")
            for key in sorted(signature[0]):
                print(f"  - {key}")
            
            print("\nService line keys:")
            for key in sorted(signature[1]):
                print(f"  - {key}")
            
            print(f"\nNumber of service lines: {signature[2]}")
            
            print("\nFiles in this group:")
            for file in files:
                if file['missing_sections']:
                    print(f"  - {file['filename']} (Missing sections: {', '.join(file['missing_sections'])})")
                else:
                    print(f"  - {file['filename']}")
        
        print("-" * 80)

if __name__ == "__main__":
    main() 