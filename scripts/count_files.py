#!/usr/bin/env python3
"""
count_files.py

Counts files in the mapped folder and its subfolders.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add project root to Python path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from utils.s3_utils import list_objects

# Load environment variables
load_dotenv()
S3_BUCKET = os.getenv('S3_BUCKET')
MAPPED_PREFIX = 'data/hcfa_json/valid/mapped/'

def main():
    print("🔍 Counting files in mapped folder...")
    
    # Get all files
    all_files = list_objects(MAPPED_PREFIX)
    
    # Count files at different depths
    depth_counts = {}
    json_counts = {}
    depth_examples = {}  # Store example paths for each depth
    
    for f in all_files:
        depth = f.count('/')
        depth_counts[depth] = depth_counts.get(depth, 0) + 1
        
        if f.endswith('.json'):
            json_counts[depth] = json_counts.get(depth, 0) + 1
            
        # Store example path for this depth
        if depth not in depth_examples:
            depth_examples[depth] = f
    
    print("\nTotal files by depth:")
    for depth in sorted(depth_counts.keys()):
        print(f"\nDepth {depth}: {depth_counts[depth]} files ({json_counts.get(depth, 0)} JSON files)")
        print(f"Example path: {depth_examples[depth]}")
    
    # Count files in root folder
    root_files = [f for f in all_files if f.count('/') == 5]
    root_json = [f for f in root_files if f.endswith('.json')]
    
    print(f"\nRoot folder (depth 5):")
    print(f"Total files: {len(root_files)}")
    print(f"JSON files: {len(root_json)}")
    if root_files:
        print("Example files:")
        for f in root_files[:3]:  # Show first 3 files
            print(f"  - {f}")
    
    # Count files in subfolders
    subfolder_files = [f for f in all_files if f.count('/') == 6]
    subfolder_json = [f for f in subfolder_files if f.endswith('.json')]
    
    print(f"\nSubfolders (depth 6):")
    print(f"Total files: {len(subfolder_files)}")
    print(f"JSON files: {len(subfolder_json)}")
    if subfolder_files:
        print("Example files:")
        for f in subfolder_files[:3]:  # Show first 3 files
            print(f"  - {f}")

if __name__ == "__main__":
    main() 