import json
from collections import Counter
from datetime import datetime

def analyze_summary():
    # Read the JSON file
    with open('data/dashboard/failed_summary.json', 'r') as f:
        data = json.load(f)
    
    # Basic statistics
    total_files = len(data)
    
    # Count failure types
    failure_types = []
    for entry in data:
        if 'failure_types' in entry:
            failure_types.extend(entry['failure_types'])
    failure_type_counts = Counter(failure_types)
    
    # Debug information about failure types
    print("\nDetailed Failure Type Analysis:")
    print(f"Total files: {total_files}")
    print(f"Files with failure_types field: {sum(1 for entry in data if 'failure_types' in entry)}")
    print(f"Files without failure_types field: {sum(1 for entry in data if 'failure_types' not in entry)}")
    print("\nFailure types by file:")
    for entry in data:
        if 'failure_types' in entry:
            print(f"  {entry['filename']}: {entry['failure_types']}")
    
    print("\nFailure Type Counts:")
    for failure_type, count in failure_type_counts.most_common():
        print(f"  {failure_type}: {count} occurrences")
    
    # Count providers
    providers = [entry['provider'] for entry in data if 'provider' in entry]
    provider_counts = Counter(providers)
    
    # Count validation status
    validation_status = [entry['provider_validation']['is_valid'] for entry in data if 'provider_validation' in entry]
    valid_count = sum(validation_status)
    invalid_count = len(validation_status) - valid_count
    
    # Age distribution
    age_days = [entry['age_days'] for entry in data if 'age_days' in entry]
    avg_age = sum(age_days) / len(age_days) if age_days else 0
    
    # Print summary
    print(f"\nSummary of Failed Files ({total_files} total files):")
    print("\nFailure Types Distribution:")
    for failure_type, count in failure_type_counts.most_common():
        print(f"  {failure_type}: {count} occurrences")
    
    print(f"\nProvider Validation Status:")
    print(f"  Valid: {valid_count}")
    print(f"  Invalid: {invalid_count}")
    
    print(f"\nAverage Age of Files: {avg_age:.1f} days")
    
    print("\nTop 10 Providers by Failure Count:")
    for provider, count in provider_counts.most_common(10):
        print(f"  {provider}: {count} files")

if __name__ == "__main__":
    analyze_summary() 