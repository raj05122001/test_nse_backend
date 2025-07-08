def analyze_raw_lines():
    """Analyze the raw structure of CSV lines"""
    
    with open("cm_contract_stream_info.csv", 'r', encoding='utf-8') as file:
        lines = file.readlines()
    
    print(f"📊 First 10 lines analysis:")
    for i, line in enumerate(lines[:10], 1):
        print(f"\nLine {i}:")
        print(f"  Raw: '{line.strip()}'")
        print(f"  Length: {len(line.strip())}")
        print(f"  Split by space: {line.strip().split()}")
        print(f"  Split by 2+ spaces: {re.split(r'\s{2,}', line.strip())}")

if __name__ == "__main__":
    import re
    analyze_raw_lines()