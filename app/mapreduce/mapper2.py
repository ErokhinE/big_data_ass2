#!/usr/bin/env python3
import sys

def main():
    for line_of_input in sys.stdin:
        try:
            line_of_input = line_of_input.strip()
            if not line_of_input:
                continue
                
            # Pass through all data from reducer1
            print(line_of_input)
            
        except Exception as e:
            sys.stderr.write(f"Error in mapper2: {str(e)}\n")

if __name__ == "__main__":
    main()