#!/usr/bin/env python3
import sys

def mapper():
    for line in sys.stdin:
        line = line.strip()
        columns = line.split('\t')

        if len(columns) > 7 and columns[0] != "Name" and columns[0] != "Global":
            region = columns[1].strip()
            
            if region == "South-East Asia":
                try:
                    death_str = columns[7].replace(',', '')
                    deaths = float(death_str)
                    print(f"South-East Asia Region\t{deaths}")
                except ValueError:
                    continue

if __name__ == "__main__":
    mapper()