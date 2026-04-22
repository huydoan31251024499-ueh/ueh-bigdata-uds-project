#!/usr/bin/env python3
import sys

def reducer():
    total_deaths = 0.0
    current_region = None

    for line in sys.stdin:
        line = line.strip()
        region, deaths = line.split('\t', 1)

        try:
            deaths = float(deaths)
        except ValueError:
            continue

        if current_region == region:
            total_deaths += deaths
        else:
            if current_region:
                print(f"{current_region}\t{total_deaths}")
            current_region = region
            total_deaths = deaths

    if current_region:
        print(f"{current_region}\t{total_deaths}")

if __name__ == "__main__":
    reducer()