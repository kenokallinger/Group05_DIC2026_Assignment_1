"""
prepare_side_data.py

Reads the output of Job 2 and writes a single JSON file
containing three pieces of information needed by Job 3:

    - global_total:        total number of reviews (N)
    - category_totals:     dictionary {category: total_reviews_in_category}
    - term_totals:         dictionary {term: total_reviews_containing_term}

This file is small enough (a few MB) to be loaded into memory by every reducer
in Job 3, eliminating the need to shuffle these statistics.
"""

import json
import sys


def main():
    if len(sys.argv) != 3:
        print("Usage: python prepare_side_data.py <job2_output> <side_data_output>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    global_total = 0
    category_totals = {}
    term_totals = {}

    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            key_str, value_str = line.rsplit('\t', 1)
            key = json.loads(key_str)
            value = int(value_str)

            if key[0] == "GLOBAL_TOTAL":
                global_total = value
            elif key[0] == "CATEGORY_TOTAL":
                _, category = key
                category_totals[category] = value
            elif key[0] == "TERM_TOTAL":
                _, term = key
                term_totals[term] = value
            # TERM_IN_CATEGORY records are ignored here

    side_data = {
        "global_total": global_total,
        "category_totals": category_totals,
        "term_totals": term_totals,
    }

    with open(output_path, 'w', encoding='utf-8') as out:
        json.dump(side_data, out)

    print(f"Side data written to {output_path}")
    print(f"  Categories: {len(category_totals)}")
    print(f"  Unique terms: {len(term_totals)}")


if __name__ == "__main__":
    main()