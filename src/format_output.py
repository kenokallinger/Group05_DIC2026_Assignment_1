"""
Post‑processing script (local, not MapReduce).

Reads the raw output of Job 3 (text file: "category" ("term", score))
and produces the final output.txt with the required format:

- One line per category (categories in alphabetical order), containing:
    <category name> term_1:score_1 term_2:score_2 ... term_75:score_75
- One final line with all these terms merged, sorted alphabetically,
  separated by spaces.
"""
import json
from collections import defaultdict


def main():
    """
    Processes 'final_output.txt' and writes 'output.txt'.
    """
    category_terms = defaultdict(list)
    merged_terms = set()

    # Read the raw output from Job 3
    with open("final_output.txt", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Split into key and value
            key_str, value_str = line.rsplit("\t", 1)

            # key_str is a JSON string of the category name
            category = json.loads(key_str)
            # value_str is a JSON list: [term, score]
            term, score = json.loads(value_str)

            category_terms[category].append((term, float(score)))
            merged_terms.add(term)

    with open("output.txt", "w", encoding="utf-8") as out:
        # Write category lines in alphabetical order
        for category in sorted(category_terms.keys()):
            # Sort terms descending by chi‑square, although they should already be in that order from Job 3
            terms_sorted = sorted(category_terms[category], key=lambda x: x[1], reverse=True)

            parts = [category]
            for term, score in terms_sorted:
                # Score formatted to 6 decimal places
                parts.append(f"{term}:{score:.6f}")

            out.write(" ".join(parts) + "\n")

        # Write merged line with all unique terms sorted alphabetically
        merged_line = " ".join(sorted(merged_terms))
        out.write(merged_line + "\n")


if __name__ == "__main__":
    main()