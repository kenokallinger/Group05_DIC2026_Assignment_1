import json
from collections import defaultdict


def main():
    category_terms = defaultdict(list)
    merged_terms = set()

    with open("final_output.txt", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            key_str, value_str = line.rsplit("\t", 1)

            category = json.loads(key_str)
            term, score = json.loads(value_str)

            category_terms[category].append((term, float(score)))
            merged_terms.add(term)

    with open("output.txt", "w", encoding="utf-8") as out:
        for category in sorted(category_terms.keys()):
            terms_sorted = sorted(category_terms[category], key=lambda x: x[1], reverse=True)

            parts = [category]
            for term, score in terms_sorted:
                parts.append(f"{term}:{score:.6f}")

            out.write(" ".join(parts) + "\n")

        merged_line = " ".join(sorted(merged_terms))
        out.write(merged_line + "\n")


if __name__ == "__main__":
    main()