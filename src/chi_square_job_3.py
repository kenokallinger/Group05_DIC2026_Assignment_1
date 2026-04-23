"""
Job 3: Compute chi‑square values and select top 75 terms per category.

Mapper input:
    - Output of Job 2, same text format.

Mapper output:
    - All records are sent to a single reducer under the dummy key "ALL".
      Each record is preserved as a tuple (key, value).

Reducer input:
    - key: "ALL"
    - values: iterator of ((key_type, ...), count) tuples.

Reducer output:
    - category -> (term, chi_square_value)   for the top 75 terms per category.
"""
from mrjob.job import MRJob
import json


class ChiSquareJob3(MRJob):
    """
    Third MapReduce job: Compute chi‑square and keep only the most discriminating terms.
    """
    def mapper(self, _, line):
        """
        Reads one line of Job 2 output and forwards it to the reducer.

        Yielded key:
            "ALL"   ->   (original_key_tuple, count)
        """
        try:
            key_str, value_str = line.rsplit('\t', 1)
            key = json.loads(key_str)
            value = int(value_str)

            yield "ALL", (key, value)

        except Exception:
            pass

    def reducer(self, _, records):
        """
        Collect all counts, compute chi‑square, and output top 75 terms per category.

        Stores:
            category_totals: dict  {category: total_reviews_in_category}
            term_totals:     dict  {term: total_reviews_containing_term}
            term_in_category: dict {(category, term): count_A}
            global_total:    int   N (total number of reviews)

        For each (category, term) where A > 0, we build a 2x2 contingency table:
            A = reviews in category containing the term
            B = term_total - A    (reviews not in category that contain the term)
            C = category_total - A (reviews in category without the term)
            D = N - A - B - C     (reviews neither in category nor containing the term)

        chi_square = N * (A*D - B*C)^2 / ((A+B)*(C+D)*(A+C)*(B+D))

        Only the top 75 terms per category (by descending chi‑square) are kept.
        """
        category_totals = {}
        term_totals = {}
        term_in_category = {}
        global_total = 0

        # First pass: collect all aggregated counts from the input records
        for key, value in records:
            if key[0] == "CATEGORY_TOTAL":
                _, category = key
                category_totals[category] = value

            elif key[0] == "GLOBAL_TOTAL":
                global_total = value

            elif key[0] == "TERM_TOTAL":
                _, term = key
                term_totals[term] = value

            elif key[0] == "TERM_IN_CATEGORY":
                _, category, term = key
                term_in_category[(category, term)] = value

        # Compute chi‑square for each (category, term) pair
        results = {} # category -> list of (term, score)

        for (category, term), A in term_in_category.items():
            category_total = category_totals.get(category, 0)
            term_total = term_totals.get(term, 0)
            N = global_total

            B = term_total - A
            C = category_total - A
            D = N - A - B - C

            denominator = (A + B) * (C + D) * (A + C) * (B + D)

            if denominator > 0:
                chi_square = ((A * D - B * C) ** 2) * N / denominator

                if category not in results:
                    results[category] = []

                results[category].append((term, chi_square))
                
        # For each category, sort by score descending and keep top 75
        for category in results:
            top_terms = sorted(results[category], key=lambda x: x[1], reverse=True)[:75]

            for term, score in top_terms:
                yield category, (term, score)


if __name__ == "__main__":
    ChiSquareJob3.run()