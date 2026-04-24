"""
Job 3 (efficient, distributed version): Compute chi‑square and select top 75 terms per category.

Mapper input:
    - Output of Job 2 (text lines).  Only lines with key "TERM_IN_CATEGORY" are used.
      Format: '["TERM_IN_CATEGORY", "<category>", "<term>"]\t<count>'

Mapper output:
    - key:   <category>               (so data is grouped by category)
    - value: (<term>, <count>)        (the A value for that term)

Reducer input:
    - key:   a specific category
    - values: iterator of (<term>, count) tuples

Reducer output:
    - key:   category
    - value: (<term>, chi_square_value)   only for the top 75 terms in that category.

Side data:
    - A JSON file (side_data.json) containing:
        "global_total": N
        "category_totals": {category: total_reviews}
        "term_totals": {term: total_reviews_containing_term}
    This file is loaded by each reducer in reducer_init() and used to compute the
    contingency table for every (category, term) pair.

This way we have:
- No reducer bottleneck - work is partitioned by the 22 categories. Each reducer only sees the terms for its assigned category and all statistics are read from locad side data JSON file.
- scalable and efficient - no single reducer has to handle all terms, and the side data is small enough to be loaded into memory by each reducer, eliminating the need for shuffling these statistics.
"""

from collections import defaultdict

from mrjob.job import MRJob
import json


class ChiSquareJob3(MRJob):
    """
    Distributed chi‑square computation using side data.
    """

    def configure_args(self):
        """Accept an optional --side-data argument for the prepared side data file."""
        super(ChiSquareJob3, self).configure_args()
        self.add_file_arg('--side-data', help='Path to side data JSON file')

    def mapper_init(self):
        """
        Nothing to initialise in the mapper.
        (The reducer initialisation loads the side data.)
        """
        pass

    def mapper(self, _, line):
        """
        Extract only TERM_IN_CATEGORY records and emit them keyed by category.

        Ignores CATEGORY_TOTAL, GLOBAL_TOTAL, and TERM_TOTAL lines because
        their data is already present in the side data file.
        """
        try:
            key_str, value_str = line.rsplit('\t', 1)
            key = json.loads(key_str)
            value = int(value_str)

            if key[0] == "TERM_IN_CATEGORY":
                _, category, term = key
                # Emit with category as key to group data per category
                yield category, (term, value)
        except Exception:
            pass

    def reducer_init(self):
        """
        Load the side data file into three dictionaries accessible by all
        reducers on their respective nodes.
        """
        with open(self.options.side_data, 'r', encoding='utf-8') as f:
            side = json.load(f)

        self.global_total = side["global_total"]
        self.category_totals = side["category_totals"]
        self.term_totals = side["term_totals"]

    def reducer(self, category, term_counts):
        """
        Compute chi‑square for all terms in the given category.

        For each term:
            A = count of documents in this category containing the term
            B = term_total - A
            C = category_total - A
            D = N - A - B - C

        chi_square = N * (A*D - B*C)^2 / ((A+B)*(C+D)*(A+C)*(B+D))

        Only the top 75 terms by chi‑square are kept and emitted.
        """
        N = self.global_total
        cat_total = self.category_totals.get(category, 0)

        terms_list = []   # will hold (term, chi_square)

        for term, A in term_counts:
            term_total = self.term_totals.get(term, 0)

            B = term_total - A
            C = cat_total - A
            D = N - A - B - C

            # denominator components
            denom = (A + B) * (C + D) * (A + C) * (B + D)
            if denom > 0:
                chi = ((A * D - B * C) ** 2) * N / denom
                terms_list.append((term, chi))

        # Keep only the top 75 (or fewer if there aren't that many)
        top_75 = sorted(terms_list, key=lambda x: x[1], reverse=True)[:75]

        for term, score in top_75:
            yield category, (term, score)

    def combiner(self, category, term_counts):
        """
        Locally aggregate term counts for the same category.

        Input:  category  – the category name
                term_counts – iterator of (term, count) tuples

        Output: category -> (term, aggregated_count)
        """
        agg = defaultdict(int)
        for term, count in term_counts:
            agg[term] += count
        for term, total in agg.items():
            yield category, (term, total)    


if __name__ == "__main__":
    ChiSquareJob3.run()