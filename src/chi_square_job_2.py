"""
Job 2: Compute total term frequency across all categories.

Mapper input:
    - Output of Job 1: "<key><value>", where key is a JSON list and value an integer.
      Example: '["TERM_IN_CATEGORY", "Books", "reading"] 42'

Mapper output (intermediate):
    - For "CATEGORY_TOTAL" and "GLOBAL_TOTAL": pass through unchanged.
    - For "TERM_IN_CATEGORY": emit two records:
        1) ("TERM_IN_CATEGORY", category, term)   value
        2) ("TERM_TOTAL", term)                   value   (ti sum across all categories)

Reducer input:
    - Grouped records by the same key.

Reducer output:
    - Summed value for each key.
"""
from mrjob.job import MRJob
import json


class ChiSquareJob2(MRJob):
    """
    Second MapReduce job: Aggregate term counts across categories.
    """

    def mapper(self, _, line):
        """
        Parse one line of Job 1 output (text format: 'key_str value').

        For "TERM_IN_CATEGORY" keys, we forward the original count AND emit an
        additional record under "TERM_TOTAL" to collect the overall frequency of
        that term.

        Yielded keys:
            ("CATEGORY_TOTAL", category)          : value
            ("GLOBAL_TOTAL", "ALL")               : value
            ("TERM_IN_CATEGORY", category, term)  : value
            ("TERM_TOTAL", term)                  : value
        """
        try:
            key_str, value_str = line.rsplit('\t', 1)
            key = json.loads(key_str) # key is a list like ["TERM_IN_CATEGORY", "Books", "reading"]
            value = int(value_str)

            if key[0] == "CATEGORY_TOTAL":
                yield tuple(key), value

            elif key[0] == "GLOBAL_TOTAL":
                yield tuple(key), value

            elif key[0] == "TERM_IN_CATEGORY":
                _, category, term = key

                # keep original term-category count
                yield ("TERM_IN_CATEGORY", category, term), value

                # also compute total count of term across all categories
                yield ("TERM_TOTAL", term), value

        except Exception:
            pass

    def reducer(self, key, values):
        """
        Sum values for identical keys.

        Input key: one of the above types.
        Input values: iterator of counts.
        Output: same key -> total_sum
        """
        yield key, sum(values)


if __name__ == "__main__":
    ChiSquareJob2.run()