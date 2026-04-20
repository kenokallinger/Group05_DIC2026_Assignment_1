from mrjob.job import MRJob
import json


class ChiSquareJob2(MRJob):

    def mapper(self, _, line):
        """
        Input: one line from Job 1 output
        Output:
        - pass CATEGORY_TOTAL and GLOBAL_TOTAL through
        - for TERM_IN_CATEGORY, emit both the original key and TERM_TOTAL
        """
        try:
            key_str, value_str = line.rsplit('\t', 1)
            key = json.loads(key_str)
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
        Sum values for identical keys
        """
        yield key, sum(values)


if __name__ == "__main__":
    ChiSquareJob2.run()