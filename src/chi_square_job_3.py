from mrjob.job import MRJob
import json


class ChiSquareJob3(MRJob):

    def mapper(self, _, line):
        """
        Read one line from Job 2 output and send everything to one reducer.
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
        Compute chi-square and keep top 75 terms per category.
        """
        category_totals = {}
        term_totals = {}
        term_in_category = {}
        global_total = 0

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

        results = {}

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

        for category in results:
            top_terms = sorted(results[category], key=lambda x: x[1], reverse=True)[:75]

            for term, score in top_terms:
                yield category, (term, score)


if __name__ == "__main__":
    ChiSquareJob3.run()