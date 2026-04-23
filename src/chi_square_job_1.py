"""
Job 1: Count occurrences of tokens in reviews per category, and total review counts.

Mapper input:
    - Raw line of JSON (one review per line)

Mapper output (intermediate):
    - key: ("CATEGORY_TOTAL", <category>)               value: 1
    - key: ("GLOBAL_TOTAL", "ALL")                      value: 1
    - key: ("TERM_IN_CATEGORY", <category>, <term>)     value: 1

Reducer input:
    - Same keys as mapper output, grouped by key

Reducer output:
    - Same key, value = total_sum
      (e.g., ("CATEGORY_TOTAL", "Books") -> 15420)
"""
from mrjob.job import MRJob
import json
import re


class ChiSquareJob1(MRJob):
    """
    First MapReduce job: Tokenize, filter, and count raw frequencies.
    """

    def configure_args(self):
        """Add custom command-line argument for the stopwords file."""
        super(ChiSquareJob1, self).configure_args()
        self.add_file_arg('--stopwords', help='Path to stopwords file')

    def mapper_init(self):
        """
        Load stopwords once per mapper.
        Each word is lowercased and stored in a set for O(1) lookup.
        """
        self.stopwords = set()

        with open(self.options.stopwords, 'r', encoding='utf-8') as f:
            for line in f:
                word = line.strip().lower()
                if word:
                    self.stopwords.add(word)

    def mapper(self, _, line):
        """
        Process a single JSON review.

        - Extracts 'category' and 'reviewText'.
        - Emits one count for the category total and one for the global total.
        - Tokenizes the lowercased text using the required delimiters.
        - Removes stopwords, single‑character tokens, and duplicates within the
          same review (binary document frequency).
        - For each surviving token emits a count of 1 for that (category, token) pair.

        Yielded keys:
            ("CATEGORY_TOTAL", category)          : 1
            ("GLOBAL_TOTAL", "ALL")               : 1
            ("TERM_IN_CATEGORY", category, token) : 1
        """
        try:
            data = json.loads(line)
            category = data.get("category", None)
            text = data.get("reviewText", "")

            if category and text:
                # Count this review in the global and category totals
                yield ("CATEGORY_TOTAL", category), 1
                yield ("GLOBAL_TOTAL", "ALL"), 1
                text = text.lower()
                # Split on whitespace, digits, and the special characters
                tokens = re.split(
                    r"[ \t\n\r0-9()\[\]{}.!?,;:+=\-_\"'`~#@&*%€$§\\/]+",
                    text
                )

                filtered_tokens = set()

                for token in tokens:
                    # Keep only tokens longer than 1 character and not in stopwords
                    if token and len(token) > 1 and token not in self.stopwords:
                        filtered_tokens.add(token)

                # Emit one count per unique token in the review
                for token in filtered_tokens:
                    yield ("TERM_IN_CATEGORY", category, token), 1

        except Exception:
            # Skip malformed JSON lines
            pass

    def reducer(self, key, counts):
        """
        Sums all 1's for a given key.

        Input key: one of the key types from the mapper.
        Input values: iterator of 1's.
        Output: key -> total_sum
        """
        yield key, sum(counts)


if __name__ == "__main__":
    ChiSquareJob1.run()