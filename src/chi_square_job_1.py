from mrjob.job import MRJob
import json
import re


class ChiSquareJob1(MRJob):

    def configure_args(self):
        super(ChiSquareJob1, self).configure_args()
        self.add_file_arg('--stopwords', help='Path to stopwords file')

    def mapper_init(self):
        self.stopwords = set()

        with open(self.options.stopwords, 'r', encoding='utf-8') as f:
            for line in f:
                word = line.strip().lower()
                if word:
                    self.stopwords.add(word)

    def mapper(self, _, line):
        """
        Input: one JSON review per line
        Output: ((category, token), 1)
        """
        try:
            data = json.loads(line)
            category = data.get("category", None)
            text = data.get("reviewText", "")

            if category and text:
                yield ("CATEGORY_TOTAL", category), 1
                yield ("GLOBAL_TOTAL", "ALL"), 1
                text = text.lower()

                tokens = re.split(
                    r"[ \t\n\r0-9()\[\]{}.!?,;:+=\-_\"'`~#@&*%€$§\\/]+",
                    text
                )

                filtered_tokens = set()

                for token in tokens:
                    if token and len(token) > 1 and token not in self.stopwords:
                        filtered_tokens.add(token)

                for token in filtered_tokens:
                    yield ("TERM_IN_CATEGORY", category, token), 1

        except Exception:
            pass

    def reducer(self, key, counts):
        """
        Input: ((category, token), [1, 1, 1, ...])
        Output: ((category, token), total_count)
        """
        yield key, sum(counts)


if __name__ == "__main__":
    ChiSquareJob1.run()