#!/bin/bash

# run_all.sh
#
# Local execution script for the Chi‑Square MapReduce pipeline.
#
# Usage:
#   ./run_all.sh [INPUT_PATH] [STOPWORDS_PATH]
#
# Default input:         data/reviews_devset.json
# Default stopwords:     src/stopwords.txt

set -e

INPUT=${1:-data/reviews_devset.json}
STOPWORDS=${2:-src/stopwords.txt}

echo "Running Job 1..."
python src/chi_square_job_1.py "$INPUT" --stopwords "$STOPWORDS" > job1_output.txt

echo "Running Job 2..."
python src/chi_square_job_2.py job1_output.txt > job2_output.txt

echo "Running Job 3..."
python src/chi_square_job_3.py job2_output.txt > final_output.txt

echo "Formatting final output..."
python src/format_output.py

echo "Done. Final result written to output.txt"