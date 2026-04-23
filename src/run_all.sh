#!/bin/bash
# ==========================================================================
# run_all.sh
#
# To be placed in src/ .
# Run from the parent directory of src/ like:
#   bash src/run_all.sh [INPUT] [STOPWORDS]
#
# Default INPUT:       ../data/reviews_devset.json   (if you keep data/ parallel to src/)
# Default STOPWORDS:   src/stopwords.txt (relative to the working directory)
#
# The script determines its own location (SCRIPT_DIR) so that all python calls
# point to the correct source files.
# ==========================================================================
set -e

# Find the directory where this script lives (src/)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Input file: first argument, or a default relative path from the working directory
INPUT=${1:-data/reviews_devset.json}
# Stopwords: second argument, or the file we keep inside src/
STOPWORDS=${2:-"${SCRIPT_DIR}/stopwords.txt"}

echo "Using input:        ${INPUT}"
echo "Using stopwords:    ${STOPWORDS}"
echo "Scripts directory:  ${SCRIPT_DIR}"

echo "Running Job 1..."
python "${SCRIPT_DIR}/chi_square_job_1.py" "$INPUT" --stopwords "$STOPWORDS" > job1_output.txt

echo "Running Job 2..."
python "${SCRIPT_DIR}/chi_square_job_2.py" job1_output.txt > job2_output.txt

echo "Preparing side data..."
python "${SCRIPT_DIR}/prepare_side_data.py" job2_output.txt side_data.json

echo "Running Job 3 (efficient, distributed)..."
python "${SCRIPT_DIR}/chi_square_job_3.py" job2_output.txt --side-data side_data.json > final_output.txt

echo "Formatting final output..."
python "${SCRIPT_DIR}/format_output.py"

echo "Done. Final result written to output.txt"