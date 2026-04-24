#!/bin/bash
#
# run_all.sh – local & cluster execution for the Chi‑Square pipeline
#
# Local mode (default):
#   bash src/run_all.sh [input_file] [stopwords_file]
#
# Cluster mode (Hadoop):
#   bash src/run_all.sh --hadoop [input_hdfs_path] [stopwords_local_path]
#
# Examples:
#   # Local development
#   bash src/run_all.sh
#
#   # Local with custom input
#   bash src/run_all.sh my_reviews.json
#
#   # Full dataset on cluster
#   bash src/run_all.sh --hadoop hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json
#
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# --- Defaults ----------------------------------------------------------------
RUN_MODE="local"                           # local / hadoop
INPUT_DEFAULT="data/reviews_devset.json"   # relative to working directory
STOPWORDS_DEFAULT="${SCRIPT_DIR}/stopwords.txt"
HADOOP_STREAMING_JAR="/usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"
HDFS_BASE=""                               # will be set in hadoop mode

# --- Parse optional --hadoop flag --------------------------------------------
if [ "$1" == "--hadoop" ]; then
    RUN_MODE="hadoop"
    shift
fi

# --- Arguments after possible flag --------------------------------------------
INPUT="${1:-$INPUT_DEFAULT}"
STOPWORDS="${2:-$STOPWORDS_DEFAULT}"

echo "Mode:            ${RUN_MODE}"
echo "Input:           ${INPUT}"
echo "Stopwords:       ${STOPWORDS}"
echo "Scripts:         ${SCRIPT_DIR}"

# --- Helper functions ---------------------------------------------------------
run_job() {
    # $1: script name (relative to SCRIPT_DIR)
    # all remaining args are passed to the python script
    local script="${SCRIPT_DIR}/$1"
    shift
    python "$script" "$@"
}

hdfs_cleanup() {
    [ -n "$HDFS_BASE" ] && hadoop fs -rm -r -f "$HDFS_BASE" 2>/dev/null || true
}

# --- Hadoop specific setup ---------------------------------------------------
if [ "$RUN_MODE" == "hadoop" ]; then
    USER=$(whoami)
    HDFS_BASE="hdfs:///user/${USER}/chi_square_tmp_$$"
    echo "Intermediate:    ${HDFS_BASE}"
    hdfs_cleanup   # clean previous run with same base name if any
fi

# --- Pipeline steps ----------------------------------------------------------

echo "=== Job 1: Tokenisation and counting ==="
if [ "$RUN_MODE" == "local" ]; then
    run_job chi_square_job_1.py "$INPUT" --stopwords "$STOPWORDS" > job1_output.txt
else
    run_job chi_square_job_1.py \
        -r hadoop \
        --hadoop-streaming-jar "$HADOOP_STREAMING_JAR" \
        --file "$STOPWORDS" \
        --stopwords stopwords.txt \
        --output-dir "${HDFS_BASE}/job1" \
        "$INPUT"
    hadoop fs -getmerge "${HDFS_BASE}/job1" job1_output.txt
fi

echo "=== Job 2: Aggregating term totals ==="
if [ "$RUN_MODE" == "local" ]; then
    run_job chi_square_job_2.py job1_output.txt > job2_output.txt
else
    run_job chi_square_job_2.py \
        -r hadoop \
        --hadoop-streaming-jar "$HADOOP_STREAMING_JAR" \
        --output-dir "${HDFS_BASE}/job2" \
        job1_output.txt
    hadoop fs -getmerge "${HDFS_BASE}/job2" job2_output.txt
fi

echo "=== Prepare side data ==="
python "${SCRIPT_DIR}/prepare_side_data.py" job2_output.txt side_data.json

echo "=== Job 3: Chi‑square and top‑75 selection ==="
if [ "$RUN_MODE" == "local" ]; then
    run_job chi_square_job_3.py job2_output.txt --side-data side_data.json > final_output.txt
else
    run_job chi_square_job_3.py \
        -r hadoop \
        --hadoop-streaming-jar "$HADOOP_STREAMING_JAR" \
        --file side_data.json \
        --side-data side_data.json \
        --output-dir "${HDFS_BASE}/job3" \
        job2_output.txt
    hadoop fs -getmerge "${HDFS_BASE}/job3" final_output.txt
fi

echo "=== Formatting final output ==="
python "${SCRIPT_DIR}/format_output.py"

# --- Cleanup -----------------------------------------------------------------
if [ "$RUN_MODE" == "hadoop" ]; then
    hdfs_cleanup
    echo "Cleaned up HDFS temporary files."
fi

echo "Done. Result: output.txt"