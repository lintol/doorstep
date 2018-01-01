#!/bin/sh

OUTPUT_FILE="${LINTOL_OUTPUT_FILE}"
PROCESSOR="$(find $LINTOL_PROCESSOR_DIRECTORY -type f -iname '*.py' | head -n 1)"
METADATA="${LINTOL_METADATA}"
INPUT_DATA="$(find $LINTOL_INPUT_DATA -type f | head -n 1)"

echo "Processing ${INPUT_DATA} with ${PROCESSOR} given ${METADATA} to ${OUTPUT_FILE}"

exec ltldoorstep \
    --output json \
    --output-file=${OUTPUT_FILE} \
    process \
    ${INPUT_DATA} ${PROCESSOR} \
    --metadata=${METADATA} \
    --engine=dask.threaded
