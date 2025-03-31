#!/bin/bash

# Check if the user provided the prefix argument
if [ $# -eq 0 ]; then
    echo "Usage: $0 <prefix>"
    exit 1
fi

# Set the bucket and path prefix from the argument
BUCKET="its-live-data"
PREFIX="$1"
OUTPUT_FILE="$2"

# Initialize continuation token
NEXT_TOKEN=""

# Clear the output file if it exists
> "$OUTPUT_FILE"

# Loop to fetch paginated results
while true; do
    # Fetch objects with the current continuation token
    if [ -z "$NEXT_TOKEN" ]; then
        RESPONSE=$(aws s3api list-objects-v2 --bucket "$BUCKET" --prefix "$PREFIX" --output json --no-sign-request)
    else
        RESPONSE=$(aws s3api list-objects-v2 --bucket "$BUCKET" --prefix "$PREFIX" --starting-token "$NEXT_TOKEN" --output json --no-sign-request)
    fi

    # Process the response using jq to filter only .nc files and extract their keys
    echo "$RESPONSE" | jq -r '.Contents[] | select(.Key | endswith(".nc")) | .Key' >> "$OUTPUT_FILE"

    # Extract the continuation token for the next iteration
    NEXT_TOKEN=$(echo "$RESPONSE" | jq -r '.NextContinuationToken')

    # If no NextContinuationToken is returned, we have finished fetching all pages
    if [ "$NEXT_TOKEN" == "null" ]; then
        break
    fi
done

echo "Results have been written to $OUTPUT_FILE"

