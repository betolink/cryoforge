#!/bin/bash

# Get parameters
S3_URL=$1        # The full S3 URL (e.g., s3://your-bucket-name/path/to/files/)
ENDPOINT=$2      # The endpoint URL (e.g., http://stac.itslive.cloud/collections/itslive-granules/bulk_items)
CHUNK_SIZE=${3:-10000}  # Default to 10000 if not provided
FILE_EXTENSION=${4:-.ndjson}  # Default to .ndjson if not provided
NUM_PARALLEL=${5:-4}  # Number of parallel uploads (default: 4)

# Validate required parameters
if [ -z "$S3_URL" ] || [ -z "$ENDPOINT" ]; then
    echo "Usage: $0 <S3_URL> <ENDPOINT> [CHUNK_SIZE] [FILE_EXTENSION] [NUM_PARALLEL]"
    exit 1
fi

# Extract the bucket name and path from the S3 URL
BUCKET_NAME=$(echo $S3_URL | awk -F'/' '{print $3}')
S3_PATH=$(echo $S3_URL | sed "s|s3://$BUCKET_NAME/||")

echo "Starting processing for S3 path: $S3_URL"
echo "Uploading to endpoint: $ENDPOINT"
echo "Chunk size: $CHUNK_SIZE"
echo "Processing in parallel with $NUM_PARALLEL workers"

# List all files recursively in the S3 bucket and filter by file extension
aws s3 ls "s3://${BUCKET_NAME}/${S3_PATH}" --recursive | \
    awk -v ext="$FILE_EXTENSION" '$4 ~ ext {print $4}' | while read s3_file; do
        echo "Processing file: $s3_file"
        
        # Download the file to the local disk
        local_file="local_${s3_file//\//_}"
        aws s3 cp "s3://${BUCKET_NAME}/${s3_file}" "$local_file"
        
        echo "Downloaded: $local_file"

        # Process the downloaded file, compact each line and split into chunks
        jq -c . "$local_file" | split -l $CHUNK_SIZE - "chunk_"

        echo "File split into chunks. Starting uploads..."

        # Use xargs to send chunks in parallel
        ls chunk_* | xargs -P $NUM_PARALLEL -I{} sh -c '
            start_time=$(date +%s)
            response=$(cat "{}" | jq -s '"'"'{items: map({(.id): .}) | add, method: "upsert"}'"'"' | \
            curl -s -X POST "'"$ENDPOINT"'" \
                 -H "Content-Type: application/json" \
                 --compressed \
                 --data-binary @-)
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "Uploaded {} in ${duration}s - Response: $response"
            rm "{}"
        '

        echo "Finished processing file: $s3_file"

        # Remove the local copy of the original file after processing
        rm "$local_file"
done

echo "All processing completed."

