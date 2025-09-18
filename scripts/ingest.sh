#!/bin/bash

# Get parameters
S3_URL=$1        # The full S3 URL (e.g., s3://your-bucket-name/path/to/files/)
ENDPOINT=$2      # The endpoint URL (e.g., http://stac.itslive.cloud/collections/itslive-granules/bulk_items)
CHUNK_SIZE=${3:-10000}  # Default to 10000 if not provided
NUM_PARALLEL_FILES=${4:-4}  # Number of parallel file processes (default: 4)

# Validate required parameters
if [ -z "$S3_URL" ] || [ -z "$ENDPOINT" ]; then
    echo "Usage: $0 <S3_URL> <ENDPOINT> [CHUNK_SIZE] [FILE_EXTENSION] [NUM_PARALLEL_FILES]"
    exit 1
fi

# Extract the bucket name and path from the S3 URL
BUCKET_NAME=$(echo "$S3_URL" | awk -F'/' '{print $3}')
S3_PATH=$(echo "$S3_URL" | sed "s|s3://$BUCKET_NAME/||")

# Use absolute paths for log files to avoid issues with subshells
LOG_DIR="$PWD/ingest_logs"
mkdir -p "$LOG_DIR"

# Create a unique identifier for this processing job
JOB_ID=$(echo "$S3_URL$ENDPOINT" | md5sum | cut -d' ' -f1)

# Progress tracking file
PROGRESS_LOG="$LOG_DIR/ingest_progress_${JOB_ID}.log"
SUMMARY_LOG="$LOG_DIR/ingest_summary_${JOB_ID}.log"

echo "Starting processing for S3 path: $S3_URL"
echo "Uploading to endpoint: $ENDPOINT"
echo "Chunk size: $CHUNK_SIZE"
echo "Processing in parallel with $NUM_PARALLEL_FILES workers"
echo "Progress log: $PROGRESS_LOG"
echo "Summary log: $SUMMARY_LOG"

# Create or append header to summary log if it doesn't exist
if [ ! -f "$SUMMARY_LOG" ]; then
    echo "Start Time: $(date)" > "$SUMMARY_LOG"
    echo "S3 URL: $S3_URL" >> "$SUMMARY_LOG"
    echo "Endpoint: $ENDPOINT" >> "$SUMMARY_LOG"
    echo "Chunk Size: $CHUNK_SIZE" >> "$SUMMARY_LOG"
    echo "File Extension: $FILE_EXTENSION" >> "$SUMMARY_LOG"
    echo "----------------------------" >> "$SUMMARY_LOG"
    echo "File Path | Items | Start Time | End Time | Duration (s)" >> "$SUMMARY_LOG"
else
    # Append a restart marker to existing summary
    echo "----------------------------" >> "$SUMMARY_LOG"
    echo "Restart Time: $(date)" >> "$SUMMARY_LOG"
    echo "----------------------------" >> "$SUMMARY_LOG"
fi

# Create a temp directory for processing files
TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR" || exit 1
echo "Working in temporary directory: $TEMP_DIR"


FILE_LIST="${LOG_DIR}/s3_files_list_${JOB_ID}.txt"

# Modified file listing section
echo "Listing files from S3..."
aws s3 ls "s3://${BUCKET_NAME}/${S3_PATH}" --no-sign-request --recursive --output text | awk '{print $4}' > "$FILE_LIST"

# Load files into array from the text file
mapfile -t files < "$FILE_LIST"
echo "Found ${#files[@]} files to process"
echo "Full file list saved to: $FILE_LIST"

# Ensure progress log exists
touch "$PROGRESS_LOG"

# Check which files have already been processed
echo "Checking for files already processed..."
processed_count=0
for s3_file in "${files[@]}"; do
    if grep -q "^COMPLETED|$s3_file|" "$PROGRESS_LOG"; then
        echo "Skipping already processed file: $s3_file"
        ((processed_count++))
    fi
done
echo "$processed_count files already processed and will be skipped"

# Shared summary file lock
LOCK_FILE="$LOG_DIR/summary_${JOB_ID}.lock"

job_count=0
for s3_file in "${files[@]}"; do
    # Skip if this file has already been processed
    if grep -q "^COMPLETED|$s3_file|" "$PROGRESS_LOG"; then
        continue
    fi

    # Skip if already in progress and not completed (might be from a crashed run)
    if grep -q "^INPROGRESS|$s3_file|" "$PROGRESS_LOG" && ! grep -q "^COMPLETED|$s3_file|" "$PROGRESS_LOG"; then
        echo "Cleaning up and reprocessing previously started but not completed file: $s3_file"
        # Remove the INPROGRESS entry to start fresh
        sed -i "/^INPROGRESS|$s3_file|/d" "$PROGRESS_LOG"
    fi

    # Mark as in progress
    echo "INPROGRESS|$s3_file|$(date '+%Y-%m-%d %H:%M:%S')" >> "$PROGRESS_LOG"
    
    (
        # Create a unique identifier for this file using a hash
        file_id=$(echo "$s3_file" | md5sum | cut -d' ' -f1)
        local_file="local_${file_id}"
        file_start_time=$(date '+%Y-%m-%d %H:%M:%S')
        epoch_start=$(date +%s)
        
        echo "Processing file: $s3_file"

        # Download the file to the local disk
        if ! aws s3 cp "s3://${BUCKET_NAME}/${s3_file}" "$local_file" --no-sign-request; then
            echo "Failed to download $s3_file" >&2
            echo "FAILED|$s3_file|$(date '+%Y-%m-%d %H:%M:%S')|download_failed" >> "$PROGRESS_LOG"
            exit 1
        fi
        echo "Downloaded: $s3_file to $local_file"

        # Count the number of items in the file
        item_count=$(wc -l < "$local_file")
        echo "File contains $item_count items"

        # Split into chunks with unique naming based on hash
        jq -c . "$local_file" | split -l "$CHUNK_SIZE" - "chunk_${file_id}_"
        
        # Count actual chunks created
        chunk_total=$(ls chunk_${file_id}_* | wc -l)
        echo "File split into $chunk_total chunks. Starting uploads..."

        # Sequential chunk upload per file
        chunks_processed=0
        chunks_failed=0
        for chunk_file in chunk_${file_id}_*; do
            start_time=$(date +%s)
            ((chunks_processed++))
            
            echo "Processing chunk $chunks_processed/$chunk_total for file: $s3_file"
            
            response=$(cat "$chunk_file" | jq -s '{items: map({(.id): .}) | add, method: "upsert"}' | \
                      curl -s -X POST "$ENDPOINT" \
                           -H "Content-Type: application/json" \
                           --compressed \
                           --data-binary @-)
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            
            # Check if response contains an error
            if echo "$response" | grep -q "error"; then
                echo "Error in chunk $chunks_processed: $response"
                echo "CHUNK_FAILED|$s3_file|$(date '+%Y-%m-%d %H:%M:%S')|chunk_${chunks_processed}_failed: $response" >> "$PROGRESS_LOG"
                ((chunks_failed++))
                # Continue with next chunk - we don't exit as some chunks might succeed
            else
                echo "File: $s3_file | Chunk: ${chunks_processed}/${chunk_total} | Duration: ${duration}s | Response: $response"
            fi
            
            rm -f "$chunk_file"
        done

        epoch_end=$(date +%s)
        total_duration=$((epoch_end - epoch_start))
        file_end_time=$(date '+%Y-%m-%d %H:%M:%S')
        
        echo "Finished processing file: $s3_file"
        
        # Report any chunk failures in the completion status
        if [ "$chunks_failed" -gt 0 ]; then
            echo "COMPLETED_WITH_ERRORS|$s3_file|$file_start_time|$file_end_time|$item_count|$total_duration|$chunks_failed failed chunks" >> "$PROGRESS_LOG"
        else
            echo "COMPLETED|$s3_file|$file_start_time|$file_end_time|$item_count|$total_duration" >> "$PROGRESS_LOG"
        fi
        
        # Use flock to safely write to summary log
        (
            flock -x 200
            echo "$s3_file | $item_count | $file_start_time | $file_end_time | $total_duration" >> "$SUMMARY_LOG"
        ) 200>"$LOCK_FILE"
        
        rm -f "$local_file"
    ) &

    ((job_count++))
    if [ "$job_count" -ge "$NUM_PARALLEL_FILES" ]; then
        wait -n
        ((job_count--))
    fi
done

# Wait for remaining jobs
wait
cd - > /dev/null || exit 1
rm -rf "$TEMP_DIR"

# Add completion timestamp to summary
echo "----------------------------" >> "$SUMMARY_LOG"
echo "Completion Time: $(date)" >> "$SUMMARY_LOG"

# Generate final statistics
completed=$(grep -c "^COMPLETED|" "$PROGRESS_LOG")
completed_with_errors=$(grep -c "^COMPLETED_WITH_ERRORS|" "$PROGRESS_LOG")
failed=$(grep -c "^FAILED|" "$PROGRESS_LOG")
total_items=$(grep -E "^COMPLETED|^COMPLETED_WITH_ERRORS" "$PROGRESS_LOG" | awk -F'|' '{sum+=$5} END {print sum}')

echo "Statistics:" >> "$SUMMARY_LOG"
echo "Files completed successfully: $completed" >> "$SUMMARY_LOG"
echo "Files completed with errors: $completed_with_errors" >> "$SUMMARY_LOG"
echo "Files failed: $failed" >> "$SUMMARY_LOG"
echo "Total items processed: $total_items" >> "$SUMMARY_LOG"

echo "Processing summary:"
echo "Files completed successfully: $completed"
echo "Files completed with errors: $completed_with_errors"
echo "Files failed: $failed"
echo "Total items processed: $total_items"
echo "See $SUMMARY_LOG for detailed report"
echo "Progress log preserved at: $PROGRESS_LOG"

echo "All processing completed."
