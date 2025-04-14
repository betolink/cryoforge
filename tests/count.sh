#!/bin/bash

# Default parameters
WORKERS=8
S3_PATH="s3://its-live-data/velocity_image_pair/sentinel2/v02/"
EXTENSION=".nc"
FORMAT="text"
CACHE_DIR="$HOME/.s3_counter_cache"
TODAY=$(date +%Y-%m-%d)

# Parse S3 path into bucket and prefix - this is more reliable
S3_PATH="${S3_PATH%/}/"  # Ensure trailing slash
BUCKET_NAME=$(echo "$S3_PATH" | cut -d'/' -f3)
PREFIX=$(echo "$S3_PATH" | cut -d'/' -f4- | sed 's/\/$//')

echo "Using bucket: $BUCKET_NAME"
echo "Using prefix: $PREFIX"

# Create cache directory
mkdir -p "$CACHE_DIR"
CACHE_FILE="${CACHE_DIR}/${BUCKET_NAME}_${PREFIX//\//_}_${EXTENSION//./_}.cache"

# Load cached results from today
declare -A CACHE
if [[ -f "$CACHE_FILE" ]]; then
    while IFS=: read -r dir count date; do
        [[ "$date" == "$TODAY" ]] && CACHE["$dir"]=$count
    done < "$CACHE_FILE"
fi

# Get all directories
echo "Listing directories in s3://$BUCKET_NAME/$PREFIX..."
ALL_DIRS=($(aws s3api list-objects-v2 \
    --bucket "$BUCKET_NAME" \
    --prefix "$PREFIX/" \
    --delimiter "/" \
    --no-sign-request \
    --query "CommonPrefixes[].Prefix" \
    --output text))

TOTAL=${#ALL_DIRS[@]}
[[ $TOTAL -eq 0 ]] && { echo "No directories found"; exit 1; }

# Identify directories needing processing
TO_PROCESS=()
for dir in "${ALL_DIRS[@]}"; do
    [[ -z "${CACHE[$dir]}" ]] && TO_PROCESS+=("$dir")
done

echo "Found $TOTAL directories, ${#TO_PROCESS[@]} require counting..."

# Setup processing files
RESULTS_FILE=$(mktemp)
PROGRESS_FILE=$(mktemp)
echo "0" > "$PROGRESS_FILE"

# Worker function for public buckets
process_directory() {
    local dir="$1"
    local count=$(aws s3api list-objects-v2 \
        --bucket "$BUCKET_NAME" \
        --prefix "$dir" \
        --no-sign-request \
        --query "length(Contents[?ends_with(Key, '${EXTENSION}')])" \
        --output text 2>/dev/null || echo "0")
    
    echo "$dir:$count:$TODAY" >> "$RESULTS_FILE"
    flock -x "$PROGRESS_FILE" -c "echo \$(( \$(cat "$PROGRESS_FILE") + 1 )) > "$PROGRESS_FILE""
}

export -f process_directory
export BUCKET_NAME EXTENSION RESULTS_FILE PROGRESS_FILE TODAY

# Process directories in parallel
if [[ ${#TO_PROCESS[@]} -gt 0 ]]; then
    printf "%s\n" "${TO_PROCESS[@]}" | \
        xargs -I {} -P "$WORKERS" bash -c 'process_directory "{}"' &

    # Progress monitoring
    while sleep 1; do
        CURRENT=$(cat "$PROGRESS_FILE")
        [[ "$CURRENT" -ge "${#TO_PROCESS[@]}" ]] && break
        PERCENT=$(( CURRENT * 100 / ${#TO_PROCESS[@]} ))
        printf "Processing: %3d%% (%d/%d)\r" "$PERCENT" "$CURRENT" "${#TO_PROCESS[@]}"
    done
    echo -e "\nCounting complete!"
fi

# Merge results
declare -A FINAL_RESULTS
for dir in "${!CACHE[@]}"; do
    FINAL_RESULTS["$dir"]=${CACHE["$dir"]}
done
while IFS=: read -r dir count date; do
    FINAL_RESULTS["$dir"]=$count
done < "$RESULTS_FILE"

# Update cache
: > "$CACHE_FILE"
for dir in "${!FINAL_RESULTS[@]}"; do
    echo "$dir:${FINAL_RESULTS[$dir]}:$TODAY" >> "$CACHE_FILE"
done

# Generate output
case "$FORMAT" in
    "csv")  echo "directory,count"
            for dir in "${!FINAL_RESULTS[@]}"; do
                echo "${dir//\//},${FINAL_RESULTS[$dir]}"
            done | sort -t, -k2n ;;
    "json") echo "{"
            first=true
            for dir in "${!FINAL_RESULTS[@]}"; do
                $first || echo ","
                printf '  "%s": %s' "${dir//\//}" "${FINAL_RESULTS[$dir]}"
                first=false
            done
            echo -e "\n}" ;;
    *)      echo "Results:"
            for dir in "${!FINAL_RESULTS[@]}"; do
                printf "%-40s %s\n" "${dir//\//}" "${FINAL_RESULTS[$dir]}"
            done | sort -k2n ;;
esac

# Cleanup
rm -f "$RESULTS_FILE" "$PROGRESS_FILE"
