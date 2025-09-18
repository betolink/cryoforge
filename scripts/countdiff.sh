#!/bin/bash

# Usage: ./compare_ndjson_differences.sh /path/to/dirA /path/to/dirB

set -euo pipefail

DIR_A="$1"
DIR_B="$2"

if [[ ! -d "$DIR_A" || ! -d "$DIR_B" ]]; then
    echo "Both arguments must be valid directories."
    exit 1
fi

cd "$DIR_A"
find . -type f -name "*.ndjson" | sort > /tmp/list_ndjson.txt
cd - > /dev/null

echo -e "CountA\tCountB\tDiff\tPath"
echo "----------------------------------------------------------"

total_a=0
total_b=0

while read -r relpath; do
    file_a="$DIR_A/$relpath"
    file_b="$DIR_B/$relpath"

    if [[ -f "$file_a" && -f "$file_b" ]]; then
        count_a=$(wc -l < "$file_a")
        count_b=$(wc -l < "$file_b")

        if [[ "$count_a" -ne "$count_b" ]]; then
            diff=$((count_b - count_a))
            printf "%6d\t%6d\t%+5d\t%s\n" "$count_a" "$count_b" "$diff" "$relpath"
        fi

        total_a=$((total_a + count_a))
        total_b=$((total_b + count_b))
    else
        echo "MISSING FILE: $relpath"
    fi
done < /tmp/list_ndjson.txt

rm /tmp/list_ndjson.txt

total_diff=$((total_b - total_a))
echo "----------------------------------------------------------"
printf "TOTAL:\t%6d\t%6d\t%+5d\t(files may match even if counts differ)\n" "$total_a" "$total_b" "$total_diff"

