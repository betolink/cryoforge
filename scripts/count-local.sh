#!/bin/bash

# Usage: ./count_ndjson.sh /path/to/dir

set -euo pipefail

DIR="${1:-.}"

find "$DIR" -type f -name "*.ndjson" | sort | while read -r file; do
    count=$(wc -l < "$file")
    echo "$count $file"
done | awk '
{
    total += $1;
    printf "%7d %s\n", $1, $2;
}
END {
    print "----------------------";
    printf "Total: %d lines\n", total;
}'

