#!/bin/bash

# Configuration
REMOTE_HOST="your_remote_host"
REMOTE_USER="your_username"
REMOTE_PATH="/path/to/remote/directory"
FILE_LIST="file_list.txt"
MAX_PARALLEL=5

# Function to transfer a single file
transfer_file() {
    local file="$1"
    local size=$(stat -c%s "$file")
    local transferred=0

    sftp -b - "$REMOTE_USER@$REMOTE_HOST" <<EOF
        progress
        put "$file" "$REMOTE_PATH/$(basename "$file")"
        bye
EOF

    echo "100" > "$file.progress"
}

# Export the function so it can be used by parallel
export -f transfer_file

# Start transfers in parallel
cat "$FILE_LIST" | parallel -j "$MAX_PARALLEL" --eta --progress --bar transfer_file {}

# Clean up progress files
rm -f *.progress

echo "All transfers completed."
