#!/bin/bash

# Define variables
SOURCE_FILE="jni/build/lib/libopensearch_sparse_ann.so"  # Relative to script location
FILENAME=$(basename "$SOURCE_FILE")
ZIP_PATTERN="opensearch-neural-search-*.zip"

# Function to add lib folder with file to a zip archive
add_lib_to_zip() {
    local zip_file="$1"
    local source_file="$2"
    local filename="$3"

    echo "Processing: ${zip_file}"

    # Verify source file exists
    if [ ! -f "${source_file}" ]; then
        echo "ERROR: Source file not found: ${source_file}"
        return 1
    fi

    # Verify zip file exists
    if [ ! -f "${zip_file}" ]; then
        echo "ERROR: Zip file not found: ${zip_file}"
        return 1
    fi

    echo "Adding ${filename} to lib folder in ${zip_file}"

    # Create a temporary directory structure
    local temp_dir=$(mktemp -d)
    mkdir -p "$temp_dir/lib"

    # Copy source file to lib folder
    cp "${source_file}" "$temp_dir/lib/${filename}"

    # Add lib folder to the zip
    (cd "$temp_dir" && zip -ur "${zip_file}" lib)

    # Clean up
    rm -rf "$temp_dir"

    echo "Updated: ${zip_file}"
    echo "-------------------"
}

# Verify source file exists before proceeding
if [ ! -f "$SOURCE_FILE" ]; then
    echo "ERROR: Source file does not exist: $SOURCE_FILE"
    echo "Please update the SOURCE_FILE variable in the script."
    exit 1
fi

echo "Using source file: $SOURCE_FILE"

# Find all matching zip files with their absolute paths
echo "Searching for $ZIP_PATTERN files..."
zip_files=($(find . -name "$ZIP_PATTERN" | xargs realpath))

# Process each zip file
found_files=${#zip_files[@]}
if [ $found_files -eq 0 ]; then
    echo "No files matching '$ZIP_PATTERN' were found in $(pwd)"
else
    echo "Found $found_files matching zip files:"
    for zip_file in "${zip_files[@]}"; do
        echo "- $zip_file"
        add_lib_to_zip "$zip_file" "$SOURCE_FILE" "$FILENAME"
    done
    echo "Processed $found_files zip files"
fi
