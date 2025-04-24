#!/bin/bash

# Output ZIP filename
output_zip_file="osora_ai_context.zip"
current_dir=$(pwd)

# Define patterns for directories to exclude
excluded_directories=(
    '.venv'
    '__pycache__'
    '.pytest_cache'
    '.vscode'
    '.history'
    'tests'
    'dist'
    'build'
    '.git'
    'deployment'
    'node_modules'
    'bin'
    'obj'
    'osora_az'
    'env'
    'venv'
    '.github'
    '.idea'
    'temp'
    'tmp'
    'logs'
)

# Define patterns for files to exclude
excluded_files=(
    'local.settings.json'
    '*.zip'
    '*.pyc'
    '*.pyo'
    '*.log'
    '*.tmp'
    '.env'
    '*.egg-info'
    '__azurite*.json'
    '*.bak'
    '*.so'
    '*.dll'
    '*.exe'
    '*.pkl'
    '*.db'
    '*.dat'
    '*.o'
    '*.a'
    '*.swp'
    '*.swo'
    '*.docx'
    '*.xlsx'
    '*.pptx'
    '*.psd'
    '*.ttf'
    '*.woff'
    '*.woff2'
    '*.eot'
    '*.mp4'
    '*.mov'
    '*.mp3'
    '*.wav'
    '*.pdf'
    '*.png'
    '*.jpg'
    '*.jpeg'
    '*.gif'
    '*.ico'
    '*.svg'
    '.DS_Store'
    'Thumbs.db'
)

# Define which file types to explicitly include
included_extensions=(
    "*.py"
    "*.json"
    "requirements.txt"
    "host.json"
    ".cursorrules"
    "README.md"
)

# Create a temporary directory for the filtered files
temp_dir=$(mktemp -d)
echo "Using temporary directory: $temp_dir"

# Create essential Azure Functions structure
mkdir -p "$temp_dir/OneDriveChangeWebhook"

# Essential files that must be included
essential_files=(
    "host.json"
    "requirements.txt"
    "OneDriveChangeWebhook/__init__.py"
    "OneDriveChangeWebhook/function.json"
)

# First copy essential files
echo "Copying essential files..."
for file in "${essential_files[@]}"; do
    if [ -f "$file" ]; then
        # Ensure the directory exists
        dir=$(dirname "$file")
        mkdir -p "$temp_dir/$dir"
        # Copy the file
        cp "$file" "$temp_dir/$file"
        echo "  Added essential file: $file"
    else
        echo "  Warning: Essential file not found: $file"
    fi
done

# Build a find command to find only the file types we want to include
include_patterns=""
for pattern in "${included_extensions[@]}"; do
    include_patterns="$include_patterns -name '$pattern' -o"
done
# Remove trailing '-o'
include_patterns=${include_patterns%-o}

# Build exclusion patterns for directories
dir_exclude=""
for dir in "${excluded_directories[@]}"; do
    dir_exclude="$dir_exclude -path './$dir' -o -path './$dir/*' -o"
done
# Remove trailing '-o'
dir_exclude=${dir_exclude%-o}

# Build exclusion patterns for files
file_exclude=""
for file in "${excluded_files[@]}"; do
    file_exclude="$file_exclude -name '$file' -o"
done
# Remove trailing '-o'
file_exclude=${file_exclude%-o}

# Find Python files and essential config files, excluding specified patterns
echo "Finding additional relevant files..."
find_command="find . -type f \( $include_patterns \) -not \( $dir_exclude \) -not \( $file_exclude \)"
echo "Using find command: $find_command"

additional_files=$(eval "$find_command")

# Remove existing zip file if it exists
if [ -f "$output_zip_file" ]; then
    echo "Removing existing zip file: $output_zip_file"
    rm -f "$output_zip_file"
fi

# Copy the additional files to the temp directory
if [ -n "$additional_files" ]; then
    echo "$additional_files" | while read -r file; do
        # Skip if it's in the temp directory
        if [[ "$file" == "$temp_dir"* ]]; then
            continue
        fi

        # Skip if it's already in the essential files
        already_copied=false
        for ess_file in "${essential_files[@]}"; do
            if [ "$file" = "./$ess_file" ] || [ "$file" = "$ess_file" ]; then
                already_copied=true
                break
            fi
        done

        if [ "$already_copied" = false ]; then
            # Create target directory
            target_file="${file:2}" # Remove leading ./
            target_dir=$(dirname "$temp_dir/$target_file")
            mkdir -p "$target_dir"
            
            # Copy file
            cp "$file" "$temp_dir/$target_file"
            filesize=$(du -h "$file" | cut -f1)
            echo "  Added: $target_file ($filesize)"
        fi
    done
    
    # Optional: Limit file sizes to prevent extremely large files
    find "$temp_dir" -type f -size +500k -exec echo "  Warning: Large file (will be truncated): {}" \; -exec truncate -s 500k {} \;
    
    # Create ZIP file from the temp directory - using -0 for no compression (store only)
    echo "Creating zip archive '$output_zip_file' with no compression..."
    (cd "$temp_dir" && zip -0 -r "$current_dir/$output_zip_file" .)
    
    # Check if ZIP creation was successful
    if [ -f "$output_zip_file" ]; then
        # Get the size of the ZIP file
        zip_size=$(du -h "$output_zip_file" | cut -f1)
        echo "Archive created successfully: $output_zip_file (Size: $zip_size)"
        
        # Optional: Count files in the archive
        file_count=$(unzip -l "$output_zip_file" | tail -n 1 | awk '{print $2}')
        echo "Total files in archive: $file_count"
        echo "Files stored without compression (no deflation)."
    else
        echo "Error creating ZIP archive. File was not created."
    fi
else
    echo "No files found matching the inclusion criteria after applying exclusions."
fi

# Clean up
rm -rf "$temp_dir"