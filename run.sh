#!/bin/bash
set -e

# Default values
BUILDERS="enrichr"
S3_BUCKETS=""
S3_PREFIXES=""
NEPTUNE_ENDPOINT=""
IAM_ROLE_ARN=""
OUTPUT_DIR="./output"
CONVERT_NEPTUNE=false
UPLOAD_S3=false
LOAD_NEPTUNE=false
IMAGE_NAME="kg-builder:latest"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --builders)
      BUILDERS="$2"
      shift
      shift
      ;;
    --s3-buckets)
      S3_BUCKETS="$2"
      shift
      shift
      ;;
    --s3-prefixes)
      S3_PREFIXES="$2"
      shift
      shift
      ;;
    --neptune-endpoint)
      NEPTUNE_ENDPOINT="$2"
      shift
      shift
      ;;
    --iam-role-arn)
      IAM_ROLE_ARN="$2"
      shift
      shift
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift
      shift
      ;;
    --output-dirs)
      OUTPUT_DIRS="$2"
      shift
      shift
      ;;
    --config-dir)
      CONFIG_DIR="$2"
      shift
      shift
      ;;
    --neptune)
      CONVERT_NEPTUNE=true
      shift
      ;;
    --upload-s3)
      UPLOAD_S3=true
      shift
      ;;
    --load-neptune)
      LOAD_NEPTUNE=true
      shift
      ;;
    --image-name)
      IMAGE_NAME="$2"
      shift
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo "Running Knowledge Graph Builder..."
echo "Builders: $BUILDERS"
echo "Output Directory: $OUTPUT_DIR"
echo "Output Directories: $OUTPUT_DIRS"
echo "Convert to Neptune: $CONVERT_NEPTUNE"
echo "Upload to S3: $UPLOAD_S3"
echo "S3 Buckets: $S3_BUCKETS"
echo "S3 Prefixes: $S3_PREFIXES"
echo "Load to Neptune: $LOAD_NEPTUNE"
echo "Neptune Endpoint: $NEPTUNE_ENDPOINT"
echo "IAM Role ARN: $IAM_ROLE_ARN"

# Build Docker image
echo "Building Docker image..."
docker build -t $IMAGE_NAME .

# Prepare command arguments
CMD_ARGS="--builders $BUILDERS"

if [ -n "$OUTPUT_DIR" ]; then
  CMD_ARGS="$CMD_ARGS --output-dir /app/output"
fi

if [ -n "$OUTPUT_DIRS" ]; then
  CMD_ARGS="$CMD_ARGS --output-dirs \"$OUTPUT_DIRS\""
fi

if [ "$CONVERT_NEPTUNE" = true ]; then
  CMD_ARGS="$CMD_ARGS --neptune"
fi

if [ "$UPLOAD_S3" = true ]; then
  CMD_ARGS="$CMD_ARGS --upload-s3"
  
  if [ -n "$S3_BUCKETS" ]; then
    CMD_ARGS="$CMD_ARGS --s3-buckets \"$S3_BUCKETS\""
  fi
  
  if [ -n "$S3_PREFIXES" ]; then
    CMD_ARGS="$CMD_ARGS --s3-prefixes \"$S3_PREFIXES\""
  fi
fi

if [ "$LOAD_NEPTUNE" = true ]; then
  CMD_ARGS="$CMD_ARGS --load-neptune"
  
  if [ -n "$NEPTUNE_ENDPOINT" ]; then
    CMD_ARGS="$CMD_ARGS --neptune-endpoint \"$NEPTUNE_ENDPOINT\""
  fi
  
  if [ -n "$IAM_ROLE_ARN" ]; then
    CMD_ARGS="$CMD_ARGS --iam-role-arn \"$IAM_ROLE_ARN\""
  fi
fi

# Create output directory if it doesn't exist
if [ -n "$OUTPUT_DIR" ]; then
  mkdir -p "$OUTPUT_DIR"
  # Ensure the output directory has the right permissions
  chmod 777 "$OUTPUT_DIR"
fi

# Create output directories from mapping if provided
if [ -n "$OUTPUT_DIRS" ]; then
  # Parse the mapping
  IFS=',' read -ra DIR_PAIRS <<< "$OUTPUT_DIRS"
  for pair in "${DIR_PAIRS[@]}"; do
    IFS=':' read -ra DIR_PAIR <<< "$pair"
    if [ ${#DIR_PAIR[@]} -eq 2 ]; then
      builder="${DIR_PAIR[0]}"
      dir="${DIR_PAIR[1]}"
      mkdir -p "$dir"
      chmod 777 "$dir"
      echo "Created output directory for $builder: $dir"
    fi
  done
fi

# Run Docker container
echo "Running Docker container with command: $CMD_ARGS"

# Start with basic Docker run command
DOCKER_CMD="docker run -it --rm"

# Add volume mount for main output directory if specified
if [ -n "$OUTPUT_DIR" ]; then
  DOCKER_CMD="$DOCKER_CMD -v \"$OUTPUT_DIR:/app/output\""
  
  # Also mount biocypher-out directory to preserve all intermediate files
  BIOCYPHER_OUT_DIR="$OUTPUT_DIR/biocypher-out"
  mkdir -p "$BIOCYPHER_OUT_DIR"
  chmod 777 "$BIOCYPHER_OUT_DIR"
  DOCKER_CMD="$DOCKER_CMD -v \"$BIOCYPHER_OUT_DIR:/app/biocypher-out\""
fi

# Add volume mount for config directory if specified
if [ -n "$CONFIG_DIR" ]; then
  DOCKER_CMD="$DOCKER_CMD -v \"$CONFIG_DIR:/app/config\""
else
  # Mount the default config directory
  DOCKER_CMD="$DOCKER_CMD -v \"$(pwd)/config:/app/config\""
fi

# Add volume mounts for each output directory in the mapping
if [ -n "$OUTPUT_DIRS" ]; then
  # Parse the mapping
  IFS=',' read -ra DIR_PAIRS <<< "$OUTPUT_DIRS"
  for pair in "${DIR_PAIRS[@]}"; do
    IFS=':' read -ra DIR_PAIR <<< "$pair"
    if [ ${#DIR_PAIR[@]} -eq 2 ]; then
      builder="${DIR_PAIR[0]}"
      dir="${DIR_PAIR[1]}"
      # Create a unique mount point in the container
      DOCKER_CMD="$DOCKER_CMD -v \"$dir:/app/custom_output/$builder\""
      
      # Update the output_dirs mapping to use the container paths
      if [ -z "$CONTAINER_OUTPUT_DIRS" ]; then
        CONTAINER_OUTPUT_DIRS="$builder:/app/custom_output/$builder"
      else
        CONTAINER_OUTPUT_DIRS="$CONTAINER_OUTPUT_DIRS,$builder:/app/custom_output/$builder"
      fi
    fi
  done
  
  # Update the command args to use the container paths
  if [ -n "$CONTAINER_OUTPUT_DIRS" ]; then
    CMD_ARGS="$CMD_ARGS --output-dirs \"$CONTAINER_OUTPUT_DIRS\""
  fi
fi

# Add environment variables
DOCKER_CMD="$DOCKER_CMD -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_REGION"

# Add image name and command args
DOCKER_CMD="$DOCKER_CMD $IMAGE_NAME $CMD_ARGS"

# Execute the Docker command
eval $DOCKER_CMD

# Fix permissions on output directories
echo "Fixing permissions on output directories..."
if [ -n "$OUTPUT_DIR" ]; then
  chmod -R 755 "$OUTPUT_DIR"
  # Change ownership to the current user if running with sudo
  if [ -n "$SUDO_USER" ]; then
    chown -R $SUDO_USER:$SUDO_USER "$OUTPUT_DIR"
  fi
  
  # Copy Neptune files from biocypher-out to the output directory
  if [ "$CONVERT_NEPTUNE" = true ]; then
    # Copy the Neptune files from the biocypher-out directory
    echo "Copying Neptune files to output directory"
    mkdir -p "$OUTPUT_DIR/enrichr/neptune"
    cp -r ./biocypher-out/20250723154503_neptune/* "$OUTPUT_DIR/enrichr/neptune/"
    
    # Fix permissions on the copied files
    chmod -R 755 "$OUTPUT_DIR/enrichr/neptune"
    if [ -n "$SUDO_USER" ]; then
      chown -R $SUDO_USER:$SUDO_USER "$OUTPUT_DIR/enrichr/neptune"
    fi
    
    # Remove empty neptune_neptune directory if it exists
    if [ -d "$OUTPUT_DIR/enrichr/neptune_neptune" ]; then
      rmdir "$OUTPUT_DIR/enrichr/neptune_neptune"
    fi
  fi
fi

# Fix permissions on custom output directories
if [ -n "$OUTPUT_DIRS" ]; then
  IFS=',' read -ra DIR_PAIRS <<< "$OUTPUT_DIRS"
  for pair in "${DIR_PAIRS[@]}"; do
    IFS=':' read -ra DIR_PAIR <<< "$pair"
    if [ ${#DIR_PAIR[@]} -eq 2 ]; then
      builder="${DIR_PAIR[0]}"
      dir="${DIR_PAIR[1]}"
      chmod -R 755 "$dir"
      # Change ownership to the current user if running with sudo
      if [ -n "$SUDO_USER" ]; then
        chown -R $SUDO_USER:$SUDO_USER "$dir"
      fi
      echo "Fixed permissions for $builder output directory: $dir"
      
      # Copy Neptune files from biocypher-out to the output directory
      if [ "$CONVERT_NEPTUNE" = true ]; then
        # Find the latest Neptune output directory
        LATEST_NEPTUNE_DIR=$(find ./biocypher-out -name "*_neptune" -type d | sort | tail -1)
        if [ -n "$LATEST_NEPTUNE_DIR" ]; then
          echo "Copying Neptune files from $LATEST_NEPTUNE_DIR to $dir/neptune"
          cp -r $LATEST_NEPTUNE_DIR/* "$dir/neptune/"
          # Fix permissions on the copied files
          chmod -R 755 "$dir/neptune"
          if [ -n "$SUDO_USER" ]; then
            chown -R $SUDO_USER:$SUDO_USER "$dir/neptune"
          fi
        fi
      fi
    fi
  done
fi

echo "Knowledge Graph Build Complete!"
if [ -n "$OUTPUT_DIR" ]; then
  echo "Main output available in: $OUTPUT_DIR"
fi
if [ -n "$OUTPUT_DIRS" ]; then
  echo "Custom outputs available in:"
  IFS=',' read -ra DIR_PAIRS <<< "$OUTPUT_DIRS"
  for pair in "${DIR_PAIRS[@]}"; do
    IFS=':' read -ra DIR_PAIR <<< "$pair"
    if [ ${#DIR_PAIR[@]} -eq 2 ]; then
      builder="${DIR_PAIR[0]}"
      dir="${DIR_PAIR[1]}"
      echo "  - $builder: $dir"
    fi
  done
fi
