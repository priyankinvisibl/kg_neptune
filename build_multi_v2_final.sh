#!/bin/bash

# Build script for Multi-Builder Knowledge Graph Builder v2 (FINAL)
# This version properly handles output directory mapping and only processes current run

echo "Building Multi-Builder Knowledge Graph Builder v2 (FINAL)..."

# Build the Docker image
docker build -f Dockerfile.multi.v2.final -t kg-builder:multi-v2-final .

if [ $? -eq 0 ]; then
    echo "✅ Docker image built successfully: kg-builder:multi-v2-final"
    echo ""
    echo "Usage:"
    echo "  # Run with Enrichr only (recommended for testing):"
    echo "  sudo docker run -it --rm \\"
    echo "    -v \"\$(pwd)/workspace:/workspace\" \\"
    echo "    kg-builder:multi-v2-final \\"
    echo "    --config config/kg_config_enrichr_only.yaml"
    echo ""
    echo "  # Run with original config (may fail due to missing HPO data):"
    echo "  sudo docker run -it --rm \\"
    echo "    -v \"\$(pwd)/workspace:/workspace\" \\"
    echo "    kg-builder:multi-v2-final \\"
    echo "    --config config/kg_config_s3.yaml"
    echo ""
    echo "To clean up old runs before building:"
    echo "  ./cleanup_old_runs.sh"
else
    echo "❌ Docker build failed"
    exit 1
fi
