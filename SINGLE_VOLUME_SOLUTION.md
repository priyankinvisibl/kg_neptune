# Single Volume Mount Solution

## Problem Solved

**Before (Complex Multi-Volume Mount):**
```bash
sudo docker run -it --rm \
  -v "$(pwd)/output:/app/output" \
  -v "$(pwd)/config:/app/config" \
  -v "$(pwd)/output/biocypher-out:/app/biocypher-out" \
  kg-builder:path-fix \
  --config /app/config/kg_config.yaml
```

**After (Simple Single Volume Mount):**
```bash
docker run -it --rm \
  -v "$(pwd)/workspace:/workspace" \
  kg-builder:single-volume \
  --config config/kg_config.yaml
```

## Quick Start

### 1. Build the Image
```bash
docker build -f Dockerfile.single-volume -t kg-builder:single-volume .
```

### 2. Create Workspace
```bash
mkdir -p ./workspace && chmod 777 ./workspace
```

### 3. Run the Build
```bash
docker run -it --rm \
  -v "$(pwd)/workspace:/workspace" \
  kg-builder:single-volume \
  --config config/kg_config.yaml
```

## What You Get

All outputs organized in a single workspace directory:

```
./workspace/
├── config/
│   └── kg_config.yaml          # Auto-copied config
├── output/
│   └── info.txt                # Raw outputs  
├── biocypher-out/
│   └── [timestamp]/            # BioCypher CSV files
├── neptune/                    # Neptune-formatted files
│   ├── node_Gene.csv
│   ├── node_Pathway.csv
│   ├── edges_PathwayGeneInteraction.csv
│   └── [other Neptune files]
├── logs/
│   └── [log files]
└── build_summary.txt           # Build summary
```

## Benefits

1. ✅ **Single volume mount** - eliminates nested mount conflicts
2. ✅ **Automatic Neptune conversion** - works inside Docker
3. ✅ **No sudo required** - works with regular Docker permissions
4. ✅ **Organized structure** - everything in logical subdirectories
5. ✅ **Easy backup** - just copy the workspace directory
6. ✅ **Production ready** - clean, tested solution

## Files

- `Dockerfile.single-volume` - Docker image for single volume mount
- `run_single_volume.py` - Python runner with workspace organization
- `SINGLE_VOLUME_SOLUTION.md` - This documentation

## Usage Examples

### Basic Build
```bash
mkdir -p ./my_workspace && chmod 777 ./my_workspace

docker run -it --rm \
  -v "$(pwd)/my_workspace:/workspace" \
  kg-builder:single-volume \
  --config config/kg_config.yaml
```

### With AWS Credentials
```bash
docker run -it --rm \
  -v "$(pwd)/my_workspace:/workspace" \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION \
  kg-builder:single-volume \
  --config config/kg_config.yaml
```

This solution provides a clean, single-command approach to building knowledge graphs with automatic Neptune file generation.
