# Multi-Builder Usage Guide

## Quick Start with sudo

### 1. Build the Image
```bash
sudo docker build -f Dockerfile.multi.v2 -t kg-builder:multi-v2 .
```

### 2. Create Workspace
```bash
mkdir -p ./workspace && chmod 777 ./workspace
```

### 3. Run Examples

#### Single Builder - Enrichr Only
```bash
sudo docker run -it --rm \
  -v "$(pwd)/workspace:/workspace" \
  kg-builder:multi-v2 \
  --config config/kg_config_enrichr_only.yaml
```

#### Single Builder - HPO Only
```bash
sudo docker run -it --rm \
  -v "$(pwd)/workspace:/workspace" \
  kg-builder:multi-v2 \
  --config config/kg_config_hpo_only.yaml
```

#### Multiple Builders - Enrichr + HPO
```bash
sudo docker run -it --rm \
  -v "$(pwd)/workspace:/workspace" \
  kg-builder:multi-v2 \
  --config config/kg_config_enrichr_hpo.yaml
```

#### With S3 Upload (when enabled in config)
```bash
sudo docker run -it --rm \
  -v "$(pwd)/workspace:/workspace" \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION \
  kg-builder:multi-v2 \
  --config config/kg_config_with_s3.yaml
```

## Configuration Examples

### Enrichr Only
```yaml
# config/kg_config_enrichr_only.yaml
general:
  builders: ["enrichr"]
  convert_to_neptune: true

datasets:
  enrichr:
    reactome:
      url: "https://maayanlab.cloud/Enrichr/geneSetLibrary?mode=text&libraryName=Reactome_2022"
      adapter: "ReactomeAdapter"

s3:
  upload: false
  bucket: ""
  prefix: ""
```

### Multiple Builders
```yaml
# config/kg_config_enrichr_hpo.yaml
general:
  builders: ["enrichr", "hpo"]
  convert_to_neptune: true

s3:
  upload: false
  bucket: ""
  prefix: ""
```

### With S3 Upload
```yaml
# config/kg_config_with_s3.yaml
general:
  builders: ["enrichr"]
  convert_to_neptune: true

s3:
  upload: true
  bucket: "my-kg-bucket"
  prefix: "enrichr-data"
```

## Expected Output Structure

```
workspace/
├── config/
│   └── kg_config.yaml
├── biocypher-out/
│   ├── enrichr_20250126_143022/
│   └── hpo_20250126_143045/
├── neptune/
│   ├── enrichr_20250126_143022/
│   │   ├── node_Gene.csv
│   │   └── edges_PathwayGeneInteraction.csv
│   └── hpo_20250126_143045/
│       ├── node_Phenotype.csv
│       └── edges_PhenotypeGeneInteraction.csv
├── logs/
└── build_summary.txt
```

## Troubleshooting

### Permission Issues
If you get permission errors:
```bash
# Make sure workspace has correct permissions
chmod 777 ./workspace

# Or run with user mapping
sudo docker run -it --rm \
  -v "$(pwd)/workspace:/workspace" \
  --user $(id -u):$(id -g) \
  kg-builder:multi-v2 \
  --config config/kg_config.yaml
```

### Docker Permission Issues
If Docker requires sudo but you want to avoid it:
```bash
# Add your user to docker group (requires logout/login)
sudo usermod -aG docker $USER

# Then you can run without sudo
docker run -it --rm ...
```

### Check Build Summary
Always check the build summary for detailed results:
```bash
cat workspace/build_summary.txt
```

## Available Builders

- ✅ **enrichr** - Enrichr gene set libraries
- ✅ **civic** - Clinical Interpretation of Variants in Cancer  
- ✅ **hpo** - Human Phenotype Ontology
- ✅ **mesh_nt** - Medical Subject Headings (NT format)
- ✅ **mesh_xml** - Medical Subject Headings (XML format)

## Builder Combinations

You can use any combination:
```yaml
builders: ["enrichr"]                    # Single
builders: ["enrichr", "hpo"]            # Dual
builders: ["enrichr", "civic", "hpo"]   # Triple
builders: ["enrichr", "civic", "hpo", "mesh_nt", "mesh_xml"]  # All
```
