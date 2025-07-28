# Knowledge Graph Builder (Multi-Builder v2)

A containerized tool for building knowledge graphs from various biomedical data sources, with support for Amazon Neptune integration.

## Prerequisites

- Docker installed locally
- AWS CLI configured with appropriate credentials (if using S3 upload or Neptune loading)
- Access to an Amazon Neptune instance (if using Neptune loading)
- S3 buckets with appropriate permissions (if using S3 upload)

## Quick Start

1. **Build the Docker image:**
   ```bash
   ./build_multi_v2.sh
   ```

2. **Create workspace directory:**
   ```bash
   mkdir -p ./workspace && chmod 777 ./workspace
   ```

3. **Run with a configuration file:**
   ```bash
   sudo docker run -it --rm \
     -v "$(pwd)/workspace:/workspace" \
     kg-builder:multi-v2 \
     --config config/kg_config_enrichr_only.yaml
   ```

## Configuration-Based Usage

The multi-builder v2 uses YAML configuration files to define what to build and how to process the data.

### Available Configuration Files

- `config/kg_config_enrichr_only.yaml` - Build only Enrichr knowledge graph
- `config/kg_config_enrichr_hpo.yaml` - Build Enrichr + HPO knowledge graphs
- `config/kg_config_enrichr_s3.yaml` - Build Enrichr with S3 upload
- `config/kg_config_multi.yaml` - Build multiple knowledge graphs
- `config/kg_config_all_builders.yaml` - Build all supported knowledge graphs

### Example Usage

1. **Build Enrichr knowledge graph only:**
   ```bash
   sudo docker run -it --rm \
     -v "$(pwd)/workspace:/workspace" \
     kg-builder:multi-v2 \
     --config config/kg_config_enrichr_only.yaml
   ```

2. **Build Enrichr + HPO knowledge graphs:**
   ```bash
   sudo docker run -it --rm \
     -v "$(pwd)/workspace:/workspace" \
     kg-builder:multi-v2 \
     --config config/kg_config_enrichr_hpo.yaml
   ```

3. **Build with S3 upload:**
   ```bash
   sudo docker run -it --rm \
     -v "$(pwd)/workspace:/workspace" \
     -e AWS_ACCESS_KEY_ID=your_key \
     -e AWS_SECRET_ACCESS_KEY=your_secret \
     kg-builder:multi-v2 \
     --config config/kg_config_enrichr_s3.yaml
   ```

## Supported Data Libraries

The solution supports the following data libraries:

- `enrichr_kg_builder` - Enrichr gene set libraries
- `civic_kg_builder` - CIViC (Clinical Interpretation of Variants in Cancer)
- `mesh_nt_kg` - MESH (Medical Subject Headings) in NT format
- `mesh_xml_kg` - MESH (Medical Subject Headings) in XML format
- `hpo_configurable_kg_builder` - HPO (Human Phenotype Ontology)

## Configuration File Structure

Each configuration file should follow this structure:

```yaml
general:
  builders:
    - enrichr_kg_builder
    - hpo_configurable_kg_builder
  convert_to_neptune: true

s3:
  upload: false
  buckets:
    enrichr_kg_builder: "my-enrichr-bucket"
    hpo_configurable_kg_builder: "my-hpo-bucket"
  prefixes:
    enrichr_kg_builder: "enrichr-data/2025"
    hpo_configurable_kg_builder: "hpo-data/2025"

neptune:
  load: false
  endpoint: "my-neptune-endpoint.amazonaws.com"
  iam_role_arn: "arn:aws:iam::123456789012:role/NeptuneLoadRole"
```

## Output Structure

The container creates the following directory structure in `/workspace`:

```
workspace/
├── biocypher-out/          # Raw BioCypher output
│   ├── enrichr_kg_builder/
│   └── hpo_configurable_kg_builder/
├── neptune/                # Neptune-formatted files
│   ├── enrichr_kg_builder/
│   └── hpo_configurable_kg_builder/
├── config/                 # Configuration files
└── logs/                   # Log files
```

## Running in AWS

To run this container in AWS (e.g., on ECS or EC2):

1. **Build and push to ECR:**
   ```bash
   aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 123456789012.dkr.ecr.us-east-1.amazonaws.com
   docker tag kg-builder:multi-v2 123456789012.dkr.ecr.us-east-1.amazonaws.com/kg-builder:multi-v2
   docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/kg-builder:multi-v2
   ```

2. **Run on ECS/EC2 with appropriate IAM roles and environment variables.**

## Troubleshooting

### Common Issues

1. **Permission Issues**: Ensure the workspace directory has proper permissions (777)
2. **AWS Credentials**: Set AWS environment variables or use IAM roles
3. **Memory Issues**: Some builders require significant memory (4GB+ recommended)

### Checking Logs

Container logs show detailed progress and any errors during the build process.

## Cleanup

To remove unnecessary files and keep only what's needed for Dockerfile.multi.v2:

```bash
./cleanup_script.sh
```

This will remove old Dockerfiles, build scripts, and other unused files while preserving the essential components for the multi-builder v2 solution.
