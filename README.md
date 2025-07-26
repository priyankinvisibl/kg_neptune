# Knowledge Graph Builder

A tool for building knowledge graphs from various biomedical data sources, with support for Amazon Neptune integration.

## Prerequisites

- Docker installed locally
- AWS CLI configured with appropriate credentials (if using S3 upload or Neptune loading)
- Access to an Amazon Neptune instance (if using Neptune loading)
- S3 buckets with appropriate permissions (if using S3 upload)

## Quick Start

Run the script with default settings (builds Enrichr knowledge graph):

```bash
./run.sh
```

## Usage Examples

### 1. Build Enrichr Knowledge Graph

```bash
./run.sh --builders enrichr
```

### 2. Build Multiple Knowledge Graphs

```bash
./run.sh --builders "enrichr civic"
```

### 3. Convert to Neptune Format

```bash
./run.sh --builders enrichr --neptune
```

### 4. Upload to S3

```bash
./run.sh --builders enrichr --neptune --upload-s3 --s3-buckets "enrichr:my-enrichr-bucket"
```

### 5. Load into Neptune

```bash
./run.sh --builders enrichr --neptune --upload-s3 --s3-buckets "enrichr:my-enrichr-bucket" --load-neptune --neptune-endpoint "my-neptune-endpoint.amazonaws.com" --iam-role-arn "arn:aws:iam::123456789012:role/NeptuneLoadRole"
```

### 6. Build Multiple Knowledge Graphs with Separate S3 Buckets

```bash
./run.sh --builders "enrichr civic" --neptune --upload-s3 --s3-buckets "enrichr:my-enrichr-bucket,civic:my-civic-bucket" --load-neptune --neptune-endpoint "my-neptune-endpoint.amazonaws.com" --iam-role-arn "arn:aws:iam::123456789012:role/NeptuneLoadRole"
```

## Command Line Arguments

- `--builders`: Data libraries to build (required, space-separated list)
- `--output-dir`: Local output directory (default: ./output)
- `--neptune`: Convert output to Neptune format
- `--upload-s3`: Upload Neptune files to S3
- `--s3-buckets`: S3 bucket mapping (e.g., 'enrichr:bucket1,civic:bucket2')
- `--s3-prefixes`: S3 prefix mapping (e.g., 'enrichr:prefix1,civic:prefix2')
- `--load-neptune`: Load data into Neptune database
- `--neptune-endpoint`: Neptune endpoint URL
- `--iam-role-arn`: IAM role ARN for Neptune to access S3
- `--image-name`: Docker image name (default: kg-builder:latest)

## Supported Data Libraries

The solution supports the following data libraries:

- `enrichr`: Enrichr gene set libraries
- `civic`: CIViC (Clinical Interpretation of Variants in Cancer)
- `mesh_nt`: MESH (Medical Subject Headings) in NT format
- `mesh_xml`: MESH (Medical Subject Headings) in XML format
- `hpo`: HPO (Human Phenotype Ontology)

## S3 Bucket and Prefix Mapping

The `--s3-buckets` and `--s3-prefixes` arguments accept a comma-separated list of key-value pairs in the format `library:value`. For example:

```
--s3-buckets "enrichr:my-enrichr-bucket,civic:my-civic-bucket"
--s3-prefixes "enrichr:enrichr-data/2025,civic:civic-data/2025"
```

## Running in AWS

To run this container in AWS (e.g., on ECS or EC2):

1. Build and push the Docker image to Amazon ECR:

```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 123456789012.dkr.ecr.us-east-1.amazonaws.com
docker build -t 123456789012.dkr.ecr.us-east-1.amazonaws.com/kg-builder:latest .
docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/kg-builder:latest
```

2. Run the container on ECS or EC2 with the appropriate command-line arguments.

## Troubleshooting

### Common Issues

1. **AWS Credentials**: Ensure that your AWS credentials are properly configured and have the necessary permissions for S3 and Neptune.

2. **Neptune Connection**: Verify that the Neptune endpoint is accessible from your environment and that the security group allows connections.

3. **S3 Permissions**: Ensure that the IAM role has the necessary permissions to read from and write to the S3 buckets.

### Checking Logs

The container logs will show the progress and any errors during the build process.
