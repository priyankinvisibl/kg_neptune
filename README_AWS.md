# Knowledge Graph Builder - AWS Deployment

This guide explains how to deploy the Knowledge Graph Builder on AWS, including uploading data to S3 and loading it into Amazon Neptune. The solution supports multiple data libraries (Enrichr, CIViC, MESH, HPO) with separate S3 buckets for each.

## Prerequisites

- AWS CLI installed and configured with appropriate permissions
- Docker installed locally
- An AWS account with permissions to create:
  - CloudFormation stacks
  - S3 buckets
  - ECR repositories
  - Neptune clusters
  - ECS clusters
  - IAM roles

## Deployment Steps

### 1. Clone the Repository

```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Deploy the AWS Infrastructure

Run the deployment script:

```bash
./deploy.sh
```

This script will:
1. Create a CloudFormation stack with all required resources
2. Build and push the Docker image to ECR
3. Run an ECS task to build the knowledge graph, upload it to S3, and load it into Neptune

You can customize the deployment with the following options:

```bash
./deploy.sh \
  --region us-east-1 \
  --stack-name kg-builder-stack \
  --s3-bucket-prefix kg-data \
  --ecr-repository kg-builder \
  --builders "enrichr,civic" \
  --s3-buckets "enrichr:my-enrichr-bucket,civic:my-civic-bucket" \
  --s3-prefixes "enrichr:enrichr-data,civic:civic-data"
```

### 3. Monitor the Deployment

You can monitor the deployment in the AWS Management Console:

1. CloudFormation: Check the stack creation progress
2. ECS: Monitor the task execution
3. CloudWatch Logs: View the application logs
4. Neptune: Verify data loading

### 4. Access the Neptune Database

Once the deployment is complete, you can access the Neptune database using the endpoint provided in the CloudFormation outputs:

```bash
aws cloudformation describe-stacks \
  --stack-name kg-builder-stack \
  --query "Stacks[0].Outputs[?OutputKey=='NeptuneEndpoint'].OutputValue" \
  --output text
```

## Manual Execution

If you want to run the process manually:

1. Build the Docker image:

```bash
docker build -t kg-builder .
```

2. Run the container:

```bash
docker run -it --rm \
  -e AWS_ACCESS_KEY_ID=<your-access-key> \
  -e AWS_SECRET_ACCESS_KEY=<your-secret-key> \
  -e AWS_REGION=<your-region> \
  kg-builder \
  --builders enrichr civic \
  --neptune \
  --upload-s3 \
  --s3-buckets "enrichr:my-enrichr-bucket,civic:my-civic-bucket" \
  --load-neptune \
  --neptune-endpoint <your-neptune-endpoint> \
  --iam-role-arn <your-iam-role-arn>
```

## Supported Data Libraries

The solution supports the following data libraries:

- `enrichr`: Enrichr gene set libraries
- `civic`: CIViC (Clinical Interpretation of Variants in Cancer)
- `mesh_nt`: MESH (Medical Subject Headings) in NT format
- `mesh_xml`: MESH (Medical Subject Headings) in XML format
- `hpo`: HPO (Human Phenotype Ontology)

## Command Line Arguments

The application supports the following command line arguments:

- `--builders`, `-b`: Data libraries to build (required)
- `--output-dir`, `-o`: Base output directory
- `--neptune`, `-n`: Convert output to Neptune format
- `--upload-s3`: Upload Neptune files to S3
- `--s3-buckets`: S3 bucket mapping (e.g., 'enrichr:bucket1,civic:bucket2')
- `--s3-prefixes`: S3 prefix mapping (e.g., 'enrichr:prefix1,civic:prefix2')
- `--load-neptune`: Load data into Neptune database
- `--neptune-endpoint`: Neptune endpoint URL
- `--iam-role-arn`: IAM role ARN for Neptune to access S3

## S3 Bucket and Prefix Mapping

The `--s3-buckets` and `--s3-prefixes` arguments accept a comma-separated list of key-value pairs in the format `library:value`. For example:

```
--s3-buckets "enrichr:my-enrichr-bucket,civic:my-civic-bucket"
--s3-prefixes "enrichr:enrichr-data/2025,civic:civic-data/2025"
```

If not specified, the deployment script will automatically generate bucket names based on the `--s3-bucket-prefix` argument.

## Cleanup

To delete all resources created by this deployment:

```bash
aws cloudformation delete-stack --stack-name kg-builder-stack
```

## Troubleshooting

### Common Issues

1. **S3 Bucket Already Exists**: S3 bucket names are globally unique. If the deployment fails because the bucket already exists, try a different bucket name.

2. **Neptune Connection Issues**: Ensure that the security group allows traffic on port 8182 and that the ECS task is running in the same VPC as Neptune.

3. **IAM Permissions**: Verify that the IAM roles have the necessary permissions for S3 access and Neptune loading.

### Checking Logs

To view the application logs:

```bash
aws logs get-log-events \
  --log-group-name /ecs/kg-builder \
  --log-stream-name <log-stream-name>
```

Replace `<log-stream-name>` with the actual log stream name, which you can find in the CloudWatch console.
