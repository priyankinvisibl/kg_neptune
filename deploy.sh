#!/bin/bash
set -e

# Configuration
STACK_NAME="kg-builder-stack"
REGION="us-east-1"
S3_BUCKET_PREFIX="kg-data"
ECR_REPOSITORY="kg-builder"
BUILDERS="enrichr"
S3_BUCKETS=""
S3_PREFIXES=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --region)
      REGION="$2"
      shift
      shift
      ;;
    --stack-name)
      STACK_NAME="$2"
      shift
      shift
      ;;
    --s3-bucket-prefix)
      S3_BUCKET_PREFIX="$2"
      shift
      shift
      ;;
    --ecr-repository)
      ECR_REPOSITORY="$2"
      shift
      shift
      ;;
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
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo "Deploying Knowledge Graph Builder to AWS..."
echo "Region: $REGION"
echo "Stack Name: $STACK_NAME"
echo "S3 Bucket Prefix: $S3_BUCKET_PREFIX"
echo "ECR Repository: $ECR_REPOSITORY"
echo "Builders: $BUILDERS"
echo "S3 Buckets Mapping: $S3_BUCKETS"
echo "S3 Prefixes Mapping: $S3_PREFIXES"

# Create S3 bucket mapping if not provided
if [ -z "$S3_BUCKETS" ]; then
  # Convert comma-separated builders to array
  IFS=',' read -ra BUILDER_ARRAY <<< "$BUILDERS"
  
  # Create S3 bucket mapping
  for builder in "${BUILDER_ARRAY[@]}"; do
    if [ -z "$S3_BUCKETS" ]; then
      S3_BUCKETS="${builder}:${S3_BUCKET_PREFIX}-${builder}"
    else
      S3_BUCKETS="${S3_BUCKETS},${builder}:${S3_BUCKET_PREFIX}-${builder}"
    fi
  done
  
  echo "Generated S3 Buckets Mapping: $S3_BUCKETS"
fi

# Create CloudFormation stack
echo "Creating CloudFormation stack..."

# Convert S3 bucket mapping to CloudFormation parameters
IFS=',' read -ra BUCKET_PAIRS <<< "$S3_BUCKETS"
BUCKET_PARAMS=""

for pair in "${BUCKET_PAIRS[@]}"; do
  IFS=':' read -ra BUCKET_PAIR <<< "$pair"
  builder="${BUCKET_PAIR[0]}"
  bucket="${BUCKET_PAIR[1]}"
  
  # Add bucket to parameters
  BUCKET_PARAMS="$BUCKET_PARAMS ParameterKey=S3Bucket${builder^},ParameterValue=$bucket"
done

# Create the stack with dynamic parameters
aws cloudformation create-stack \
  --stack-name $STACK_NAME \
  --template-body file://cloudformation.yaml \
  --parameters \
    ParameterKey=ECRRepositoryName,ParameterValue=$ECR_REPOSITORY \
    $BUCKET_PARAMS \
  --capabilities CAPABILITY_IAM \
  --region $REGION

echo "Waiting for stack creation to complete..."
aws cloudformation wait stack-create-complete \
  --stack-name $STACK_NAME \
  --region $REGION

# Get ECR repository URI
ECR_REPOSITORY_URI=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --query "Stacks[0].Outputs[?OutputKey=='ECRRepositoryURI'].OutputValue" \
  --output text \
  --region $REGION)

echo "ECR Repository URI: $ECR_REPOSITORY_URI"

# Login to ECR
echo "Logging in to ECR..."
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR_REPOSITORY_URI

# Build and push Docker image
echo "Building Docker image..."
docker build -t $ECR_REPOSITORY_URI:latest .

echo "Pushing Docker image to ECR..."
docker push $ECR_REPOSITORY_URI:latest

# Get Neptune endpoint
NEPTUNE_ENDPOINT=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --query "Stacks[0].Outputs[?OutputKey=='NeptuneEndpoint'].OutputValue" \
  --output text \
  --region $REGION)

echo "Neptune Endpoint: $NEPTUNE_ENDPOINT"

# Get Neptune S3 role ARN
NEPTUNE_S3_ROLE_ARN=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --query "Stacks[0].Outputs[?OutputKey=='NeptuneS3RoleARN'].OutputValue" \
  --output text \
  --region $REGION)

echo "Neptune S3 Role ARN: $NEPTUNE_S3_ROLE_ARN"

# Prepare ECS command
COMMAND_ARGS="--builders $BUILDERS --neptune --upload-s3"

# Add S3 buckets if provided
if [ -n "$S3_BUCKETS" ]; then
  COMMAND_ARGS="$COMMAND_ARGS --s3-buckets '$S3_BUCKETS'"
fi

# Add S3 prefixes if provided
if [ -n "$S3_PREFIXES" ]; then
  COMMAND_ARGS="$COMMAND_ARGS --s3-prefixes '$S3_PREFIXES'"
fi

# Add Neptune parameters
COMMAND_ARGS="$COMMAND_ARGS --load-neptune --neptune-endpoint $NEPTUNE_ENDPOINT --iam-role-arn $NEPTUNE_S3_ROLE_ARN"

# Run ECS task
echo "Running ECS task with command: $COMMAND_ARGS"
aws ecs run-task \
  --cluster kg-builder-cluster \
  --task-definition kg-builder-task \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[$(aws cloudformation describe-stack-resources --stack-name $STACK_NAME --logical-resource-id NeptuneSubnet1 --query 'StackResources[0].PhysicalResourceId' --output text)],securityGroups=[$(aws cloudformation describe-stack-resources --stack-name $STACK_NAME --logical-resource-id NeptuneSecurityGroup --query 'StackResources[0].PhysicalResourceId' --output text)],assignPublicIp=ENABLED}" \
  --overrides "{\"containerOverrides\":[{\"name\":\"kg-builder-container\",\"command\":[\"$COMMAND_ARGS\"]}]}" \
  --region $REGION

echo "Deployment complete!"
echo "You can monitor the task in the ECS console and check the CloudWatch logs."
