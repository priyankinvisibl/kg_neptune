cat > neptune-comprehensive-trust-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "rds.amazonaws.com",
                    "neptune.amazonaws.com"
                ]
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
cat > neptune-comprehensive-permissions.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3Access",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket",
                "s3:ListBucketVersions",
                "s3:GetBucketLocation",
                "s3:GetBucketAcl",
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::neptune-test-data",
                "arn:aws:s3:::neptune-test-data/*"
            ]
        },
        {
            "Sid": "NeptuneDataAccess",
            "Effect": "Allow",
            "Action": [
                "neptune-db:*"
            ],
            "Resource": "*"
        },
        {
            "Sid": "NeptuneManagementAccess",
            "Effect": "Allow",
            "Action": [
                "neptune:DescribeDBClusters",
                "neptune:DescribeDBInstances",
                "neptune:ListTagsForResource"
            ],
            "Resource": "*"
        },
        {
            "Sid": "CloudWatchLogs",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:DescribeLogStreams"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Sid": "IAMPassRole",
            "Effect": "Allow",
            "Action": [
                "iam:PassRole"
            ],
            "Resource": "arn:aws:iam::009160053326:role/NeptuneETLRole"
        }
    ]
}
EOF

aws iam create-role \
    --role-name NeptuneETLRole \
    --assume-role-policy-document file://neptune-comprehensive-trust-policy.json \
    --region us-east-1


aws iam put-role-policy \
    --role-name NeptuneETLRole \
    --policy-name NeptuneETLComprehensivePolicy \
    --policy-document file://neptune-comprehensive-permissions.json \
    --region us-east-1