genaixpubmed/enrichr-kg/enrichr/20250808072949/node_Gene.csv
python3 test_neptune.py

test-connection:
curl https://biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com:8182/status

sample-gremlin-query:
curl -X POST \
  https://biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com:8182/gremlin \
  -H 'Content-Type: application/json' \
  -d '{"gremlin": "g.V().limit(1)"}'

s3-test
aws s3 ls s3://genaixpubmed/enrichr-kg/enrichr/20250808072949/node_Gene.csv --region ap-southeast-1


assume-role
aws sts assume-role --role-arn "arn:aws:iam::009160053326:role/SSMCore" --role-session-name "neptune-test"

cli:
aws neptunedata start-loader-job \
  --s3-bucket-region="ap-southeast-1" \
  --endpoint-url="https://biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com:8182" \
  --iam-role-arn="arn:aws:iam::009160053326:role/SSMCore" \
  --source="s3://genaixpubmed/enrichr-kg/enrichr/20250808072949/node_Gene.csv" \
  --format="csv" \
  --mode="AUTO" \
  --no-fail-on-error \
  --update-single-cardinality-properties 

  aws neptunedata start-loader-job   --s3-bucket-region="us-east-1"   --endpoint-url="https://db-neptune-1-instance-1.cigfmmqgs6p9.us-east-1.neptune.amazonaws.com:8182"   --iam-role-arn="arn:aws:iam::554248189203:role/sghTestETLRole"   --source="s3://neptune-test-data/new-genes/node_Gene.csv"   --format="csv"   --mode="AUTO"   --no-fail-on-error   --update-single-cardinality-properties 



connection:

curl -v --connect-timeout 10 "https://biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com:8182/status"


curl -X POST \
  https://biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com:8182/gremlin \
  -H 'Content-Type: application/json' \
  -d '{"gremlin": "g.addV(\"Person\").property(\"id\", \"123\").property(\"name\", \"John Doe\").property(\"age\", 30)"}'

aws neptunedata start-loader-job \
  --endpoint-url https://biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com:8182 \
  --iam-role-arn arn:aws:iam::009160053326:role/SSMCore \
  --source s3://genaixpubmed/enrichr-kg/enrichr/20250808072949/node_Gene.csv \
  --format csv \
  --mode AUTO \
  --no-fail-on-error \
  --update-single-cardinality-properties

aws neptunedata start-loader-job \
  --s3-bucket-region ap-southeast-1 \
  --endpoint-url https://biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com:8182 \
  --iam-role-arn arn:aws:iam::009160053326:role/SSMCore \
  --source s3://genaixpubmed/enrichr-kg/enrichr/20250808072949/node_Gene.csv \
  --format csv \
  --mode AUTO \
  --no-fail-on-error \
  --update-single-cardinality-properties \
  --debug \
  --cli-read-timeout 0 \
  --cli-connect-timeout 60

biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com

biocuration-knowledge-graph.cluster-cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com

biocuration-knowledge-graph.cluster-ro-cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com

biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com
aws neptune add-role-to-db-cluster \
    --db-cluster-identifier db-neptune-1 \
    --role-arn arn:aws:iam::009160053326:role/NeptuneETLRole \
    --region ap-southeast-1


aws neptune modify-db-cluster \
    --db-cluster-identifier db-neptune-1 \
    --vpc-security-group-ids sg-0816eacb656107d07 \
    --region YOUR_REGION

aws neptune modify-db-cluster \
    --db-cluster-identifier biocuration-knowledge-graph\
    --vpc-security-group-ids sg-08b21426ac748cf83 \
    --region ap-southeast-1



#####Security Groups commands############
aws ec2 describe-security-groups \
    --group-ids sg-0816eacb656107d07 \
    --query 'SecurityGroups[0].IpPermissions' \
    --output json > sg-backup.json
aws ec2 describe-security-groups \
    --group-ids sg-0816eacb656107d07 \
    --query 'SecurityGroups[0].IpPermissions' \
    --output json > rules-to-delete.json
aws ec2 revoke-security-group-ingress \
    --group-id sg-0816eacb656107d07 \
    --ip-permissions file://rules-to-delete.json

aws ec2 authorize-security-group-ingress \
    --group-id sg-0816eacb656107d07 \
    --ip-permissions file://sg-backup.json

aws ec2 authorize-security-group-ingress \
    --group-id sg-0816eacb656107d07 \
    --ip-permissions file://sg-backup.json

aws ec2 authorize-security-group-ingress \
    --group-id sg-0816eacb656107d07 \
    --protocol tcp \
    --port 22 \
    --cidr 10.11.0.0/25
    
aws ec2 authorize-security-group-ingress \
    --group-id sg-0816eacb656107d07 \
    --protocol -1 \
    --source-group sg-0816eacb656107d07

aws ec2 authorize-security-group-ingress \
    --group-id sg-0816eacb656107d07 \
    --protocol tcp \
    --port 2049 \
    --source-group sg-0816eacb656107d07

needed for neptune*****8
aws ec2 authorize-security-group-ingress \
    --group-id sg-0816eacb656107d07 \
    --protocol -1 \
    --cidr 10.2.0.0/23




I am auth