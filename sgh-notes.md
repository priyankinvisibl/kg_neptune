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
  --endpoint-url="https://biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com" \
  --iam-role-arn="arn:aws:iam::009160053326:role/SSMCore" \
  --source="s3://genaixpubmed/enrichr-kg/enrichr/20250808072949/node_Gene.csv" \
  --format="csv" \
  --mode="AUTO" \
  --no-fail-on-error \
  --update-single-cardinality-properties 


connection:

curl -v --connect-timeout 10 "https://biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com:8182/status"


curl -X POST \
  https://biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com:8182/gremlin \
  -H 'Content-Type: application/json' \
  -d '{"gremlin": "g.addV(\"Person\").property(\"id\", \"123\").property(\"name\", \"John Doe\").property(\"age\", 30)"}'

aws neptunedata start-loader-job \
  --s3-bucket-region ap-southeast-1 \
  --endpoint-url https://biocuration-knowledge-graph-instance-1.cdayu8m0iuvf.ap-southeast-1.neptune.amazonaws.com:8182 \
  --iam-role-arn arn:aws:iam::009160053326:role/SSMCore \
  --source s3://genaixpubmed/enrichr-kg/enrichr/20250808072949/node_Gene.csv \
  --format csv \
  --mode AUTO \
  --no-fail-on-error \
  --update-single-cardinality-properties

aws neptune add-role-to-db-cluster \
    --db-cluster-identifier db-neptune-1 \
    --role-arn arn:aws:iam::009160053326:role/NeptuneETLRole \
    --region ap-southeast-1
