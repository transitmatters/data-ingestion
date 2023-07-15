#!/bin/bash -x

if [[ -z "$MBTA_V2_API_KEY" ]]; then
    echo "Must provide MBTA_V2_API_KEY in environment" 1>&2
    exit 1
fi

STACK_NAME=ingestor
BUCKET=ingestor-lambda-deployments

poetry export -f requirements.txt --output ingestor/requirements.txt --without-hashes

pushd ingestor/

poetry run chalice package --stage prod --merge-template .chalice/envvars.json --merge-template .chalice/dynamo_tables.json cfn/
aws cloudformation package --template-file cfn/sam.json --s3-bucket $BUCKET --output-template-file cfn/packaged.yaml
aws cloudformation deploy --template-file cfn/packaged.yaml --stack-name $STACK_NAME \
    --capabilities CAPABILITY_NAMED_IAM --no-fail-on-empty-changeset \
    --parameter-overrides MbtaV2ApiKey=$MBTA_V2_API_KEY DDApiKey=$DD_API_KEY
