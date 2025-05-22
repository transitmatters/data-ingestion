#!/bin/bash -x

if [[ -z "$MBTA_V3_API_KEY" || -z "$DD_API_KEY"  || -z "$YANKEE_API_KEY"  ]]; then
    echo "Must provide MBTA_V3_API_KEY, YANKEE_API_KEY, and DD_API_KEY in environment" 1>&2
    exit 1
fi

STACK_NAME=ingestor
BUCKET=ingestor-lambda-deployments

# Identify the version and commit of the current deploy
GIT_VERSION=`git describe --tags --always`
GIT_SHA=`git rev-parse HEAD`
echo "Deploying version $GIT_VERSION | $GIT_SHA"

# Adding some datadog tags to get better data
DD_TAGS="git.commit.sha:$GIT_SHA,git.repository_url:github.com/transitmatters/data-ingestion,version:$GIT_VERSION"
DD_GIT_REPOSITORY_URL="github.com/transitmatters/data-ingestion"
DD_GIT_COMMIT_SHA="$GIT_SHA"

poetry export -f requirements.txt --output ingestor/requirements.txt --without-hashes

pushd ingestor/

poetry run chalice package --stage prod --merge-template .chalice/resources.json cfn/

# Shrink the deployment package for the lambda layer https://stackoverflow.com/a/69355796
actualsize=$(wc -c <"cfn/layer-deployment.zip")
echo "Shrinking the deployment package for the lambda layer. It's currently $actualsize bytes."

source ../devops/helpers.sh
shrink > /dev/null 2>&1

# Check the size of the new package
newsize=$(wc -c <"cfn/layer-deployment.zip")
echo "The deployment package is now $newsize bytes"
diffsize=$(($actualsize - $newsize))
echo "Difference: $diffsize bytes"

# Check package size before deploying
maximumsize=79100000
actualsize=$(wc -c <"cfn/layer-deployment.zip")
difference=$(expr $actualsize - $maximumsize)
if [ $actualsize -ge $maximumsize ]; then
    echo ""
    echo "layer-deployment.zip is over $maximumsize bytes. Shrink the package by $difference bytes to be able to deploy"
    exit 1
fi
echo "layer-deployment.zip is under the maximum size of $maximumsize bytes, by $difference bytes"

aws cloudformation package --template-file cfn/sam.json --s3-bucket $BUCKET --output-template-file cfn/packaged.yaml
aws cloudformation deploy --template-file cfn/packaged.yaml --stack-name $STACK_NAME \
    --capabilities CAPABILITY_NAMED_IAM --no-fail-on-empty-changeset \
    --tags service=ingestor env=prod \
    --parameter-overrides MbtaV2ApiKey=$MBTA_V2_API_KEY DDApiKey=$DD_API_KEY YankeeApiKey=$YANKEE_API_KEY GitVersion=$GIT_VERSION DDTags=$DD_TAGS
