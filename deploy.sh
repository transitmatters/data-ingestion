#!/bin/bash -x

if [[ -z "$MBTA_V2_API_KEY" || -z "$DD_API_KEY"  || -z "$YANKEE_API_KEY"  ]]; then
    echo "Must provide MBTA_V2_API_KEY, YANKEE_API_KEY, and DD_API_KEY in environment" 1>&2
    exit 1
fi

STACK_NAME=ingestor
BUCKET=ingestor-lambda-deployments

# Identify the version and commit of the current deploy
GIT_VERSION=`git describe --tags --always`
GIT_SHA=`git rev-parse HEAD`
echo "Deploying version $GIT_VERSION | $GIT_SHA"

# Adding some datadog tags to get better data
DD_TAGS="git.commit.sha:$GIT_SHA,git.repository_url:github.com/transitmatters/data-ingestion"
DD_GIT_REPOSITORY_URL="github.com/transitmatters/data-ingestion"
DD_GIT_COMMIT_SHA="$GIT_SHA"

poetry export -f requirements.txt --output ingestor/requirements.txt --without-hashes

pushd ingestor/

poetry run chalice package --stage prod --merge-template .chalice/resources.json cfn/

# Shrink the deployment package for the lambda layer https://stackoverflow.com/a/69355796
echo "Shrinking the deployment package for the lambda layer"

zip -d -qq cfn/layer-deployment.zip '*/__pycache__/*'
zip -d -qq cfn/layer-deployment.zip '**/*.pyc'
zip -d -qq cfn/layer-deployment.zip '**/LICENSE*'
zip -d -qq cfn/layer-deployment.zip '**/AUTHOR*'
zip -d -qq cfn/layer-deployment.zip '**/NOTICE*'
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/numpy*/tests/**/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/pandas*/tests/**/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/pyarrow*/tests/**/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/examples/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/a*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/b*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/co*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/ch*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/cu*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/da*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/di*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/do*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/de*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/e*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/f*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/g*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/h*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/i*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/j*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/k*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/l*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/m*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/n*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/o*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/p*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/q*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/r*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/sa*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/se*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/ss*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/t*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/u*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/v*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/w*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/x*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/y*/*
zip -d -qq cfn/layer-deployment.zip python/lib/python3.11/site-packages/boto*/data/z*/*

Check package size before deploying
maximumsize=79100000
actualsize=$(wc -c <"cfn/layer-deployment.zip")
if [ $actualsize -ge $maximumsize ]; then
    echo ""
    echo "layer-deployment.zip is over $maximumsize bytes. Shrink the package further to be able to deploy"
    exit 1
fi

aws cloudformation package --template-file cfn/sam.json --s3-bucket $BUCKET --output-template-file cfn/packaged.yaml
aws cloudformation deploy --template-file cfn/packaged.yaml --stack-name $STACK_NAME \
    --capabilities CAPABILITY_NAMED_IAM --no-fail-on-empty-changeset \
    --parameter-overrides MbtaV2ApiKey=$MBTA_V2_API_KEY DDApiKey=$DD_API_KEY YankeeApiKey=$YANKEE_API_KEY GitVersion=$GIT_VERSION DDTags=$DD_TAGS
