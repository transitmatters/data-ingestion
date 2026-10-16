#!/bin/bash

# Shrink the deployment package for the lambda layer https://stackoverflow.com/a/69355796
function shrink {
    zip -d -qq cfn/layer-deployment.zip '*/__pycache__/*' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.pyc' || true
    zip -d -qq cfn/layer-deployment.zip '**/LICENSE*' || true
    zip -d -qq cfn/layer-deployment.zip '**/AUTHOR*' || true
    zip -d -qq cfn/layer-deployment.zip '**/NOTICE*' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.md' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.c' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.cpp' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.h' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.pyx' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.pxd' || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy*/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pandas*/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pandas/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pandas/_libs/tslibs/src/datetime/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pandas/io/formats/templates/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/examples/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/sqlalchemy*/testing/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/sqlalchemy/dialects/oracle/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/sqlalchemy/dialects/mssql/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/sqlalchemy/dialects/mysql/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/sqlalchemy/dialects/postgresql/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Africa/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Asia/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Atlantic/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Arctic/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Antarctica/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Australia/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Brazil/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Chile/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Europe/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Indian/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Pacific/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Mexico/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Singapore || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Turkey || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Poland || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Egypt || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Hongkong || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/I* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/J* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/a*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/b*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/co*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/ch*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/cu*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/da*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/di*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/do*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/de*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/e*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/f*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/g*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/h*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/i*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/j*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/k*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/l*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/m*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/n*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/o*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/p*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/q*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/r*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/sa*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/ss*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/t*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/u*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/v*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/w*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/x*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/y*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/z*/* || true
}

# Check if the layer deployment package is under the maximum size
function check_package_size {
    local zipfile="${1:-cfn/layer-deployment.zip}"
    local maximumsize=79100000
    
    if [ ! -f "$zipfile" ]; then
        echo "Error: $zipfile not found"
        exit 1
    fi
    
    actualsize=$(wc -c <"$zipfile")
    difference=$(expr $actualsize - $maximumsize)
    
    echo "$zipfile is $actualsize bytes"
    
    if [ $actualsize -ge $maximumsize ]; then
        echo ""
        echo "$zipfile is over $maximumsize bytes. Shrink the package by $difference bytes to be able to deploy"
        exit 1
    fi
    
    echo "$zipfile is under the maximum size of $maximumsize bytes, by $difference bytes"
}

# Check the unzipped size, which is what Lambda actually enforces: 262144000 bytes for the function
# code plus all its layers combined. That's our layer, the app's deployment.zip and Datadog-Extension.
function check_unzipped_size {
    local zipfile="${1:-cfn/layer-deployment.zip}"
    local appzip="${2:-cfn/deployment.zip}"
    local maximumsize=262144000
    # Datadog-Extension:101 (ingestor/.chalice/config.json); update when bumping the extension
    local extensionsize=11683061

    layersize=$(unzip -l "$zipfile" | tail -1 | awk '{print $1}')
    appsize=$(unzip -l "$appzip" | tail -1 | awk '{print $1}')
    totalsize=$(($layersize + $appsize + $extensionsize))
    difference=$(($maximumsize - $totalsize))

    echo "$zipfile unzips to $layersize bytes, $appzip to $appsize, Datadog-Extension to $extensionsize"

    if [ $totalsize -ge $maximumsize ]; then
        echo ""
        echo "Function plus layers unzip to $totalsize bytes, over $maximumsize. Shrink the package by $((-$difference)) bytes to be able to deploy"
        exit 1
    fi

    echo "Function plus layers unzip to $totalsize bytes, under the maximum size of $maximumsize bytes by $difference bytes"
}

# Import the shrunk layer and the app inside the Lambda Python runtime image, so a too-aggressive
# shrink fails here instead of in production
function smoke_layer {
    local zipfile="${1:-cfn/layer-deployment.zip}"
    local appzip="${2:-cfn/deployment.zip}"
    local helpers_dir
    helpers_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local layer_dir
    layer_dir="$(mktemp -d)"
    local app_dir
    app_dir="$(mktemp -d)"

    unzip -q "$zipfile" -d "$layer_dir"
    unzip -q "$appzip" -d "$app_dir"
    # Run in the image matching the Python version the layer was built for (python/lib/pythonX.Y)
    local python_version
    python_version="$(basename "$(ls -d "$layer_dir"/python/lib/python3.*)" | sed 's/^python//')"
    docker run --rm --platform linux/amd64 \
        -v "$layer_dir:/opt:ro" \
        -v "$app_dir:/var/task:ro" \
        -v "$helpers_dir/smoke_layer.py:/smoke_layer.py:ro" \
        -e PYTHONPATH="/var/task:/opt/python/lib/python$python_version/site-packages" \
        -e AWS_DEFAULT_REGION=us-east-1 \
        -e DD_TRACE_ENABLED=false \
        --entrypoint python3 \
        "public.ecr.aws/lambda/python:$python_version" /smoke_layer.py
    local status=$?
    rm -rf "$layer_dir" "$app_dir"
    return $status
}
