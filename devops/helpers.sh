#!/bin/bash

# Shrink the deployment package for the lambda layer https://stackoverflow.com/a/69355796
function shrink {
    zip -d -qq cfn/layer-deployment.zip '*/__pycache__/*'
    zip -d -qq cfn/layer-deployment.zip '**/*.pyc'
    zip -d -qq cfn/layer-deployment.zip '**/LICENSE*'
    zip -d -qq cfn/layer-deployment.zip '**/AUTHOR*'
    zip -d -qq cfn/layer-deployment.zip '**/NOTICE*'
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy*/tests/**/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pandas*/tests/**/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pyarrow*/tests/**/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/examples/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/a*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/b*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/co*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/ch*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/cu*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/da*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/di*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/do*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/de*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/e*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/f*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/g*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/h*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/i*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/j*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/k*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/l*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/m*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/n*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/o*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/p*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/q*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/r*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/sa*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/se*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/ss*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/t*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/u*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/v*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/w*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/x*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/y*/*
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/z*/*
}
