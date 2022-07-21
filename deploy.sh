#!/bin/bash -x

poetry export -f requirements.txt --output ingestor/requirements.txt --without-hashes

cd ingestor && poetry run chalice deploy
