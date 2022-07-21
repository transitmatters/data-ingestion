#!/bin/bash -x

poetry export -f requirements.txt --output ingestor/requirements.txt

cd ingestor && poetry run chalice deploy