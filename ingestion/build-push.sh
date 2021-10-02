#!/bin/bash
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 597495568095.dkr.ecr.us-east-2.amazonaws.com
docker build -t igti-edc-m5-ingestion:1.0 .
docker tag igti-edc-m5-ingestion:1.0 597495568095.dkr.ecr.us-east-2.amazonaws.com/igti-edc-m5-ingestion:1.0
docker push 597495568095.dkr.ecr.us-east-2.amazonaws.com/igti-edc-m5-ingestion:1.0

