#!/usr/bin/env bash
set -e
IMAGE="YOUR_ECR_IMAGE_ID_HERE"
TAG=$IMAGE:master
AWS="aws --profile <profile> --region eu-west-1"

if [ -n "$1" ]
then
  TAG+=":$1"
fi
docker build --no-cache -t $TAG .
eval $(${AWS} ecr get-login --no-include-email | sed 's;https://;;g')
docker push $TAG