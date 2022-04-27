#!/usr/bin/env bash
set -x
IMAGE=YOUR_ECR_IMAGE_ID_HERE:master
aws ecr get-login-password | docker login --username AWS --password-stdin $IMAGE
docker pull $IMAGE