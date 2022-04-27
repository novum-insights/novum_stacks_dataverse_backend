#!/usr/bin/env bash
set -x
CONTAINER_NAME=YOUR_CHOSEN_DOCKER_CONTAINER_NAME

docker ps -q --filter "name=$CONTAINER_NAME" | grep -q .
if [ $? -eq 0 ]
then
  docker stop $CONTAINER_NAME || true
  exit 0
else
  echo "Container $CONTAINER_NAME is not running."
  exit 0
fi

