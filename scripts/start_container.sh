#!/usr/bin/env bash
set -x
IMAGE=YOUR_ECR_IMAGE_ID_HERE
CONTAINER_NAME=YOUR_CHOSEN_DOCKER_CONTAINER_NAME

docker rm -fv $CONTAINER_NAME || true && sleep 5
docker run --rm --name $CONTAINER_NAME -d --log-driver=journald --env-file ~/env.list -p 8080:8080 $IMAGE
until [ "$(docker inspect -f {{.State.Running}} $CONTAINER_NAME)" == "true" ]; do
    sleep 0.1;
done;
cid=$(docker ps -qf "name=$CONTAINER_NAME")
docker cp ~/secrets/. "$cid":/usr/local/airflow/secrets