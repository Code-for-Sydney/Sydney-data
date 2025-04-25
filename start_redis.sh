#!/bin/bash

# Container name
CONTAINER_NAME="redis-geocode-cache"

# Check if container already exists
if [ "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]; then
    echo "Container '$CONTAINER_NAME' already exists. Starting it..."
    docker start $CONTAINER_NAME
else
    echo "Creating and starting new Redis container '$CONTAINER_NAME'..."
    docker run -d \
        --name $CONTAINER_NAME \
        -p 6379:6379 \
        redis:7-alpine \
        redis-server --save 900 1 --loglevel warning
fi

echo "Redis is now running on localhost:6379"
