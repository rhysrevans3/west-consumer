#!/bin/bash

IMAGE=west-consumer

aws ecs update-service \ 
    --force-new-deployment \
    --service $IMAGE-service \
    --cluster $IMAGE \
    --profile esgf2
