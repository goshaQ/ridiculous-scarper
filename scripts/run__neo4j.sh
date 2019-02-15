#!/bin/bash

docker run \
    -p 7473:7473 \
    -p 7474:7474 \
    -p 7687:7687 \
    -v $(pwd)/neo4j/data:/data \
    -v $(pwd)/neo4j/logs:/logs \
    neo4j


