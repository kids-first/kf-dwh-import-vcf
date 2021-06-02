#!/bin/bash
set -x
mkdir -p ~/.ivy2 ~/.sbt ~/.m2 ~/.sbt_cache
docker run --rm -v $(pwd):/app/build \
    --user $(id -u):$(id -g) \
    -v ~/.m2:/app/.m2 \
    -v ~/.ivy2:/app/.ivy2 \
    -v ~/.sbt:/app/.sbt \
    -v ~/.sbt_cache:/app/.cache \
    -w /app/build hseeberger/scala-sbt:11.0.10_1.5.2_2.12.13 \
    sbt -Duser.home=/app clean test