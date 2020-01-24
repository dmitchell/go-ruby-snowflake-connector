#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME=gorubyconnectorbuilder 
docker build -t $IMAGE_NAME .
docker run --rm -it -v ${PWD}:/src $IMAGE_NAME ./build_scripts/build_pkg.bash
