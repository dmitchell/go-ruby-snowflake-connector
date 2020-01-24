#!/usr/bin/env bash
set -euo pipefail

pushd ext
go get || true
make build
popd
rake build
