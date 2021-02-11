#!/usr/bin/env bash
set -euxo pipefail

export RUSTC_BOOTSTRAP=1
cargo build -p example-lock -Zunstable-options --out-dir ./out
docker build -f example-lock/Dockerfile -t kube-utils-example-lock ./out