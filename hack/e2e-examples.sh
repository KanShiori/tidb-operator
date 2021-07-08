#!/usr/bin/env bash

# Copyright 2020 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

#
# E2E entrypoint script for examples.
#

ROOT=$(unset CDPATH && cd $(dirname "${BASH_SOURCE[0]}")/.. && pwd)
cd $ROOT

source "${ROOT}/hack/lib.sh"

hack::ensure_kind

# 使用 kind 创建 cluster
# 默认配置
echo "info: create a Kubernetes cluster"
$KIND_BIN create cluster

# 本地启动 TiDBOperator
# 镜像来自于本地编译好的
echo "info: start tidb-operator"
hack/local-up-operator.sh

# 执行 tests/examples/ 中的测试脚本(00x-xxx.sh)
echo "info: testing examples"
export PATH=$PATH:$OUTPUT_BIN
hack::ensure_kubectl

cnt=0
for t in $(find tests/examples/ -regextype sed -regex '.*/[0-9]\{3\}-.*\.sh'); do
    echo "info: testing $t"
    $t
    if [ $? -eq 0 ]; then
        echo "info: test $t passed"
    else
        echo "error: test $t failed"
        ((cnt++))
    fi
done
if [ $cnt -gt 0 ]; then
    echo "fatal: $cnt tests failed"
    exit 1
fi
