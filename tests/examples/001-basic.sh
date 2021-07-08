#!/bin/bash

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

ROOT=$(unset CDPATH && cd $(dirname "${BASH_SOURCE[0]}")/../.. && pwd)
cd $ROOT

source "${ROOT}/hack/lib.sh"
source "${ROOT}/tests/examples/t.sh"

NS=$(basename ${0%.*})

function cleanup_if_succeess() {
	if [ $? -eq 0 ]; then
        kubectl -n $NS delete -f examples/basic/tidb-cluster.yaml
        kubectl delete ns $NS
    fi
}

trap cleanup_if_succeess EXIT

# 创建 namespace 001-basic
kubectl create ns $NS

# 等待 ns OK
hack::wait_for_success 10 3 "t::ns_is_active $NS"

# 部署 TiDBCluster
kubectl -n $NS apply -f examples/basic/tidb-cluster.yaml

# 等待 TiDBCluster OK
# kubectl wait --for=condition=Ready --timeout 10s tc/${name}
hack::wait_for_success 1800 30 "t::tc_is_ready $NS basic"
