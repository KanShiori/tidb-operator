// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tidbcluster

import (
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	utiltidbcluster "github.com/pingcap/tidb-operator/pkg/util/tidbcluster"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
)

// TidbClusterConditionUpdater interface that translates cluster state into
// into tidb cluster status conditions.
type TidbClusterConditionUpdater interface {
	Update(*v1alpha1.TidbCluster) error
}

type tidbClusterConditionUpdater struct {
}

var _ TidbClusterConditionUpdater = &tidbClusterConditionUpdater{}

func (u *tidbClusterConditionUpdater) Update(tc *v1alpha1.TidbCluster) error {
	u.updateReadyCondition(tc)
	// in the future, we may return error when we need to Kubernetes API, etc.
	return nil
}

func allStatefulSetsAreUpToDate(tc *v1alpha1.TidbCluster) bool {
	isUpToDate := func(status *appsv1.StatefulSetStatus, requireExist bool) bool {
		if status == nil {
			return !requireExist
		}
		return status.CurrentRevision == status.UpdateRevision
	}
	return (isUpToDate(tc.Status.PD.StatefulSet, false)) &&
		(isUpToDate(tc.Status.TiKV.StatefulSet, false)) &&
		(isUpToDate(tc.Status.TiDB.StatefulSet, false)) &&
		isUpToDate(tc.Status.TiFlash.StatefulSet, false)
}

func (u *tidbClusterConditionUpdater) updateReadyCondition(tc *v1alpha1.TidbCluster) {
	status := v1.ConditionFalse
	reason := ""
	message := ""

	// 依次检查各个组件是否正常
	//  1. PD TiKV TiDB TiFlash 的 StatefulSet 是否正在运行（不是升级流程）
	//  2. PD 所有 member 正常运行（都是 health 的）
	//  3. TiKV 所有 Store 是 Up 状态
	//  4. TiDB 所有 member 是 health 的
	//  5. Tiflash 所有 Store 都是 Ready 的
	// 其中一个不正常，那么 Ready 就是 False
	switch {
	case !allStatefulSetsAreUpToDate(tc):
		reason = utiltidbcluster.StatfulSetNotUpToDate
		message = "Statefulset(s) are in progress"
	case tc.Spec.PD != nil && !tc.PDAllMembersReady():
		reason = utiltidbcluster.PDUnhealthy
		message = "PD(s) are not healthy"
	case tc.Spec.TiKV != nil && !tc.TiKVAllStoresReady():
		reason = utiltidbcluster.TiKVStoreNotUp
		message = "TiKV store(s) are not up"
	case tc.Spec.TiDB != nil && !tc.TiDBAllMembersReady():
		reason = utiltidbcluster.TiDBUnhealthy
		message = "TiDB(s) are not healthy"
	case tc.Spec.TiFlash != nil && !tc.TiFlashAllStoresReady():
		reason = utiltidbcluster.TiFlashStoreNotUp
		message = "TiFlash store(s) are not up"
	default:
		status = v1.ConditionTrue
		reason = utiltidbcluster.Ready
		message = "TiDB cluster is fully up and running"
	}
	cond := utiltidbcluster.NewTidbClusterCondition(v1alpha1.TidbClusterReady, status, reason, message)
	utiltidbcluster.SetTidbClusterCondition(&tc.Status, *cond)
}
