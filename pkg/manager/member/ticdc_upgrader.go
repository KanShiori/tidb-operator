// Copyright 2018 PingCAP, Inc.
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

package member

import (
	"fmt"

	"github.com/pingcap/advanced-statefulset/client/apis/apps/v1/helper"
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/controller"
	apps "k8s.io/api/apps/v1"
	"k8s.io/klog"
)

// ticdcUpgrader 实现业务层 TiCDC 的升级
type ticdcUpgrader struct {
	deps *controller.Dependencies
}

// NewTiCDCUpgrader returns a ticdc Upgrader
func NewTiCDCUpgrader(deps *controller.Dependencies) Upgrader {
	return &ticdcUpgrader{
		deps: deps,
	}
}

func (u *ticdcUpgrader) Upgrade(tc *v1alpha1.TidbCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	// TiCDC 副本为 0，还要啥升级
	// return nil when scale replicas to 0
	if tc.Spec.TiCDC.Replicas == int32(0) {
		return nil
	}

	ns := tc.GetNamespace()
	tcName := tc.GetName()

	// WHY? 为什么先设置 Phase，然后检查前后版本？
	tc.Status.TiCDC.Phase = v1alpha1.UpgradePhase

	if !templateEqual(newSet, oldSet) {
		return nil
	}

	if tc.Status.TiCDC.StatefulSet.UpdateRevision == tc.Status.TiCDC.StatefulSet.CurrentRevision {
		return nil
	}

	if oldSet.Spec.UpdateStrategy.Type == apps.OnDeleteStatefulSetStrategyType || oldSet.Spec.UpdateStrategy.RollingUpdate == nil {
		// Manually bypass tidb-operator to modify statefulset directly, such as modify ticdc statefulset's RollingUpdate strategy to OnDelete strategy,
		// or set RollingUpdate to nil, skip tidb-operator's rolling update logic in order to speed up the upgrade in the test environment occasionally.
		// If we encounter this situation, we will let the native statefulset controller do the upgrade completely, which may be unsafe for upgrading tidb.
		// Therefore, in the production environment, we should try to avoid modifying the tidb statefulset update strategy directly.
		newSet.Spec.UpdateStrategy = oldSet.Spec.UpdateStrategy
		klog.Warningf("tidbcluster: [%s/%s] ticdc statefulset %s UpdateStrategy has been modified manually", ns, tcName, oldSet.GetName())
		return nil
	}

	// 设置 StatefulSet Partition，默认不继续 Pod 升级
	// 当下面执行完升级后会减小 Partition
	setUpgradePartition(newSet, *oldSet.Spec.UpdateStrategy.RollingUpdate.Partition)

	// List 所有的 Pod，从大到小判断版本是否一致，是否需要进行升级
	// TiCDC 升级过程不需要进行业务上的操作，仅仅是检查已升级的 TiCDC 是否还健康
	podOrdinals := helper.GetPodOrdinals(*oldSet.Spec.Replicas, oldSet).List()
	for _i := len(podOrdinals) - 1; _i >= 0; _i-- {
		i := podOrdinals[_i]
		podName := ticdcPodName(tcName, i)
		pod, err := u.deps.PodLister.Pods(ns).Get(podName)
		if err != nil {
			return fmt.Errorf("ticdcUpgrader.Upgrade: failed to get pod %s for cluster %s/%s, error: %s", podName, ns, tcName, err)
		}
		revision, exist := pod.Labels[apps.ControllerRevisionHashLabelKey]
		if !exist {
			return controller.RequeueErrorf("tidbcluster: [%s/%s]'s ticdc pod: [%s] has no label: %s", ns, tcName, podName, apps.ControllerRevisionHashLabelKey)
		}

		if revision == tc.Status.TiCDC.StatefulSet.UpdateRevision {
			// 已升级的 TiCDC 异常，中断升级流程
			if _, exist := tc.Status.TiCDC.Captures[podName]; !exist {
				return controller.RequeueErrorf("tidbcluster: [%s/%s]'s ticdc upgraded pod: [%s] is not ready", ns, tcName, podName)
			}
			continue
		}

		// 减小 Partition
		setUpgradePartition(newSet, i)
		return nil
	}

	return nil
}
