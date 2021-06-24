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

// pdUpgrader 负责 PD 业务层的升级
type pdUpgrader struct {
	deps *controller.Dependencies
}

// NewPDUpgrader returns a pdUpgrader
func NewPDUpgrader(deps *controller.Dependencies) Upgrader {
	return &pdUpgrader{
		deps: deps,
	}
}

func (u *pdUpgrader) Upgrade(tc *v1alpha1.TidbCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	return u.gracefulUpgrade(tc, oldSet, newSet)
}

func (u *pdUpgrader) gracefulUpgrade(tc *v1alpha1.TidbCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	ns := tc.GetNamespace()
	tcName := tc.GetName()
	if !tc.Status.PD.Synced {
		return fmt.Errorf("tidbcluster: [%s/%s]'s pd status sync failed, can not to be upgraded", ns, tcName)
	}
	// 如果 TiCDC TiFlash 正在升级，或者 PD 正在 Scale，那么不进行升级操作
	// 又将 StatefulSet.Spec 改为旧的（这样当次不会触发 StatefulSet 的升级流程）
	if tc.Status.TiCDC.Phase == v1alpha1.UpgradePhase ||
		tc.Status.TiFlash.Phase == v1alpha1.UpgradePhase ||
		tc.PDScaling() {
		klog.Infof("TidbCluster: [%s/%s]'s ticdc status is %v, "+
			"tiflash status is %v, pd status is %v, can not upgrade pd",
			ns, tcName, tc.Status.TiCDC.Phase,
			tc.Status.TiFlash.Phase, tc.Status.PD.Phase)
		_, podSpec, err := GetLastAppliedConfig(oldSet)
		if err != nil {
			return err
		}
		newSet.Spec.Template.Spec = *podSpec
		return nil
	}

	// 进行升级
	tc.Status.PD.Phase = v1alpha1.UpgradePhase

	// WHY?: 这里再次比较是为啥？？
	if !templateEqual(newSet, oldSet) {
		return nil
	}

	// StatefulSet 已经升级成功，直接退出
	if tc.Status.PD.StatefulSet.UpdateRevision == tc.Status.PD.StatefulSet.CurrentRevision {
		return nil
	}

	// StatefulSet 升级策略为 OnDelete，那么由用户手动进行升级操作，Operator 不负责
	if oldSet.Spec.UpdateStrategy.Type == apps.OnDeleteStatefulSetStrategyType || oldSet.Spec.UpdateStrategy.RollingUpdate == nil {
		// Manually bypass tidb-operator to modify statefulset directly, such as modify pd statefulset's RollingUpdate straregy to OnDelete strategy,
		// or set RollingUpdate to nil, skip tidb-operator's rolling update logic in order to speed up the upgrade in the test environment occasionally.
		// If we encounter this situation, we will let the native statefulset controller do the upgrade completely, which may be unsafe for upgrading pd.
		// Therefore, in the production environment, we should try to avoid modifying the pd statefulset update strategy directly.
		newSet.Spec.UpdateStrategy = oldSet.Spec.UpdateStrategy
		klog.Warningf("tidbcluster: [%s/%s] pd statefulset %s UpdateStrategy has been modified manually", ns, tcName, oldSet.GetName())
		return nil
	}

	// 设置 StatefulSet Partition，默认不继续 Pod 升级
	// 当下面 upgradePDPod 成功后会减小 Partition
	setUpgradePartition(newSet, *oldSet.Spec.UpdateStrategy.RollingUpdate.Partition)

	// List 所有的 Pod，从大到小判断版本是否一致，是否需要进行升级
	podOrdinals := helper.GetPodOrdinals(*oldSet.Spec.Replicas, oldSet).List()
	for _i := len(podOrdinals) - 1; _i >= 0; _i-- {
		i := podOrdinals[_i]
		podName := PdPodName(tcName, i)
		pod, err := u.deps.PodLister.Pods(ns).Get(podName)
		if err != nil {
			return fmt.Errorf("gracefulUpgrade: failed to get pods %s for cluster %s/%s, error: %s", podName, ns, tcName, err)
		}

		// 得到 Pod 当前版本 Revision
		revision, exist := pod.Labels[apps.ControllerRevisionHashLabelKey]
		if !exist {
			return controller.RequeueErrorf("tidbcluster: [%s/%s]'s pd pod: [%s] has no label: %s", ns, tcName, podName, apps.ControllerRevisionHashLabelKey)
		}

		// 当前版本与需要升级 Revision 相同，说明 Pod 已经升级
		if revision == tc.Status.PD.StatefulSet.UpdateRevision {
			// 检查 PD 是否存在并且是否 health，如果已经升级的 Pod 异常，那么不继续进行升级
			if member, exist := tc.Status.PD.Members[PdName(tc.Name, i, tc.Namespace, tc.Spec.ClusterDomain)]; !exist || !member.Health {
				return controller.RequeueErrorf("tidbcluster: [%s/%s]'s pd upgraded pod: [%s] is not ready", ns, tcName, podName)
			}

			// Pod i 升级成功检查下一个
			continue
		}

		// 走到这里，说明 Pod i 还未升级

		// WHAT? Webhook 为啥不需要升级了
		if u.deps.CLIConfig.PodWebhookEnabled {
			setUpgradePartition(newSet, i)
			return nil
		}

		// 处理 Pod i 的升级
		// 这里直接 return，说明每次处理一个 Pod 的升级
		return u.upgradePDPod(tc, i, newSet)
	}

	return nil
}

// upgradePDPod 进行某个 Pod 的业务上的升级
func (u *pdUpgrader) upgradePDPod(tc *v1alpha1.TidbCluster, ordinal int32, newSet *apps.StatefulSet) error {
	ns := tc.GetNamespace()
	tcName := tc.GetName()
	upgradePdName := PdName(tcName, ordinal, tc.Namespace, tc.Spec.ClusterDomain)
	upgradePodName := PdPodName(tcName, ordinal)

	// 当前 Pod 的 PD 是 leader
	if tc.Status.PD.Leader.Name == upgradePdName || tc.Status.PD.Leader.Name == upgradePodName {
		var targetName string
		if tc.PDStsActualReplicas() > 1 {
			// 多副本情况，进行 Leader 转移

			// 得到编号最大的 Pod，如果自己就是编号最大的，那么先给编号最小的 Pod
			// 因此被转移的 Leader 可能是：编号最大的新版本 Pod OR 编号最小的旧版本 Pod
			targetOrdinal := helper.GetMaxPodOrdinal(*newSet.Spec.Replicas, newSet)
			if ordinal == targetOrdinal {
				targetOrdinal = helper.GetMinPodOrdinal(*newSet.Spec.Replicas, newSet)
			}
			targetName = PdName(tcName, targetOrdinal, tc.Namespace, tc.Spec.ClusterDomain)
			if _, exist := tc.Status.PD.Members[targetName]; !exist {
				targetName = PdPodName(tcName, targetOrdinal)
			}
		} else {
			// 单副本情况，寻找异构的集群的 PD
			for _, member := range tc.Status.PD.PeerMembers {
				if member.Name != upgradePdName && member.Health {
					targetName = member.Name
					break
				}
			}
		}

		// targetName 大于 0 表明找到了需要转移 Leader 的 Pod，进行 transfer
		if len(targetName) > 0 {
			err := u.transferPDLeaderTo(tc, targetName)
			if err != nil {
				klog.Errorf("pd upgrader: failed to transfer pd leader to: %s, %v", targetName, err)
				return err
			}
			klog.Infof("pd upgrader: transfer pd leader to: %s successfully", targetName)
			return controller.RequeueErrorf("tidbcluster: [%s/%s]'s pd member: [%s] is transferring leader to pd member: [%s]", ns, tcName, upgradePdName, targetName)
		}
	}

	// 不是 leader OR 单副本无法转移，直接缩小 Partition
	setUpgradePartition(newSet, ordinal)
	return nil
}

// transferPDLeaderTo 调用 PD 接口进行 Transfer
func (u *pdUpgrader) transferPDLeaderTo(tc *v1alpha1.TidbCluster, targetName string) error {
	return controller.GetPDClient(u.deps.PDControl, tc).TransferPDLeader(targetName)
}

type fakePDUpgrader struct{}

// NewFakePDUpgrader returns a fakePDUpgrader
func NewFakePDUpgrader() Upgrader {
	return &fakePDUpgrader{}
}

func (u *fakePDUpgrader) Upgrade(tc *v1alpha1.TidbCluster, _ *apps.StatefulSet, _ *apps.StatefulSet) error {
	if !tc.Status.PD.Synced {
		return fmt.Errorf("tidbcluster: pd status sync failed, can not to be upgraded")
	}
	tc.Status.PD.Phase = v1alpha1.UpgradePhase
	return nil
}
