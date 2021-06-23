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
	"github.com/pingcap/tidb-operator/pkg/util"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
)

// TODO add e2e test specs

// pdScaler 负责 PD 的缩扩容的业务层逻辑
type pdScaler struct {
	generalScaler
}

// NewPDScaler returns a Scaler
func NewPDScaler(deps *controller.Dependencies) Scaler {
	return &pdScaler{generalScaler: generalScaler{deps: deps}}
}

// Scale 缩扩容操作入口
func (s *pdScaler) Scale(meta metav1.Object, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	scaling, _, _, _ := scaleOne(oldSet, newSet)
	if scaling > 0 {
		return s.ScaleOut(meta, oldSet, newSet)
	} else if scaling < 0 {
		return s.ScaleIn(meta, oldSet, newSet)
	}
	return s.SyncAutoScalerAnn(meta, oldSet)
}

// ScaleOut 进行扩容操作
//
// 仅仅修改了 newSet 的 Replica 数量，后续继续的 Sync 操作会进行 StatefulSet 的更新
func (s *pdScaler) ScaleOut(meta metav1.Object, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	tc, ok := meta.(*v1alpha1.TidbCluster)
	if !ok {
		return nil
	}

	// 得到当前需要扩的 Pod 编号，以及扩容后的 Replica
	_, ordinal, replicas, deleteSlots := scaleOne(oldSet, newSet)
	resetReplicas(newSet, oldSet)
	ns := tc.GetNamespace()
	tcName := tc.GetName()

	klog.Infof("scaling out pd statefulset %s/%s, ordinal: %d (replicas: %d, delete slots: %v)", oldSet.Namespace, oldSet.Name, ordinal, replicas, deleteSlots.List())
	// 删除新 Pod 旧的 PVC（可能之前缩容过来的），让 StatefulSet 自动创建新的
	_, err := s.deleteDeferDeletingPVC(tc, v1alpha1.PDMemberType, ordinal)
	if err != nil {
		return err
	}

	if !tc.Status.PD.Synced {
		return fmt.Errorf("TidbCluster: %s/%s's pd status sync failed, can't scale out now", ns, tcName)
	}

	// 设置 StatefulSet 为新的副本数量
	setReplicasAndDeleteSlots(newSet, replicas, deleteSlots)
	return nil
}

// ScaleIn 缩容操作
//
// We need remove member from cluster before reducing statefulset replicas
// only remove one member at a time when scale down
func (s *pdScaler) ScaleIn(meta metav1.Object, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	tc, ok := meta.(*v1alpha1.TidbCluster)
	if !ok {
		return nil
	}

	// 得到当前要缩容的 PD Pod
	ns := tc.GetNamespace()
	tcName := tc.GetName()
	_, ordinal, replicas, deleteSlots := scaleOne(oldSet, newSet)
	resetReplicas(newSet, oldSet)
	memberName := PdName(tcName, ordinal, tc.Namespace, tc.Spec.ClusterDomain)
	pdPodName := PdPodName(tcName, ordinal)

	if !tc.Status.PD.Synced {
		return fmt.Errorf("TidbCluster: %s/%s's pd status sync failed, can't scale in now", ns, tcName)
	}

	klog.Infof("scaling in pd statefulset %s/%s, ordinal: %d (replicas: %d, delete slots: %v)", oldSet.Namespace, oldSet.Name, ordinal, replicas, deleteSlots.List())

	// WHY? WebHook 下不需要进行实际的缩容？
	if s.deps.CLIConfig.PodWebhookEnabled {
		setReplicasAndDeleteSlots(newSet, replicas, deleteSlots)
		return nil
	}

	// 检查是否允许缩容
	// Pod 缩容为 0，但是其他组件还存在，那么就不允许缩容
	// limit scale in when multi-cluster is enabled
	if pass := s.preCheckUpMembers(tc, pdPodName); !pass {
		return nil
	}

	pdClient := controller.GetPDClient(s.deps.PDControl, tc)
	leader, err := pdClient.GetPDLeader()
	if err != nil {
		return err
	}
	// If the PD pod was PD leader during scale-in, we would transfer PD leader first
	// If the PD StatefulSet would be scale-in to zero and no other members in the PD cluster,
	// we would directly delete the member without the leader transferring
	if leader.Name == memberName || leader.Name == pdPodName {
		// 如果当前要缩容的是 PD Leader，进行 transfer
		if *newSet.Spec.Replicas > 1 {
			minOrdinal := helper.GetMinPodOrdinal(*newSet.Spec.Replicas, newSet)
			targetOrdinal := helper.GetMaxPodOrdinal(*newSet.Spec.Replicas, newSet)
			if ordinal > minOrdinal {
				targetOrdinal = minOrdinal
			}
			targetPdName := PdName(tcName, targetOrdinal, tc.Namespace, tc.Spec.ClusterDomain)
			if _, exist := tc.Status.PD.Members[targetPdName]; exist {
				err = pdClient.TransferPDLeader(targetPdName)
			} else {
				err = pdClient.TransferPDLeader(PdPodName(tcName, targetOrdinal))
			}
			if err != nil {
				return err
			}
		} else {
			// 缩容到 0，将其 transfer 到异构集群的 PD
			for _, member := range tc.Status.PD.PeerMembers {
				if member.Health && member.Name != memberName {
					err = pdClient.TransferPDLeader(member.Name)
					if err != nil {
						return err
					}
					return controller.RequeueErrorf("tc[%s/%s]'s pd pod[%s/%s] is transferring pd leader,can't scale-in now", ns, tcName, ns, memberName)
				}
			}
		}
	}

	// PD 侧删除
	err = pdClient.DeleteMember(memberName)
	if err != nil {
		klog.Errorf("pdScaler.ScaleIn: failed to delete member %s, %v", memberName, err)
		return err
	}
	klog.Infof("pdScaler.ScaleIn: delete member %s successfully", memberName)

	pod, err := s.deps.PodLister.Pods(ns).Get(pdPodName)
	if err != nil {
		return fmt.Errorf("pdScaler.ScaleIn: failed to get pod %s/%s for pd in tc %s/%s, error: %s", ns, pdPodName, ns, tcName, err)
	}

	pvcs, err := util.ResolvePVCFromPod(pod, s.deps.PVCLister)
	if err != nil {
		return fmt.Errorf("pdScaler.ScaleIn: failed to get pvcs for pod %s/%s in tc %s/%s, error: %s", ns, pod.Name, ns, tcName, err)
	}

	// 将 Pod 包含的 PVC 标记为删除
	for _, pvc := range pvcs {
		if err := addDeferDeletingAnnoToPVC(tc, pvc, s.deps.PVCControl); err != nil {
			return err
		}
	}

	// 设置 StatefulSet 的 ReplicaSet 进行缩容
	setReplicasAndDeleteSlots(newSet, replicas, deleteSlots)
	return nil
}

func (s *pdScaler) SyncAutoScalerAnn(meta metav1.Object, actual *apps.StatefulSet) error {
	return nil
}

func (s *pdScaler) preCheckUpMembers(tc *v1alpha1.TidbCluster, podName string) bool {
	upComponents := 0

	upComponents += len(tc.Status.TiKV.Stores) + len(tc.Status.TiFlash.Stores) + len(tc.Status.TiDB.Members)

	if tc.Status.TiCDC.StatefulSet != nil {
		upComponents += int(tc.Status.TiCDC.StatefulSet.Replicas)
	}

	if tc.Status.Pump.StatefulSet != nil {
		upComponents += int(tc.Status.Pump.StatefulSet.Replicas)
	}

	if upComponents != 0 && tc.Spec.PD.Replicas == 0 {
		errMsg := fmt.Sprintf("The PD is in use by TidbCluster [%s/%s], can't scale in PD, podname %s", tc.GetNamespace(), tc.GetName(), podName)
		klog.Error(errMsg)
		s.deps.Recorder.Event(tc, v1.EventTypeWarning, "FailedScaleIn", errMsg)
		return false
	}

	return true
}

type fakePDScaler struct{}

// NewFakePDScaler returns a fake Scaler
func NewFakePDScaler() Scaler {
	return &fakePDScaler{}
}

func (s *fakePDScaler) Scale(meta metav1.Object, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	if *newSet.Spec.Replicas > *oldSet.Spec.Replicas {
		return s.ScaleOut(meta, oldSet, newSet)
	} else if *newSet.Spec.Replicas < *oldSet.Spec.Replicas {
		return s.ScaleIn(meta, oldSet, newSet)
	}
	return nil
}

func (s *fakePDScaler) ScaleOut(_ metav1.Object, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	setReplicasAndDeleteSlots(newSet, *oldSet.Spec.Replicas+1, nil)
	return nil
}

func (s *fakePDScaler) ScaleIn(_ metav1.Object, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	setReplicasAndDeleteSlots(newSet, *oldSet.Spec.Replicas-1, nil)
	return nil
}

func (s *fakePDScaler) SyncAutoScalerAnn(tc metav1.Object, actual *apps.StatefulSet) error {
	return nil
}
