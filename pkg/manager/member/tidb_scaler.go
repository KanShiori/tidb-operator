// Copyright 2021 PingCAP, Inc.
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

	apps "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog"

	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/controller"
	"github.com/pingcap/tidb-operator/pkg/util"
)

// tidbScaler 负责 TiDB 的缩扩容的业务层逻辑
type tidbScaler struct {
	generalScaler
}

// NewTiDBScaler returns a TiDB Scaler.
func NewTiDBScaler(deps *controller.Dependencies) *tidbScaler {
	return &tidbScaler{generalScaler: generalScaler{deps: deps}}
}

// Scale 缩扩容操作入口
// Scale scales in or out of the statefulset.
func (s *tidbScaler) Scale(meta metav1.Object, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	scaling, _, _, _ := scaleOne(oldSet, newSet)
	if scaling > 0 {
		return s.ScaleOut(meta, oldSet, newSet)
	} else if scaling < 0 {
		return s.ScaleIn(meta, oldSet, newSet)
	}
	return nil
}

// ScaleOut 进行扩容操作
// ScaleOut scales out of the statefulset.
func (s *tidbScaler) ScaleOut(meta metav1.Object, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	// 得到当前需要扩的 Pod 编号，以及扩容后的 Replica
	_, ordinal, replicas, deleteSlots := scaleOne(oldSet, newSet)
	resetReplicas(newSet, oldSet)
	obj, ok := meta.(runtime.Object)
	if !ok {
		klog.Errorf("cluster[%s/%s] can't convert to runtime.Object", meta.GetNamespace(), meta.GetName())
		return nil
	}
	klog.Infof("scaling out tidb statefulset %s/%s, ordinal: %d (replicas: %d, delete slots: %v)", oldSet.Namespace, oldSet.Name, ordinal, replicas, deleteSlots.List())
	// 删除新 Pod 旧的 PVC（可能之前缩容过来的），让 StatefulSet 自动创建新的
	skipReason, err := s.deleteDeferDeletingPVC(obj, v1alpha1.TiDBMemberType, ordinal)
	if err != nil {
		return err
	} else if len(skipReason) != 1 || skipReason[ordinalPodName(v1alpha1.TiDBMemberType, meta.GetName(), ordinal)] != skipReasonScalerPVCNotFound {
		// wait for all PVCs to be deleted
		return controller.RequeueErrorf("tidbScaler.ScaleOut, cluster %s/%s ready to scale out, skip reason %v, wait for next round", meta.GetNamespace(), meta.GetName(), skipReason)
	}
	// 设置 StatefulSet 为新的副本数量
	setReplicasAndDeleteSlots(newSet, replicas, deleteSlots)
	return nil
}

// ScaleIn 缩容操作
// ScaleIn scales in of the statefulset.
func (s *tidbScaler) ScaleIn(meta metav1.Object, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	ns := meta.GetNamespace()
	tcName := meta.GetName()
	// 得到当前要缩容的 PD Pod
	// NOW, we can only remove one member at a time when scaling in
	_, ordinal, replicas, deleteSlots := scaleOne(oldSet, newSet)
	resetReplicas(newSet, oldSet)

	klog.Infof("scaling in tidb statefulset %s/%s, ordinal: %d (replicas: %d, delete slots: %v)", oldSet.Namespace, oldSet.Name, ordinal, replicas, deleteSlots.List())
	// We need to remove member from cluster before reducing statefulset replicas
	var podName string
	switch meta.(type) {
	case *v1alpha1.TidbCluster:
		podName = ordinalPodName(v1alpha1.TiDBMemberType, tcName, ordinal)
	default:
		klog.Errorf("tidbScaler.ScaleIn: failed to convert cluster %s/%s", meta.GetNamespace(), meta.GetName())
		return nil
	}
	pod, err := s.deps.PodLister.Pods(ns).Get(podName)
	if err != nil {
		return fmt.Errorf("tidbScaler.ScaleIn: failed to get pods %s for cluster %s/%s, error: %s", podName, ns, tcName, err)
	}

	// 将 Pod 包含的 PVC 标记为删除
	pvcs, err := util.ResolvePVCFromPod(pod, s.deps.PVCLister)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("tidbScaler.ScaleIn: failed to get pvcs for pod %s/%s in tc %s/%s, error: %s", ns, pod.Name, ns, tcName, err)
	}
	tc, _ := meta.(*v1alpha1.TidbCluster)
	for _, pvc := range pvcs {
		if err := addDeferDeletingAnnoToPVC(tc, pvc, s.deps.PVCControl); err != nil {
			return err
		}
	}

	// 设置 StatefulSet 的 ReplicaSet 进行缩容
	setReplicasAndDeleteSlots(newSet, replicas, deleteSlots)
	return nil
}

// SyncAutoScalerAnn would reclaim the auto-scaling out slots if the target pod is no longer existed
// NOTE: this method should be removed later.
func (s *tidbScaler) SyncAutoScalerAnn(_ metav1.Object, _ *apps.StatefulSet) error {
	return nil
}
