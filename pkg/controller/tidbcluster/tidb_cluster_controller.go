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

package tidbcluster

import (
	"fmt"
	"time"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/controller"
	mm "github.com/pingcap/tidb-operator/pkg/manager/member"
	"github.com/pingcap/tidb-operator/pkg/manager/meta"
	apps "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
)

// Controller 为 TiDBCluster 的 controller
// Controller controls tidbclusters.
type Controller struct {
	deps *controller.Dependencies // 所有组件依赖
	// control returns an interface capable of syncing a tidb cluster.
	// Abstracted out for testing.
	control ControlInterface // 抽象 Control 接口，用于测试模拟
	// tidbclusters that need to be synced.
	queue workqueue.RateLimitingInterface // Event Queue
}

// NewController creates a tidbcluster controller.
func NewController(deps *controller.Dependencies) *Controller {
	c := &Controller{
		deps: deps,
		control: NewDefaultTidbClusterControl(
			deps.TiDBClusterControl,
			mm.NewPDMemberManager(deps, mm.NewPDScaler(deps), mm.NewPDUpgrader(deps), mm.NewPDFailover(deps)),
			mm.NewTiKVMemberManager(deps, mm.NewTiKVFailover(deps), mm.NewTiKVScaler(deps), mm.NewTiKVUpgrader(deps)),
			mm.NewTiDBMemberManager(deps, mm.NewTiDBScaler(deps), mm.NewTiDBUpgrader(deps), mm.NewTiDBFailover(deps)),
			meta.NewReclaimPolicyManager(deps),
			meta.NewMetaManager(deps),
			mm.NewOrphanPodsCleaner(deps),
			mm.NewRealPVCCleaner(deps),
			mm.NewPVCResizer(deps),
			mm.NewPumpMemberManager(deps, mm.NewPumpScaler(deps)),
			mm.NewTiFlashMemberManager(deps, mm.NewTiFlashFailover(deps), mm.NewTiFlashScaler(deps), mm.NewTiFlashUpgrader(deps)),
			mm.NewTiCDCMemberManager(deps, mm.NewTiCDCScaler(deps), mm.NewTiCDCUpgrader(deps)),
			mm.NewTidbDiscoveryManager(deps),
			mm.NewTidbClusterStatusManager(deps),
			&tidbClusterConditionUpdater{},
			deps.Recorder,
		),
		queue: workqueue.NewNamedRateLimitingQueue(
			controller.NewControllerRateLimiter(1*time.Second, 100*time.Second),
			"tidbcluster",
		),
	}

	// 向 Informer 注入回调
	//
	// 使用的 Informer 有：
	//   1. TiDBCluster Informer : 监控 CR TiDBCluster 资源变化
	//   2. StatefulSet Informer : TiDB Operator 所部署的组件（PD TiDB TiKV Pump TiCDC TiFlash）都是基于 StatefulSet 部署的，
	//							   所以这里监控 StatefulSet 的变化
	// 但是，所有的回调最后都会调用 enqueueTidbCluster 函数来进行入 queue 处理
	tidbClusterInformer := deps.InformerFactory.Pingcap().V1alpha1().TidbClusters()
	statefulsetInformer := deps.KubeInformerFactory.Apps().V1().StatefulSets()
	tidbClusterInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueueTidbCluster,
		UpdateFunc: func(old, cur interface{}) {
			c.enqueueTidbCluster(cur)
		},
		DeleteFunc: c.enqueueTidbCluster,
	})
	statefulsetInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.addStatefulSet,
		UpdateFunc: func(old, cur interface{}) {
			c.updateStatefulSet(old, cur)
		},
		DeleteFunc: c.deleteStatefulSet,
	})

	return c
}

// Run TiDBCluster Controller 的入口函数
// Run runs the tidbcluster controller.
func (c *Controller) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Info("Starting tidbcluster controller")
	defer klog.Info("Shutting down tidbcluster controller")

	// 启动多个 worker，每个 worker 消费 Controller.work， 执行操作
	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}

	<-stopCh
}

// worker runs a worker goroutine that invokes processNextWorkItem until the the controller's queue is closed
func (c *Controller) worker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理一次来自 queue 的事件
// processNextWorkItem dequeues items, processes them, and marks them done. It enforces that the syncHandler is never
// invoked concurrently with the same key.
func (c *Controller) processNextWorkItem() bool {
	// 阻塞读取一个 event
	// event 来自于 informer 的回调，将 event 放入了 queue 中
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	// 进行 sync 协调调用
	if err := c.sync(key.(string)); err != nil {
		if perrors.Find(err, controller.IsRequeueError) != nil {
			klog.Infof("TidbCluster: %v, still need sync: %v, requeuing", key.(string), err)
		} else {
			utilruntime.HandleError(fmt.Errorf("TidbCluster: %v, sync failed %v, requeuing", key.(string), err))
		}

		// 处理失败，重启加入队列重试，不过要收到 rate limit 限制
		c.queue.AddRateLimited(key)
	} else {
		// 处理成功，从 rate limit 移除元素
		c.queue.Forget(key)
	}
	return true
}

// sync syncs the given tidbcluster.
func (c *Controller) sync(key string) error {
	startTime := time.Now()
	defer func() {
		klog.V(4).Infof("Finished syncing TidbCluster %q (%v)", key, time.Since(startTime))
	}()

	// 从 key 得到 TiDBCluster 对象
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	tc, err := c.deps.TiDBClusterLister.TidbClusters(ns).Get(name)
	if errors.IsNotFound(err) {
		klog.Infof("TidbCluster has been deleted %v", key)
		// 已经被删除，直接返回
		// NOTE: 因为 Kubernetes 中删除对象，会先标记其对象被删除 (deletionTimestamp)，然后 controller 清理，最后清理对象记录
		// 所以这里是对象记录都被删除了，表明经过了清理流程，因此直接返回
		return nil
	}
	if err != nil {
		return err
	}

	// 进行 sync
	// WHY? 这里为什么需要进行 Deepcopy
	return c.syncTidbCluster(tc.DeepCopy())
}

func (c *Controller) syncTidbCluster(tc *v1alpha1.TidbCluster) error {
	return c.control.UpdateTidbCluster(tc)
}

// enqueueTidbCluster 为 TiDBCluster Add Update Delete 的回调，将 key 放入 queue
// enqueueTidbCluster enqueues the given tidbcluster in the work queue.
func (c *Controller) enqueueTidbCluster(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Cound't get key for object %+v: %v", obj, err))
		return
	}
	c.queue.Add(key)
}

// addStatefulSet 为 StatefulSet Add 回调
// addStatefulSet adds the tidbcluster for the statefulset to the sync queue
func (c *Controller) addStatefulSet(obj interface{}) {
	set := obj.(*apps.StatefulSet)
	ns := set.GetNamespace()
	setName := set.GetName()

	if set.DeletionTimestamp != nil {
		// on a restart of the controller manager, it's possible a new statefulset shows up in a state that
		// is already pending deletion. Prevent the statefulset from being a creation observation.
		c.deleteStatefulSet(set)
		return
	}

	// 当 StatefuleSet 新创建时，更新 TiDBCluster status
	// If it has a ControllerRef, that's all that matters.
	tc := c.resolveTidbClusterFromSet(ns, set)
	if tc == nil {
		return
	}
	klog.V(4).Infof("StatefulSet %s/%s created, TidbCluster: %s/%s", ns, setName, ns, tc.Name)
	c.enqueueTidbCluster(tc)
}

// updateStatefulSet 为 StatefulSet Update 回调
// updateStatefulSet adds the tidbcluster for the current and old statefulsets to the sync queue.
func (c *Controller) updateStatefulSet(old, cur interface{}) {
	curSet := cur.(*apps.StatefulSet)
	oldSet := old.(*apps.StatefulSet)
	ns := curSet.GetNamespace()
	setName := curSet.GetName()
	if curSet.ResourceVersion == oldSet.ResourceVersion {
		// Periodic resync will send update events for all known statefulsets.
		// Two different versions of the same statefulset will always have different RVs.
		return
	}

	// If it has a ControllerRef, that's all that matters.
	tc := c.resolveTidbClusterFromSet(ns, curSet)
	if tc == nil {
		return
	}
	klog.V(4).Infof("StatefulSet %s/%s updated, TidbCluster: %s/%s", ns, setName, ns, tc.Name)
	c.enqueueTidbCluster(tc)
}

// deleteStatefulSet 为 StatefulSet Delete 回调
// deleteStatefulSet enqueues the tidbcluster for the statefulset accounting for deletion tombstones.
func (c *Controller) deleteStatefulSet(obj interface{}) {
	set, ok := obj.(*apps.StatefulSet)
	ns := set.GetNamespace()
	setName := set.GetName()

	// When a delete is dropped, the relist will notice a statefuset in the store not
	// in the list, leading to the insertion of a tombstone object which contains
	// the deleted key/value.
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %+v", obj))
			return
		}
		set, ok = tombstone.Obj.(*apps.StatefulSet)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a statefuset %+v", obj))
			return
		}
	}

	// If it has a TidbCluster, that's all that matters.
	tc := c.resolveTidbClusterFromSet(ns, set)
	if tc == nil {
		return
	}
	klog.V(4).Infof("StatefulSet %s/%s deleted through %v.", ns, setName, utilruntime.GetCaller())
	c.enqueueTidbCluster(tc)
}

// resolveTidbClusterFromSet returns the TidbCluster by a StatefulSet,
// or nil if the StatefulSet could not be resolved to a matching TidbCluster
// of the correct Kind.
func (c *Controller) resolveTidbClusterFromSet(namespace string, set *apps.StatefulSet) *v1alpha1.TidbCluster {
	controllerRef := metav1.GetControllerOf(set)
	if controllerRef == nil {
		return nil
	}

	// We can't look up by UID, so look up by Name and then verify UID.
	// Don't even try to look up by Name if it's the wrong Kind.
	if controllerRef.Kind != controller.ControllerKind.Kind {
		return nil
	}
	tc, err := c.deps.TiDBClusterLister.TidbClusters(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if tc.UID != controllerRef.UID {
		// The controller we found with this Name is not the same one that the
		// ControllerRef points to.
		return nil
	}
	return tc
}
