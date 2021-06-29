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

package main

import (
	"context"
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	"github.com/pingcap/advanced-statefulset/client/apis/apps/v1/helper"
	asclientset "github.com/pingcap/advanced-statefulset/client/client/clientset/versioned"
	"github.com/pingcap/tidb-operator/pkg/client/clientset/versioned"
	"github.com/pingcap/tidb-operator/pkg/controller"
	"github.com/pingcap/tidb-operator/pkg/controller/autoscaler"
	"github.com/pingcap/tidb-operator/pkg/controller/backup"
	"github.com/pingcap/tidb-operator/pkg/controller/backupschedule"
	"github.com/pingcap/tidb-operator/pkg/controller/dmcluster"
	"github.com/pingcap/tidb-operator/pkg/controller/periodicity"
	"github.com/pingcap/tidb-operator/pkg/controller/restore"
	"github.com/pingcap/tidb-operator/pkg/controller/tidbcluster"
	"github.com/pingcap/tidb-operator/pkg/controller/tidbinitializer"
	"github.com/pingcap/tidb-operator/pkg/controller/tidbmonitor"
	"github.com/pingcap/tidb-operator/pkg/features"
	"github.com/pingcap/tidb-operator/pkg/metrics"
	"github.com/pingcap/tidb-operator/pkg/scheme"
	"github.com/pingcap/tidb-operator/pkg/upgrader"
	"github.com/pingcap/tidb-operator/pkg/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-base/logs"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
	var cfg *rest.Config
	cliCfg := controller.DefaultCLIConfig()
	cliCfg.AddFlag(flag.CommandLine)
	features.DefaultFeatureGate.AddFlag(flag.CommandLine)
	flag.Parse()

	if cliCfg.PrintVersion {
		version.PrintVersionInfo()
		os.Exit(0)
	}

	logs.InitLogs()
	defer logs.FlushLogs()

	version.LogVersionInfo()
	flag.VisitAll(func(flag *flag.Flag) {
		klog.V(1).Infof("FLAG: --%s=%q", flag.Name, flag.Value)
	})

	// 注册 Metrics 接口
	metrics.RegisterMetrics()

	hostName, err := os.Hostname()
	if err != nil {
		klog.Fatalf("failed to get hostname: %v", err)
	}

	ns := os.Getenv("NAMESPACE")
	if ns == "" {
		klog.Fatal("NAMESPACE environment variable not set")
	}

	helmRelease := os.Getenv("HELM_RELEASE")
	if helmRelease == "" {
		klog.Info("HELM_RELEASE environment variable not set")
	}

	// 读取 kube config 路径
	// 这里得到了 API 访问需要使用的 cfg
	kubconfig := os.Getenv("KUBECONFIG")
	if kubconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		klog.Fatalf("failed to get config: %v", err)
	}

	/////////////////////////
	// ClientSet 创建
	//////////////////////////

	// ClientSet 包含了一个 group 下的所有 version cli
	// 通过 Kubernetes code-generator 会自动生成 CRD 对应的 ClientSet

	// 从 cfg 构建多版本的 Pincap Clientset
	cli, err := versioned.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to create Clientset: %v", err)
	}

	// 从 cfg 构建 Kubernetes 内置的 Clientset
	// 包含了 Kubernetes 基本的资源对象的 cli
	var kubeCli kubernetes.Interface
	kubeCli, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to get kubernetes Clientset: %v", err)
	}

	// 从 cfg 构建多版本的 Advanced Stateful ClientSet
	asCli, err := asclientset.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to get advanced-statefulset Clientset: %v", err)
	}

	// WHAT? genericCli 的作用是啥
	// TODO: optimize the read of genericCli with the shared cache
	genericCli, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		klog.Fatalf("failed to get the generic kube-apiserver client: %v", err)
	}

	// WHAT? 创建 upgrader，用于 Operator 的升级
	// note that kubeCli here must not be the hijacked one
	var operatorUpgrader upgrader.Interface
	if cliCfg.ClusterScoped {
		operatorUpgrader = upgrader.NewUpgrader(kubeCli, cli, asCli, metav1.NamespaceAll)
	} else {
		operatorUpgrader = upgrader.NewUpgrader(kubeCli, cli, asCli, ns)
	}

	// 打开了 Advanced Stateful Set 功能，那么修改 kubcli 使用 StatefulSet 的行为
	if features.DefaultFeatureGate.Enabled(features.AdvancedStatefulSet) {
		// If AdvancedStatefulSet is enabled, we hijack the Kubernetes client to use
		// AdvancedStatefulSet.
		kubeCli = helper.NewHijackClient(kubeCli, asCli)
	}

	// 构建 Dependencies
	// 所有组件都会放在 Dependencies
	deps := controller.NewDependencies(ns, cliCfg, cli, kubeCli, genericCli)

	// onStarted 启动函数
	// 选举成功后会调用 onStarted 启动
	onStarted := func(ctx context.Context) {
		// Upgrade before running any controller logic. If it fails, we wait
		// for process supervisor to restart it again.
		if err := operatorUpgrader.Upgrade(); err != nil {
			klog.Fatalf("failed to upgrade: %v", err)
		}

		// Define some nested types to simplify the codebase
		type Controller interface {
			Run(int, <-chan struct{})
		}
		type InformerFactory interface {
			Start(stopCh <-chan struct{})
			WaitForCacheSync(stopCh <-chan struct{}) map[reflect.Type]bool
		}

		// 初始化所有 CR Controller
		//  + TiDBCluster Controller 管理 TiDBCluster 资源
		//  + DMCluster Controller 管理 DMCluster 资源
		//  + Backup Controller 管理 Backup 资源
		//  + Restore Controller 管理 Restore 资源
		//  + Backup Schedule Controller 管理 BackupSchedule 资源
		//  + TiDBInitialize Controller 管理 TiDBInitialize 资源
		//  + TiDBMonitor Controller 管理 TiDBMonitor 资源
		// Initialize all controllers
		controllers := []Controller{
			tidbcluster.NewController(deps),
			dmcluster.NewController(deps),
			backup.NewController(deps),
			restore.NewController(deps),
			backupschedule.NewController(deps),
			tidbinitializer.NewController(deps),
			tidbmonitor.NewController(deps),
		}
		if cliCfg.PodWebhookEnabled {
			controllers = append(controllers, periodicity.NewController(deps))
		}
		if features.DefaultFeatureGate.Enabled(features.AutoScaling) {
			controllers = append(controllers, autoscaler.NewController(deps))
		}

		// 所有 Informer 同步缓存
		// Start informer factories after all controllers are initialized.
		informerFactories := []InformerFactory{
			deps.InformerFactory,
			deps.KubeInformerFactory,
			deps.LabelFilterKubeInformerFactory,
		}
		for _, f := range informerFactories {
			f.Start(ctx.Done())
			for v, synced := range f.WaitForCacheSync(wait.NeverStop) {
				if !synced {
					klog.Fatalf("error syncing informer for %v", v)
				}
			}
		}
		klog.Info("cache of informer factories sync successfully")

		// 启动所有 Controller
		// 所有 Control 循环执行 Run 函数
		// Start syncLoop for all controllers
		for _, controller := range controllers {
			c := controller
			go wait.Forever(func() { c.Run(cliCfg.Workers, ctx.Done()) }, cliCfg.WaitDuration)
		}
	}
	// onStopped 停止函数
	onStopped := func() {
		klog.Fatal("leader election lost")
	}

	// WHAT? 多个 tidb-controller-manager 情况下进行选举
	endPointsName := "tidb-controller-manager"
	if helmRelease != "" {
		endPointsName += "-" + helmRelease
	}
	// leader election for multiple tidb-controller-manager instances
	go wait.Forever(func() {
		leaderelection.RunOrDie(context.TODO(), leaderelection.LeaderElectionConfig{
			Lock: &resourcelock.EndpointsLock{
				EndpointsMeta: metav1.ObjectMeta{
					Namespace: ns,
					Name:      endPointsName,
				},
				Client: kubeCli.CoreV1(),
				LockConfig: resourcelock.ResourceLockConfig{
					Identity:      hostName,
					EventRecorder: &record.FakeRecorder{},
				},
			},
			LeaseDuration: cliCfg.LeaseDuration,
			RenewDeadline: cliCfg.RenewDeadline,
			RetryPeriod:   cliCfg.RetryPeriod,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: onStarted,
				OnStoppedLeading: onStopped,
			},
		})
	}, cliCfg.WaitDuration)

	// 创建 HTTP Server
	// 仅仅是 metrics 接口
	srv := createHTTPServer()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	go func() {
		sig := <-sc
		klog.Infof("got signal %s to exit", sig)
		if err2 := srv.Shutdown(context.Background()); err2 != nil {
			klog.Fatal("fail to shutdown the HTTP server", err2)
		}
	}()

	if err = srv.ListenAndServe(); err != http.ErrServerClosed {
		klog.Fatal(err)
	}
	klog.Infof("tidb-controller-manager exited")
}

func createHTTPServer() *http.Server {
	serverMux := http.NewServeMux()
	// HTTP path for prometheus.
	serverMux.Handle("/metrics", promhttp.Handler())

	return &http.Server{
		Addr:    ":6060",
		Handler: serverMux,
	}
}
