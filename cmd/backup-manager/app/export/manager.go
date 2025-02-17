// Copyright 2019 PingCAP, Inc.
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

package export

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pingcap/tidb-operator/cmd/backup-manager/app/constants"
	"github.com/pingcap/tidb-operator/cmd/backup-manager/app/util"
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	bkconstants "github.com/pingcap/tidb-operator/pkg/backup/constants"
	backuputil "github.com/pingcap/tidb-operator/pkg/backup/util"
	listers "github.com/pingcap/tidb-operator/pkg/client/listers/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/controller"
	pkgutil "github.com/pingcap/tidb-operator/pkg/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	errorutils "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

// BackupManager mainly used to manage backup related work
type BackupManager struct {
	backupLister  listers.BackupLister
	StatusUpdater controller.BackupConditionUpdaterInterface
	Options
}

// NewBackupManager return a BackupManager
func NewBackupManager(
	backupLister listers.BackupLister,
	statusUpdater controller.BackupConditionUpdaterInterface,
	backupOpts Options) *BackupManager {
	return &BackupManager{
		backupLister,
		statusUpdater,
		backupOpts,
	}
}

func (bm *BackupManager) setOptions(backup *v1alpha1.Backup) (string, error) {
	bm.Options.Host = backup.Spec.From.Host

	if backup.Spec.From.Port != 0 {
		bm.Options.Port = backup.Spec.From.Port
	} else {
		bm.Options.Port = v1alpha1.DefaultTidbPort
	}

	if backup.Spec.From.User != "" {
		bm.Options.User = backup.Spec.From.User
	} else {
		bm.Options.User = v1alpha1.DefaultTidbUser
	}
	bm.Options.Password = util.GetOptionValueFromEnv(bkconstants.TidbPasswordKey, bkconstants.BackupManagerEnvVarPrefix)

	prefix, reason, err := backuputil.GetBackupPrefixName(backup)
	if err != nil {
		return reason, err
	}
	bm.Options.Prefix = strings.Trim(prefix, "/")
	return "", nil
}

// ProcessBackup used to process the backup logic
func (bm *BackupManager) ProcessBackup() error {
	ctx, cancel := util.GetContextForTerminationSignals(bm.ResourceName)
	defer cancel()

	var errs []error
	backup, err := bm.backupLister.Backups(bm.Namespace).Get(bm.ResourceName)
	if err != nil {
		errs = append(errs, err)
		klog.Errorf("can't find cluster %s backup %s CRD object, err: %v", bm, bm.ResourceName, err)
		uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
			Type:    v1alpha1.BackupFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "GetBackupCRFailed",
			Message: err.Error(),
		}, nil)
		errs = append(errs, uerr)
		return errorutils.NewAggregate(errs)
	}

	reason, err := bm.setOptions(backup)
	if err != nil {
		errs = append(errs, err)
		klog.Errorf("set dumpling backup %s option for cluster %s failed, err: %v", bm.ResourceName, bm, err)
		uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
			Type:    v1alpha1.BackupFailed,
			Status:  corev1.ConditionTrue,
			Reason:  reason,
			Message: err.Error(),
		}, nil)
		errs = append(errs, uerr)
		return errorutils.NewAggregate(errs)
	}

	var db *sql.DB
	var dsn string
	err = wait.PollImmediate(constants.PollInterval, constants.CheckTimeout, func() (done bool, err error) {
		dsn, err = bm.GetDSN(bm.TLSClient)
		if err != nil {
			klog.Errorf("can't get dsn of tidb cluster %s, err: %s", bm, err)
			return false, err
		}

		db, err = pkgutil.OpenDB(ctx, dsn)
		if err != nil {
			klog.Warningf("can't connect to tidb cluster %s, err: %s", bm, err)
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			return false, nil
		}
		return true, nil
	})

	if err != nil {
		errs = append(errs, err)
		klog.Errorf("cluster %s connect failed, err: %s", bm, err)
		uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
			Type:    v1alpha1.BackupFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "ConnectTidbFailed",
			Message: err.Error(),
		}, nil)
		errs = append(errs, uerr)
		return errorutils.NewAggregate(errs)
	}

	defer db.Close()
	return bm.performBackup(ctx, backup.DeepCopy(), db)
}

// use dumpling to export data
func (bm *BackupManager) performBackup(ctx context.Context, backup *v1alpha1.Backup, db *sql.DB) error {
	started := time.Now()

	err := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
		Type:   v1alpha1.BackupPrepare,
		Status: corev1.ConditionTrue,
	}, nil)
	if err != nil {
		return err
	}

	var errs []error
	oldTikvGCTime, err := bm.GetTikvGCLifeTime(ctx, db)
	if err != nil {
		errs = append(errs, err)
		klog.Errorf("cluster %s get %s failed, err: %s", bm, constants.TikvGCVariable, err)
		uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
			Type:    v1alpha1.BackupFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "GetTikvGCLifeTimeFailed",
			Message: err.Error(),
		}, nil)
		errs = append(errs, uerr)
		return errorutils.NewAggregate(errs)
	}
	klog.Infof("cluster %s %s is %s", bm, constants.TikvGCVariable, oldTikvGCTime)

	oldTikvGCTimeDuration, err := time.ParseDuration(oldTikvGCTime)
	if err != nil {
		errs = append(errs, err)
		klog.Errorf("cluster %s parse old %s failed, err: %s", bm, constants.TikvGCVariable, err)
		uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
			Type:    v1alpha1.BackupFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "ParseOldTikvGCLifeTimeFailed",
			Message: err.Error(),
		}, nil)
		errs = append(errs, uerr)
		return errorutils.NewAggregate(errs)
	}

	var tikvGCTimeDuration time.Duration
	var tikvGCLifeTime string
	if backup.Spec.TikvGCLifeTime != nil {
		tikvGCLifeTime = *backup.Spec.TikvGCLifeTime
		tikvGCTimeDuration, err = time.ParseDuration(tikvGCLifeTime)
		if err != nil {
			errs = append(errs, err)
			klog.Errorf("cluster %s parse configured %s failed, err: %s", bm, constants.TikvGCVariable, err)
			uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
				Type:    v1alpha1.BackupFailed,
				Status:  corev1.ConditionTrue,
				Reason:  "ParseConfiguredTikvGCLifeTimeFailed",
				Message: err.Error(),
			}, nil)
			errs = append(errs, uerr)
			return errorutils.NewAggregate(errs)
		}
	} else {
		tikvGCLifeTime = constants.TikvGCLifeTime
		tikvGCTimeDuration, err = time.ParseDuration(tikvGCLifeTime)
		if err != nil {
			errs = append(errs, err)
			klog.Errorf("cluster %s parse default %s failed, err: %s", bm, constants.TikvGCVariable, err)
			uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
				Type:    v1alpha1.BackupFailed,
				Status:  corev1.ConditionTrue,
				Reason:  "ParseDefaultTikvGCLifeTimeFailed",
				Message: err.Error(),
			}, nil)
			errs = append(errs, uerr)
			return errorutils.NewAggregate(errs)
		}
	}

	if oldTikvGCTimeDuration < tikvGCTimeDuration {
		err = bm.SetTikvGCLifeTime(ctx, db, tikvGCLifeTime)
		if err != nil {
			errs = append(errs, err)
			klog.Errorf("cluster %s set tikv GC life time to %s failed, err: %s", bm, constants.TikvGCLifeTime, err)
			uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
				Type:    v1alpha1.BackupFailed,
				Status:  corev1.ConditionTrue,
				Reason:  "SetTikvGCLifeTimeFailed",
				Message: err.Error(),
			}, nil)
			errs = append(errs, uerr)
			return errorutils.NewAggregate(errs)
		}
		klog.Infof("set cluster %s %s to %s success", bm, constants.TikvGCVariable, constants.TikvGCLifeTime)
	}

	backupFullPath := bm.getBackupFullPath()
	// TODO: Concurrent get file size and upload backup data to speed up processing time
	archiveBackupPath := backupFullPath + constants.DefaultArchiveExtention
	remotePath := strings.TrimPrefix(archiveBackupPath, constants.BackupRootPath+"/")
	bucketURI := bm.getDestBucketURI(remotePath)
	updatePathStatus := &controller.BackupUpdateStatus{
		BackupPath: &bucketURI,
	}
	err = bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
		Type:   v1alpha1.BackupRunning,
		Status: corev1.ConditionTrue,
	}, updatePathStatus)
	if err != nil {
		return err
	}

	backupErr := bm.dumpTidbClusterData(ctx, backupFullPath, backup)
	if oldTikvGCTimeDuration < tikvGCTimeDuration {
		// use another context to revert `tikv_gc_life_time` back.
		// `DefaultTerminationGracePeriodSeconds` for a pod is 30, so we use a smaller timeout value here.
		ctx2, cancel2 := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel2()
		err = bm.SetTikvGCLifeTime(ctx2, db, oldTikvGCTime)
		if err != nil {
			if backupErr != nil {
				errs = append(errs, backupErr)
			}
			errs = append(errs, err)
			klog.Errorf("cluster %s reset tikv GC life time to %s failed, err: %s", bm, oldTikvGCTime, err)
			uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
				Type:    v1alpha1.BackupFailed,
				Status:  corev1.ConditionTrue,
				Reason:  "ResetTikvGCLifeTimeFailed",
				Message: err.Error(),
			}, nil)
			errs = append(errs, uerr)
			return errorutils.NewAggregate(errs)
		}
		klog.Infof("reset cluster %s %s to %s success", bm, constants.TikvGCVariable, oldTikvGCTime)
	}

	if backupErr != nil {
		errs = append(errs, backupErr)
		klog.Errorf("dump cluster %s data failed, err: %s", bm, backupErr)
		uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
			Type:    v1alpha1.BackupFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "DumpTidbClusterFailed",
			Message: backupErr.Error(),
		}, nil)
		errs = append(errs, uerr)
		return errorutils.NewAggregate(errs)
	}
	klog.Infof("dump cluster %s data to %s success", bm, backupFullPath)

	commitTs, err := util.GetCommitTsFromMetadata(backupFullPath)
	if err != nil {
		errs = append(errs, err)
		klog.Errorf("get cluster %s commitTs failed, err: %s", bm, err)
		uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
			Type:    v1alpha1.BackupFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "GetCommitTsFailed",
			Message: err.Error(),
		}, nil)
		errs = append(errs, uerr)
		return errorutils.NewAggregate(errs)
	}
	klog.Infof("get cluster %s commitTs %s success", bm, commitTs)

	err = archiveBackupData(backupFullPath, archiveBackupPath)
	if err != nil {
		errs = append(errs, err)
		klog.Errorf("archive cluster %s backup data %s failed, err: %s", bm, archiveBackupPath, err)
		uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
			Type:    v1alpha1.BackupFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "ArchiveBackupDataFailed",
			Message: err.Error(),
		}, nil)
		errs = append(errs, uerr)
		return errorutils.NewAggregate(errs)
	}
	klog.Infof("archive cluster %s backup data %s success", bm, archiveBackupPath)

	opts := util.GetOptions(backup.Spec.StorageProvider)
	size, err := getBackupSize(ctx, archiveBackupPath, opts)
	if err != nil {
		errs = append(errs, err)
		klog.Errorf("get cluster %s archived backup file %s size %d failed, err: %s", bm, archiveBackupPath, size, err)
		uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
			Type:    v1alpha1.BackupFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "GetBackupSizeFailed",
			Message: err.Error(),
		}, nil)
		errs = append(errs, uerr)
		return errorutils.NewAggregate(errs)
	}
	klog.Infof("get cluster %s archived backup file %s size %d success", bm, archiveBackupPath, size)

	// archive backup data successfully, origin dir can be deleted safely
	os.RemoveAll(backupFullPath)

	err = bm.backupDataToRemote(ctx, archiveBackupPath, bucketURI, opts)
	if err != nil {
		errs = append(errs, err)
		klog.Errorf("backup cluster %s data to %s failed, err: %s", bm, bm.StorageType, err)
		uerr := bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
			Type:    v1alpha1.BackupFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "BackupDataToRemoteFailed",
			Message: err.Error(),
		}, nil)
		errs = append(errs, uerr)
		return errorutils.NewAggregate(errs)
	}
	klog.Infof("backup cluster %s data to %s success", bm, bm.StorageType)
	// backup to remote succeed, archive can be deleted now
	os.RemoveAll(archiveBackupPath)

	finish := time.Now()

	backupSizeReadable := humanize.Bytes(uint64(size))
	updateStatus := &controller.BackupUpdateStatus{
		TimeStarted:        &metav1.Time{Time: started},
		TimeCompleted:      &metav1.Time{Time: finish},
		BackupSize:         &size,
		BackupSizeReadable: &backupSizeReadable,
		CommitTs:           &commitTs,
	}

	return bm.StatusUpdater.Update(backup, &v1alpha1.BackupCondition{
		Type:   v1alpha1.BackupComplete,
		Status: corev1.ConditionTrue,
	}, updateStatus)
}
