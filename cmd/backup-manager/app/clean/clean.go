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

package clean

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"github.com/pingcap/tidb-operator/cmd/backup-manager/app/constants"
	"github.com/pingcap/tidb-operator/cmd/backup-manager/app/util"
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
)

var (
	defaultBackoff = wait.Backoff{
		Duration: 100 * time.Millisecond,
		Factor:   2.0,
		Jitter:   0,
		Steps:    8,
		Cap:      time.Second,
	}
)

// Options contains the input arguments to the backup command
type Options struct {
	Namespace  string
	BackupName string
}

func (bo *Options) String() string {
	return fmt.Sprintf("%s/%s", bo.Namespace, bo.BackupName)
}

// cleanBRRemoteBackupData clean the backup data from remote
func (bo *Options) cleanBRRemoteBackupData(ctx context.Context, backup *v1alpha1.Backup) error {
	opt := backup.GetCleanOption()

	backend, err := util.NewStorageBackend(backup.Spec.StorageProvider)
	if err != nil {
		return err
	}
	defer backend.Close()

	round := 0
	return util.RetryOnError(opt.RetryCount, 0, func() error {
		round++
		return bo.cleanBRRemoteBackupDataOnce(ctx, backend, opt, round)
	})
}

func (bo *Options) cleanBRRemoteBackupDataOnce(ctx context.Context, backend *util.StorageBackend, opt v1alpha1.CleanOption, round int) error {
	klog.Infof("For backup %s clean %d, start to clean backup with opt: %+v", bo, round, opt)

	iter := backend.ListPage(nil)
	backoff := defaultBackoff
	index := 0
	count, deletedCount, failedCount := 0, 0, 0
	for {
		needBackoff := false
		index++
		logPrefix := fmt.Sprintf("For backup %s clean %d-%d", bo, round, index)

		objs, err := iter.Next(ctx, int(opt.PageSize))
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		klog.Infof("%s, try to delete %d objects", logPrefix, len(objs))
		result := backend.BatchDeleteObjects(ctx, objs, opt.BatchDeleteOption)

		count += len(objs)
		deletedCount += len(result.Deleted)
		failedCount += len(result.Errors)

		if len(result.Deleted) != 0 {
			klog.Infof("%s, delete %d objects successfully", logPrefix, len(result.Deleted))
			for _, obj := range result.Deleted {
				klog.V(4).Infof("%s, delete object %s successfully", logPrefix, obj)
			}
		}
		if len(result.Errors) != 0 {
			klog.Errorf("%s, delete %d objects failed", logPrefix, len(result.Errors))
			for _, oerr := range result.Errors {
				klog.V(4).Infof("%s, delete object %s failed: %s", logPrefix, oerr.Key, oerr.Err)
			}
			needBackoff = true
		}
		if len(result.Deleted)+len(result.Errors) < len(objs) {
			klog.Errorf("%s, sum of deleted and failed objects %d is less than expected", logPrefix, len(result.Deleted)+len(result.Errors))
			needBackoff = true
		}

		if needBackoff {
			time.Sleep(backoff.Step())
		} else {
			backoff = defaultBackoff
		}
	}

	klog.Infof("For backup %s clean %d, clean backup finished, total:%d deleted:%d failed:%d", bo, round, count, deletedCount, failedCount)

	if deletedCount < count {
		return fmt.Errorf("some objects failed to be deleted")
	}

	objs, err := backend.ListPage(nil).Next(ctx, int(opt.PageSize))
	if err != nil && err != io.EOF {
		return err
	}
	if len(objs) != 0 {
		return fmt.Errorf("some objects are missing to be deleted")
	}

	return nil
}

func (bo *Options) cleanRemoteBackupData(ctx context.Context, bucket string, opts []string) error {
	destBucket := util.NormalizeBucketURI(bucket)
	args := util.ConstructRcloneArgs(constants.RcloneConfigArg, opts, "delete", destBucket, "", true)
	output, err := exec.CommandContext(ctx, "rclone", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("cluster %s, execute rclone delete command failed, output: %s, err: %v", bo, string(output), err)
	}

	args = util.ConstructRcloneArgs(constants.RcloneConfigArg, opts, "delete", fmt.Sprintf("%s.tmp", destBucket), "", true)
	output, err = exec.CommandContext(ctx, "rclone", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("cluster %s, execute rclone delete command failed, output: %s, err: %v", bo, string(output), err)
	}

	klog.Infof("cluster %s backup %s was deleted successfully", bo, bucket)
	return nil
}
