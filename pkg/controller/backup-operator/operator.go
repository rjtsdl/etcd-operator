// Copyright 2017 The etcd-operator Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"fmt"
	"os"

	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"
	"github.com/coreos/etcd-operator/pkg/backup"
	"github.com/coreos/etcd-operator/pkg/client"
	"github.com/coreos/etcd-operator/pkg/generated/clientset/versioned"
	"github.com/coreos/etcd-operator/pkg/util/constants"
	"github.com/coreos/etcd-operator/pkg/util/k8sutil"

	"github.com/sirupsen/logrus"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
)

type Controller struct {
	logger *logrus.Entry

	namespace string

	backupCRCli versioned.Interface
	kubeExtCli  apiextensionsclient.Interface

	createCRD bool

	// The key of the backups map would be the key used in the workqueue
	backups map[string]*backup.Backup
}

// New creates a backup operator.
func New(createCRD bool) *Controller {
	return &Controller{
		logger:      logrus.WithField("pkg", "controller"),
		namespace:   os.Getenv(constants.EnvOperatorPodNamespace),
		backupCRCli: client.MustNewInCluster(),
		kubeExtCli:  k8sutil.MustNewKubeExtClient(),
		createCRD:   createCRD,
	}
}

// Start starts the Backup operator.
func (b *Controller) Start(ctx context.Context) error {
	if b.createCRD {
		if err := b.initCRD(); err != nil {
			return err
		}
	}

	go b.run(ctx)
	<-ctx.Done()
	return ctx.Err()
}

func (b *Controller) initCRD() error {
	err := k8sutil.CreateCRD(b.kubeExtCli, api.EtcdBackupCRDName, api.EtcdBackupResourceKind, api.EtcdBackupResourcePlural, "")
	if err != nil {
		return fmt.Errorf("failed to create CRD: %v", err)
	}
	return k8sutil.WaitCRDReady(b.kubeExtCli, api.EtcdBackupCRDName)
}
