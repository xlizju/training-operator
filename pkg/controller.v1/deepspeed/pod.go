// Copyright 2023 The Kubeflow Authors.
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

package deepspeed

import (
	"context"
	"fmt"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	kubeflowv1 "github.com/kubeflow/training-operator/pkg/apis/kubeflow.org/v1"
	"github.com/kubeflow/training-operator/pkg/common/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetPodsForJob returns the set of pods that this job should manage.
// It also reconciles ControllerRef by adopting/orphaning.
// Note that the returned Pods are pointers into the cache.
func (jc *DeepSpeedJobReconciler) GetPodsForJob(jobObject interface{}) ([]*corev1.Pod, error) {
	job, ok := jobObject.(metav1.Object)
	if !ok {
		return nil, fmt.Errorf("job is not of type metav1.Object")
	}

	// Create selector.
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: jc.GenLabels(job.GetName()),
	})

	if err != nil {
		return nil, fmt.Errorf("couldn't convert Job selector: %v", err)
	}
	// List all pods to include those that don't match the selector anymore
	// but have a ControllerRef pointing to this controller.
	podlist := &corev1.PodList{}
	err = jc.List(context.Background(), podlist,
		client.MatchingLabelsSelector{Selector: selector}, client.InNamespace(job.GetNamespace()))
	if err != nil {
		return nil, err
	}

	var filter util.ObjectFilterFunction = func(obj metav1.Object) bool {
		return metav1.IsControlledBy(obj, job)
	}

	return util.ConvertPodListWithFilter(podlist.Items, filter), nil
}

func (jc *DeepSpeedJobReconciler) ReconcilePods(
	job interface{},
	jobStatus *commonv1.JobStatus,
	pods []*corev1.Pod,
	rtype commonv1.ReplicaType,
	spec *commonv1.ReplicaSpec,
	replicas map[commonv1.ReplicaType]*commonv1.ReplicaSpec,
) error {

	deepspeedJob, ok := job.(*kubeflowv1.DeepSpeedJob)
	if !ok {
		return fmt.Errorf("%v is not a type of DeepSpeedJob", deepspeedJob)
	}

	// first set StartTime.
	if jobStatus.StartTime == nil {
		now := metav1.Now()
		jobStatus.StartTime = &now
	}

	initializeReplicaStatuses(jobStatus, rtype)

	// Get the launcher Job for this DeepSpeedJob.
	launcher, err := jc.getLauncherJob(deepspeedJob)
	if err != nil {
		return err
	}

	var worker []*corev1.Pod
	// We're done if the launcher either succeeded or failed.
	done := launcher != nil && isJobFinished(launcher)

	if !done {
		if config, err := jc.getOrCreateConfigMap(deepspeedJob); config == nil || err != nil {
			return err
		}

		_, err = jc.getOrCreateSSHAuthSecret(deepspeedJob)
		if err != nil {
			return fmt.Errorf("creating SSH auth secret: %w", err)
		}

		worker, err = jc.getOrCreateWorker(deepspeedJob)
		if err != nil {
			return err
		}

		if launcher == nil {
			launcher, err = jc.KubeClientSet.BatchV1().Jobs(deepspeedJob.Namespace).Create(context.TODO(), jc.newLauncherJob(deepspeedJob), metav1.CreateOptions{})
			if err != nil {
				jc.recorder.Eventf(deepspeedJob, corev1.EventTypeWarning, deepspeedJobFailedReason, "launcher job created failed: %v", err)
				return fmt.Errorf("creating launcher job: %w", err)
			}
		}
	}

	// Finally, we update the status block of the DeepSpeedJob resource to reflect the
	// current state of the world.
	err = jc.updateDeepSpeedJobStatus(deepspeedJob, launcher, worker)
	if err != nil {
		return err
	}
	return nil
}
