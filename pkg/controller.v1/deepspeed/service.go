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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	kubeflowv1 "github.com/kubeflow/training-operator/pkg/apis/kubeflow.org/v1"
	"github.com/kubeflow/training-operator/pkg/common/util"
)

// reconcileServices checks and updates services for each given ReplicaSpec.
// It will requeue the job in case of an error while creating/deleting services.
func (jc *DeepSpeedJobReconciler) ReconcileServices(
	job metav1.Object,
	services []*corev1.Service,
	rtype commonv1.ReplicaType,
	spec *commonv1.ReplicaSpec) error {

	deepspeedJob, ok := job.(*kubeflowv1.DeepSpeedJob)
	if !ok {
		return fmt.Errorf("%v is not a type of DeepSpeedJob", deepspeedJob)
	}

	// ignoring launcher service for now.
	if rtype == kubeflowv1.DeepSpeedReplicaTypeLauncher {
		return nil
	}

	genericLabels := jc.GenLabels(job.GetName())
	labels := defaultWorkerLabels(genericLabels)
	newSvc := newService(deepspeedJob, deepspeedJob.Name+workerSuffix, labels)
	_, err := jc.getOrCreateService(deepspeedJob, newSvc)
	return err

}

// GetServicesForJob returns the set of services that this job should manage.
// It also reconciles ControllerRef by adopting/orphaning.
// Note that the returned services are pointers into the cache.
func (jc *DeepSpeedJobReconciler) GetServicesForJob(jobObject interface{}) ([]*corev1.Service, error) {
	job, ok := jobObject.(metav1.Object)
	if !ok {
		return nil, fmt.Errorf("job is not of type metav1.Object")
	}

	// List all services to include those that don't match the selector anymore
	// but have a ControllerRef pointing to this controller.
	serviceList := &corev1.ServiceList{}
	err := jc.List(context.Background(), serviceList, client.MatchingLabels(jc.GenLabels(job.GetName())), client.InNamespace(job.GetNamespace()))
	if err != nil {
		return nil, err
	}

	ret := util.ConvertServiceList(serviceList.Items)
	return ret, nil
}

func (jc *DeepSpeedJobReconciler) getOrCreateService(job *kubeflowv1.DeepSpeedJob, newSvc *corev1.Service) (*corev1.Service, error) {
	svc := &corev1.Service{}
	NamespacedName := types.NamespacedName{Namespace: job.Namespace, Name: newSvc.Name}
	err := jc.Get(context.Background(), NamespacedName, svc)

	if errors.IsNotFound(err) {
		return jc.KubeClientSet.CoreV1().Services(job.Namespace).Create(context.TODO(), newSvc, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(svc, job) {
		msg := fmt.Sprintf(MessageResourceExists, svc.Name, svc.Kind)
		jc.recorder.Event(job, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	// If the Service selector is changed, update it.
	if !equality.Semantic.DeepEqual(svc.Spec.Selector, newSvc.Spec.Selector) {
		svc = svc.DeepCopy()
		svc.Spec.Selector = newSvc.Spec.Selector
		return jc.KubeClientSet.CoreV1().Services(svc.Namespace).Update(context.TODO(), svc, metav1.UpdateOptions{})
	}

	return svc, nil
}

func newService(job *kubeflowv1.DeepSpeedJob, name string, selector map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: job.Namespace,
			Labels: map[string]string{
				"app": job.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, kubeflowv1.SchemeGroupVersion.WithKind(kubeflowv1.DeepSpeedJobKind)),
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  selector,
		},
	}
}
