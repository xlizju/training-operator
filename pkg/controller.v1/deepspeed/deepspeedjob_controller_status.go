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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	kubeflowv1 "github.com/kubeflow/training-operator/pkg/apis/kubeflow.org/v1"
)

const (
	// deepspeedJobCreatedReason is added in a deepspeedjob when it is created.
	deepspeedJobCreatedReason = "DeepSpeedJobCreated"
	// deepspeedJobSucceededReason is added in a deepspeedjob when it is succeeded.
	deepspeedJobSucceededReason = "DeepSpeedJobSucceeded"
	// deepspeedJobRunningReason is added in a deepspeedjob when it is running.
	deepspeedJobRunningReason = "DeepSpeedJobRunning"
	// deepspeedJobFailedReason is added in a deepspeedjob when it is failed.
	deepspeedJobFailedReason = "DeepSpeedJobFailed"
	// deepspeedJobEvict is added in a deepspeedjob when it is evicted.
	deepspeedJobEvict = "DeepSpeedJobEvicted"
)

// initializeDeepSpeedJobStatuses initializes the ReplicaStatuses for DeepSpeedJob.
func initializeDeepSpeedJobStatuses(deepspeedJob *kubeflowv1.DeepSpeedJob, mtype commonv1.ReplicaType) {
	replicaType := commonv1.ReplicaType(mtype)
	if deepspeedJob.Status.ReplicaStatuses == nil {
		deepspeedJob.Status.ReplicaStatuses = make(map[commonv1.ReplicaType]*commonv1.ReplicaStatus)
	}

	deepspeedJob.Status.ReplicaStatuses[replicaType] = &commonv1.ReplicaStatus{}
}

// updateDeepSpeedJobConditions updates the conditions of the given deepspeedJob.
func updateDeepSpeedJobConditions(deepspeedJob *kubeflowv1.DeepSpeedJob, conditionType commonv1.JobConditionType, status v1.ConditionStatus, reason, message string) bool {
	condition := newCondition(conditionType, status, reason, message)
	return setCondition(&deepspeedJob.Status, condition)
}

// newCondition creates a new deepspeedJob condition.
func newCondition(conditionType commonv1.JobConditionType, status v1.ConditionStatus, reason, message string) commonv1.JobCondition {
	return commonv1.JobCondition{
		Type:               conditionType,
		Status:             status,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// getCondition returns the condition with the provided type.
func getCondition(status commonv1.JobStatus, condType commonv1.JobConditionType) *commonv1.JobCondition {
	for _, condition := range status.Conditions {
		if condition.Type == condType {
			return &condition
		}
	}
	return nil
}

func hasCondition(status commonv1.JobStatus, condType commonv1.JobConditionType) bool {
	for _, condition := range status.Conditions {
		if condition.Type == condType && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

func isFinished(status commonv1.JobStatus) bool {
	return isSucceeded(status) || isFailed(status)
}

func isSucceeded(status commonv1.JobStatus) bool {
	return hasCondition(status, commonv1.JobSucceeded)
}

func isFailed(status commonv1.JobStatus) bool {
	return hasCondition(status, commonv1.JobFailed)
}

// setCondition updates the deepspeedJob to include the provided condition.
// If the condition that we are about to add already exists
// and has the same status and reason then we are not going to update.
func setCondition(status *commonv1.JobStatus, condition commonv1.JobCondition) bool {
	currentCond := getCondition(*status, condition.Type)

	// Do nothing if condition doesn't change
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return false
	}

	// Do not update lastTransitionTime if the status of the condition doesn't change.
	if currentCond != nil && currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}

	// Append the updated condition
	newConditions := filterOutCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
	return true
}

// filterOutCondition returns a new slice of deepspeedJob conditions without conditions with the provided type.
func filterOutCondition(conditions []commonv1.JobCondition, condType commonv1.JobConditionType) []commonv1.JobCondition {
	var newConditions []commonv1.JobCondition
	for _, c := range conditions {
		if condType == commonv1.JobRestarting && c.Type == commonv1.JobRunning {
			continue
		}
		if condType == commonv1.JobRunning && c.Type == commonv1.JobRestarting {
			continue
		}

		if c.Type == condType {
			continue
		}

		// Set the running condition status to be false when current condition failed or succeeded
		if (condType == commonv1.JobFailed || condType == commonv1.JobSucceeded) && (c.Type == commonv1.JobRunning || c.Type == commonv1.JobFailed) {
			c.Status = v1.ConditionFalse
		}

		newConditions = append(newConditions, c)
	}
	return newConditions
}

// initializeReplicaStatuses initializes the ReplicaStatuses for replica.
// originally from pkg/controller.v1/tensorflow/status.go (deleted)
func initializeReplicaStatuses(jobStatus *commonv1.JobStatus, rtype commonv1.ReplicaType) {
	if jobStatus.ReplicaStatuses == nil {
		jobStatus.ReplicaStatuses = make(map[commonv1.ReplicaType]*commonv1.ReplicaStatus)
	}

	jobStatus.ReplicaStatuses[rtype] = &commonv1.ReplicaStatus{}
}
