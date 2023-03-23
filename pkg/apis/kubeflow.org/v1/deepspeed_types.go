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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

const (
	// DeepSpeedJobFrameworkName is the name of the ML Framework
	DeepSpeedJobFrameworkName = "deepspeed"

	// DeepSpeedJobKind
	DeepSpeedJobKind = "DeepSpeedJob"
	// DeepSpeedJobSingular is the singular for DeepSpeedJob.
	DeepSpeedJobSingular = "deepspeedjob"
	// DeepSpeedJobPlural is the plural for DeepSpeedJob.
	DeepSpeedJobPlural    = "deepspeedjobs"
	DeepSpeedJobShortName = "dsjob"

	// DeepSpeedJobDefaultContainerName is the name of the DeepSpeedJob container.
	DeepSpeedJobDefaultContainerName = "deepspeed"
	// DeepSpeedJobDefaultPortName is name of the port used to communicate between Master and Workers.
	DeepSpeedJobDefaultPortName = "deepspeed-port"
	// DeepSpeedJobDefaultPort is default value of the port.
	DeepSpeedJobDefaultPort = 9999

	// DeepSpeedJobDefaultLauncherRestartPolicy is default RestartPolicy for Launcher Job.
	DeepSpeedJobDefaultLauncherRestartPolicy = commonv1.RestartPolicyOnFailure
	// DeepSpeedJobDefaultWorkerRestartPolicy is default RestartPolicy for Worker Job.
	DeepSpeedJobDefaultWorkerRestartPolicy = commonv1.RestartPolicyNever

	// DeepSpeedReplicaTypeLauncher is the type for launcher replica.
	DeepSpeedReplicaTypeLauncher commonv1.ReplicaType = "Launcher"

	// DeepSpeedReplicaTypeWorker is the type for worker replicas.
	DeepSpeedReplicaTypeWorker commonv1.ReplicaType = "Worker"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +resource:path=deepspeedjob
// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name="Age",type=date
// +kubebuilder:printcolumn:JSONPath=`.status.conditions[-1:].type`,name="State",type=string
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dsjob

type DeepSpeedJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DeepSpeedJobSpec   `json:"spec,omitempty"`
	Status            commonv1.JobStatus `json:"status,omitempty"`
}

type DeepSpeedJobSpec struct {

	// Specifies the number of slots per worker used in hostfile.
	// Defaults to 1.
	// +optional
	// +kubebuilder:default:=1
	SlotsPerWorker *int32 `json:"slotsPerWorker,omitempty"`

	// RunPolicy encapsulates various runtime policies of the job.
	RunPolicy commonv1.RunPolicy `json:"runPolicy,omitempty"`

	// DeepSpeedReplicaSpecs contains maps from `DeepSpeedReplicaType` to `ReplicaSpec` that
	// specify the DeepSpeed replicas to run.
	DeepSpeedReplicaSpecs map[commonv1.ReplicaType]*commonv1.ReplicaSpec `json:"deepspeedReplicaSpecs"`

	// SSHAuthMountPath is the directory where SSH keys are mounted.
	// Defaults to "/root/.ssh".
	// +kubebuilder:default:="/root/.ssh"
	SSHAuthMountPath string `json:"sshAuthMountPath,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +resource:path=deepspeedjobs
// +kubebuilder:object:root=true

type DeepSpeedJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []DeepSpeedJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DeepSpeedJob{}, &DeepSpeedJobList{})
	SchemeBuilder.SchemeBuilder.Register(addDeepSpeedJobDefaultingFuncs)
}
