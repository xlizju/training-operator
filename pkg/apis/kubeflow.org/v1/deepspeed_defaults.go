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
	"k8s.io/apimachinery/pkg/runtime"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

func addDeepSpeedJobDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// setDefaultsTypeLauncher sets the default value to launcher.
func setDefaultsTypeLauncher(spec *commonv1.ReplicaSpec) {
	if spec == nil {
		return
	}
	if spec.RestartPolicy == "" {
		spec.RestartPolicy = DeepSpeedJobDefaultLauncherRestartPolicy
	}
	if spec.Replicas == nil {
		spec.Replicas = newInt32(1)
	}
}

// setDefaultsTypeWorker sets the default value to worker.
func setDefaultsTypeWorker(spec *commonv1.ReplicaSpec) {
	if spec == nil {
		return
	}
	if spec.RestartPolicy == "" {
		spec.RestartPolicy = DeepSpeedJobDefaultWorkerRestartPolicy
	}
	if spec.Replicas == nil {
		spec.Replicas = newInt32(0)
	}
}

func setDefaultsRunPolicy(policy *commonv1.RunPolicy) {
	if policy.CleanPodPolicy == nil {
		policy.CleanPodPolicy = cleanPodPolicyPointer(commonv1.CleanPodPolicyNone)
	}
	// The remaining fields are passed as-is to the k8s Job API, which does its
	// own defaulting.
}

func SetDefaults_DeepSpeedJob(deepspeedJob *DeepSpeedJob) {
	setDefaultsRunPolicy(&deepspeedJob.Spec.RunPolicy)
	if deepspeedJob.Spec.SlotsPerWorker == nil {
		deepspeedJob.Spec.SlotsPerWorker = newInt32(1)
	}
	if deepspeedJob.Spec.SSHAuthMountPath == "" {
		deepspeedJob.Spec.SSHAuthMountPath = "/root/.ssh"
	}

	// set default to Launcher
	setDefaultsTypeLauncher(deepspeedJob.Spec.DeepSpeedReplicaSpecs[DeepSpeedReplicaTypeLauncher])

	// set default to Worker
	setDefaultsTypeWorker(deepspeedJob.Spec.DeepSpeedReplicaSpecs[DeepSpeedReplicaTypeWorker])

}
