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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

func TestValidateDeepSpeedJob(t *testing.T) {
	cases := map[string]struct {
		job      DeepSpeedJob
		wantErrs field.ErrorList
	}{
		"valid": {
			job: DeepSpeedJob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: DeepSpeedJobSpec{
					SlotsPerWorker: newInt32(2),
					RunPolicy: commonv1.RunPolicy{
						CleanPodPolicy: cleanPodPolicyPointer(commonv1.CleanPodPolicyRunning),
					},
					SSHAuthMountPath: "/home/deepspeeduser/.ssh",
					DeepSpeedReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
						DeepSpeedReplicaTypeLauncher: {
							Replicas:      newInt32(1),
							RestartPolicy: commonv1.RestartPolicyNever,
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{}},
								},
							},
						},
					},
				},
			},
		},
		"valid with worker": {
			job: DeepSpeedJob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: DeepSpeedJobSpec{
					SlotsPerWorker: newInt32(2),
					RunPolicy: commonv1.RunPolicy{
						CleanPodPolicy: cleanPodPolicyPointer(commonv1.CleanPodPolicyRunning),
					},
					SSHAuthMountPath: "/home/deepspeeduser/.ssh",
					DeepSpeedReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
						DeepSpeedReplicaTypeLauncher: {
							Replicas:      newInt32(1),
							RestartPolicy: commonv1.RestartPolicyOnFailure,
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{}},
								},
							},
						},
						DeepSpeedReplicaTypeWorker: {
							Replicas:      newInt32(3),
							RestartPolicy: commonv1.RestartPolicyNever,
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{}},
								},
							},
						},
					},
				},
			},
		},
		"empty job": {
			wantErrs: field.ErrorList{
				&field.Error{
					Type:  field.ErrorTypeInvalid,
					Field: "metadata.name",
				},
				&field.Error{
					Type:  field.ErrorTypeRequired,
					Field: "spec.deepspeedReplicaSpecs",
				},
				&field.Error{
					Type:  field.ErrorTypeRequired,
					Field: "spec.slotsPerWorker",
				},
				&field.Error{
					Type:  field.ErrorTypeRequired,
					Field: "spec.runPolicy.cleanPodPolicy",
				},
				&field.Error{
					Type:  field.ErrorTypeRequired,
					Field: "spec.sshAuthMountPath",
				},
			},
		},
		"invalid fields": {
			job: DeepSpeedJob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "this-name-is-waaaaaaaay-too-long-for-a-worker-hostname",
				},
				Spec: DeepSpeedJobSpec{
					SlotsPerWorker: newInt32(2),
					RunPolicy: commonv1.RunPolicy{
						CleanPodPolicy:          cleanPodPolicyPointer("unknown"),
						TTLSecondsAfterFinished: newInt32(-1),
						ActiveDeadlineSeconds:   newInt64(-1),
						BackoffLimit:            newInt32(-1),
					},
					SSHAuthMountPath: "/root/.ssh",
					DeepSpeedReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
						DeepSpeedReplicaTypeLauncher: {
							Replicas:      newInt32(1),
							RestartPolicy: commonv1.RestartPolicyNever,
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{}},
								},
							},
						},
						DeepSpeedReplicaTypeWorker: {
							Replicas:      newInt32(1000),
							RestartPolicy: commonv1.RestartPolicyNever,
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{}},
								},
							},
						},
					},
				},
			},
			wantErrs: field.ErrorList{
				{
					Type:  field.ErrorTypeInvalid,
					Field: "metadata.name",
				},
				{
					Type:  field.ErrorTypeNotSupported,
					Field: "spec.runPolicy.cleanPodPolicy",
				},
				{
					Type:  field.ErrorTypeInvalid,
					Field: "spec.runPolicy.ttlSecondsAfterFinished",
				},
				{
					Type:  field.ErrorTypeInvalid,
					Field: "spec.runPolicy.activeDeadlineSeconds",
				},
				{
					Type:  field.ErrorTypeInvalid,
					Field: "spec.runPolicy.backoffLimit",
				},
			},
		},
		"empty replica specs": {
			job: DeepSpeedJob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: DeepSpeedJobSpec{
					SlotsPerWorker: newInt32(2),
					RunPolicy: commonv1.RunPolicy{
						CleanPodPolicy: cleanPodPolicyPointer(commonv1.CleanPodPolicyRunning),
					},
					SSHAuthMountPath:      "/root/.ssh",
					DeepSpeedReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{},
				},
			},
			wantErrs: field.ErrorList{
				&field.Error{
					Type:  field.ErrorTypeRequired,
					Field: "spec.deepspeedReplicaSpecs[Launcher]",
				},
			},
		},
		"missing replica spec fields": {
			job: DeepSpeedJob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: DeepSpeedJobSpec{
					SlotsPerWorker: newInt32(2),
					RunPolicy: commonv1.RunPolicy{
						CleanPodPolicy: cleanPodPolicyPointer(commonv1.CleanPodPolicyRunning),
					},
					SSHAuthMountPath: "/root/.ssh",
					DeepSpeedReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
						DeepSpeedReplicaTypeLauncher: {},
						DeepSpeedReplicaTypeWorker:   {},
					},
				},
			},
			wantErrs: field.ErrorList{
				{
					Type:  field.ErrorTypeRequired,
					Field: "spec.deepspeedReplicaSpecs[Launcher].replicas",
				},
				{
					Type:  field.ErrorTypeNotSupported,
					Field: "spec.deepspeedReplicaSpecs[Launcher].restartPolicy",
				},
				{
					Type:  field.ErrorTypeRequired,
					Field: "spec.deepspeedReplicaSpecs[Launcher].template.spec.containers",
				},
				{
					Type:  field.ErrorTypeRequired,
					Field: "spec.deepspeedReplicaSpecs[Worker].replicas",
				},
				{
					Type:  field.ErrorTypeNotSupported,
					Field: "spec.deepspeedReplicaSpecs[Worker].restartPolicy",
				},
				{
					Type:  field.ErrorTypeRequired,
					Field: "spec.deepspeedReplicaSpecs[Worker].template.spec.containers",
				},
			},
		},
		"invalid replica fields": {
			job: DeepSpeedJob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Spec: DeepSpeedJobSpec{
					SlotsPerWorker: newInt32(2),
					RunPolicy: commonv1.RunPolicy{
						CleanPodPolicy: cleanPodPolicyPointer(commonv1.CleanPodPolicyRunning),
					},
					SSHAuthMountPath: "/root/.ssh",
					DeepSpeedReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
						DeepSpeedReplicaTypeLauncher: {
							Replicas:      newInt32(2),
							RestartPolicy: commonv1.RestartPolicyAlways,
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{}},
								},
							},
						},
						DeepSpeedReplicaTypeWorker: {
							Replicas:      newInt32(0),
							RestartPolicy: "Invalid",
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{}},
								},
							},
						},
					},
				},
			},
			wantErrs: field.ErrorList{
				{
					Type:  field.ErrorTypeNotSupported,
					Field: "spec.deepspeedReplicaSpecs[Launcher].restartPolicy",
				},
				{
					Type:  field.ErrorTypeInvalid,
					Field: "spec.deepspeedReplicaSpecs[Launcher].replicas",
				},
				{
					Type:  field.ErrorTypeNotSupported,
					Field: "spec.deepspeedReplicaSpecs[Worker].restartPolicy",
				},
				{
					Type:  field.ErrorTypeInvalid,
					Field: "spec.deepspeedReplicaSpecs[Worker].replicas",
				},
			},
		},
		"invalid deepspeedJob name": {
			job: DeepSpeedJob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "1-foo",
				},
				Spec: DeepSpeedJobSpec{
					SlotsPerWorker: newInt32(2),
					RunPolicy: commonv1.RunPolicy{
						CleanPodPolicy: cleanPodPolicyPointer(commonv1.CleanPodPolicyRunning),
					},
					SSHAuthMountPath: "/home/deepspeeduser/.ssh",
					DeepSpeedReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
						DeepSpeedReplicaTypeLauncher: {
							Replicas:      newInt32(1),
							RestartPolicy: commonv1.RestartPolicyNever,
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{}},
								},
							},
						},
					},
				},
			},
			wantErrs: field.ErrorList{{
				Type:  field.ErrorTypeInvalid,
				Field: "metadata.name",
			}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := ValidateDeepSpeedJob(&tc.job)
			if diff := cmp.Diff(tc.wantErrs, got, cmpopts.IgnoreFields(field.Error{}, "Detail", "BadValue")); diff != "" {
				t.Errorf("Unexpected errors (-want,+got):\n%s", diff)
			}
		})
	}
}
