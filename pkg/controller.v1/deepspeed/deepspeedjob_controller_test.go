// Copyright 2021 The Kubeflow Authors.
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
	"time"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"

	kubeflowv1 "github.com/kubeflow/training-operator/pkg/apis/kubeflow.org/v1"
)

const (
	gpuResourceName = "nvidia.com/gpu"
)

func newLauncherPod(job *batchv1.Job, startTime *metav1.Time) *corev1.Pod {
	labels := map[string]string{}
	for k, v := range job.Labels {
		labels[k] = v
	}
	labels["controller-uid"] = string(job.UID)
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name + rand.String(5),
			Namespace: metav1.NamespaceDefault,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, kubeflowv1.SchemeGroupVersion.WithKind("Job")),
			},
		},
		Spec: job.Spec.Template.Spec,
	}
	if startTime != nil {
		pod.Status.StartTime = startTime
	}
	return pod
}

func newDeepSpeedJobCommon(name string, startTime, completionTime *metav1.Time) *kubeflowv1.DeepSpeedJob {
	cleanPodPolicyAll := commonv1.CleanPodPolicyAll
	deepspeedJob := &kubeflowv1.DeepSpeedJob{
		TypeMeta: metav1.TypeMeta{APIVersion: kubeflowv1.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: kubeflowv1.DeepSpeedJobSpec{
			RunPolicy: commonv1.RunPolicy{
				CleanPodPolicy: &cleanPodPolicyAll,
			},
			DeepSpeedReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
				kubeflowv1.DeepSpeedReplicaTypeWorker: {
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "foo",
									Image: "bar",
								},
							},
						},
					},
				},
				kubeflowv1.DeepSpeedReplicaTypeLauncher: {
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "foo",
									Image: "bar",
								},
							},
						},
					},
				},
			},
		},
		Status: commonv1.JobStatus{},
	}

	if startTime != nil {
		deepspeedJob.Status.StartTime = startTime
	}
	if completionTime != nil {
		deepspeedJob.Status.CompletionTime = completionTime
	}

	return deepspeedJob
}

func newDeepSpeedJobOld(name string, replicas *int32, pusPerReplica int64, resourceName string, startTime, completionTime *metav1.Time) *kubeflowv1.DeepSpeedJob {
	deepspeedJob := newDeepSpeedJobCommon(name, startTime, completionTime)

	deepspeedJob.Spec.DeepSpeedReplicaSpecs[kubeflowv1.DeepSpeedReplicaTypeWorker].Replicas = replicas

	workerContainers := deepspeedJob.Spec.DeepSpeedReplicaSpecs[kubeflowv1.DeepSpeedReplicaTypeWorker].Template.Spec.Containers
	for i := range workerContainers {
		container := &workerContainers[i]
		container.Resources = corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceName(resourceName): *resource.NewQuantity(pusPerReplica, resource.DecimalExponent),
			},
		}
	}

	return deepspeedJob
}

var newDeepspeedJob = newDeepspeedJobWithLauncher

func newDeepspeedJobWithLauncher(name string, replicas *int32, pusPerReplica int64, resourceName string, startTime, completionTime *metav1.Time) *kubeflowv1.DeepSpeedJob {
	deepspeedJob := newDeepSpeedJobOld(name, replicas, pusPerReplica, resourceName, startTime, completionTime)

	deepspeedJob.Spec.DeepSpeedReplicaSpecs[kubeflowv1.DeepSpeedReplicaTypeLauncher].Replicas = pointer.Int32(1)

	launcherContainers := deepspeedJob.Spec.DeepSpeedReplicaSpecs[kubeflowv1.DeepSpeedReplicaTypeLauncher].Template.Spec.Containers
	for i := range launcherContainers {
		container := &launcherContainers[i]
		container.Resources = corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceName(resourceName): *resource.NewQuantity(pusPerReplica, resource.DecimalExponent),
			},
		}
	}

	return deepspeedJob
}

var _ = Describe("DeepSpeedJob controller", func() {
	// Define utility constants for object names and testing timeouts/durations and intervals.
	const (
		timeout  = 10 * time.Second
		interval = 1000 * time.Millisecond
	)

	Context("Test DeepSpeedJob with succeeded launcher Pod", func() {
		It("Should contains desired launcher ReplicaStatus", func() {
			By("By marking a launcher pod with Phase Succeeded")
			ctx := context.Background()
			startTime := metav1.Now()
			completionTime := metav1.Now()

			jobName := "test-launcher-succeeded"

			deepspeedJob := newDeepspeedJobWithLauncher(jobName, pointer.Int32(64), 1, gpuResourceName, &startTime, &completionTime)
			Expect(testK8sClient.Create(ctx, deepspeedJob)).Should(Succeed())

			launcher := reconciler.newLauncherJob(deepspeedJob)
			launcherKey := types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      launcher.GetName(),
			}
			Eventually(func() error {
				launcherCreated := &batchv1.Job{}
				if err := testK8sClient.Get(ctx, launcherKey, launcherCreated); err != nil {
					return err
				}
				launcherCreated.Status.Conditions = append(launcherCreated.Status.Conditions, batchv1.JobCondition{
					Type:   batchv1.JobComplete,
					Status: corev1.ConditionTrue,
				})
				return testK8sClient.Status().Update(ctx, launcherCreated)
			}, timeout, interval).Should(BeNil())

			created := &kubeflowv1.DeepSpeedJob{}
			launcherStatus := &commonv1.ReplicaStatus{
				Active:    0,
				Succeeded: 1,
				Failed:    0,
			}
			Eventually(func() bool {
				err := testK8sClient.Get(ctx, types.NamespacedName{Namespace: metav1.NamespaceDefault, Name: jobName}, created)
				if err != nil {
					return false
				}
				return ReplicaStatusMatch(created.Status.ReplicaStatuses, kubeflowv1.DeepSpeedReplicaTypeLauncher, launcherStatus)
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("Test DeepSpeedJob with failed launcher Pod", func() {
		It("Should contains desired launcher ReplicaStatus", func() {
			By("By marking a launcher pod with Phase Failed")
			ctx := context.Background()
			startTime := metav1.Now()
			completionTime := metav1.Now()

			jobName := "test-launcher-failed"

			deepspeedJob := newDeepspeedJobWithLauncher(jobName, pointer.Int32(64), 1, gpuResourceName, &startTime, &completionTime)
			Expect(testK8sClient.Create(ctx, deepspeedJob)).Should(Succeed())

			launcher := reconciler.newLauncherJob(deepspeedJob)
			launcherKey := types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      launcher.GetName(),
			}
			Eventually(func() error {
				launcherCreated := &batchv1.Job{}
				if err := testK8sClient.Get(ctx, launcherKey, launcherCreated); err != nil {
					return err
				}
				launcherCreated.Status.Conditions = append(launcherCreated.Status.Conditions, batchv1.JobCondition{
					Type:   batchv1.JobFailed,
					Status: corev1.ConditionTrue,
				})
				launcherCreated.Status.Failed = *pointer.Int32(1)
				return testK8sClient.Status().Update(ctx, launcherCreated)
			}, timeout, interval).Should(BeNil())

			launcherStatus := &commonv1.ReplicaStatus{
				Active:    0,
				Succeeded: 0,
				Failed:    1,
			}
			created := &kubeflowv1.DeepSpeedJob{}
			Eventually(func() bool {
				err := testK8sClient.Get(ctx, types.NamespacedName{Namespace: metav1.NamespaceDefault, Name: jobName}, created)
				if err != nil {
					return false
				}
				return ReplicaStatusMatch(created.Status.ReplicaStatuses, kubeflowv1.DeepSpeedReplicaTypeLauncher, launcherStatus)
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("Test DeepSpeedJob with succeeded launcher pod", func() {
		It("Should contain desired ReplicaStatuses for worker", func() {
			By("By marking the launcher Pod as Succeeded")
			ctx := context.Background()
			startTime := metav1.Now()
			completionTime := metav1.Now()

			jobName := "test-launcher-succeeded2"

			deepspeedJob := newDeepspeedJobWithLauncher(jobName, pointer.Int32(64), 1, gpuResourceName, &startTime, &completionTime)
			Expect(testK8sClient.Create(ctx, deepspeedJob)).Should(Succeed())

			launcher := reconciler.newLauncherJob(deepspeedJob)
			launcher.Status.Conditions = append(launcher.Status.Conditions, batchv1.JobCondition{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			})
			launcherKey := types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      launcher.GetName(),
			}
			Eventually(func() error {
				launcherCreated := &batchv1.Job{}
				if err := testK8sClient.Get(ctx, launcherKey, launcherCreated); err != nil {
					return err
				}
				launcherCreated.Status.Conditions = append(launcherCreated.Status.Conditions, batchv1.JobCondition{
					Type:   batchv1.JobComplete,
					Status: corev1.ConditionTrue,
				})
				return testK8sClient.Status().Update(ctx, launcherCreated)
			}, timeout, interval).Should(BeNil())

			created := &kubeflowv1.DeepSpeedJob{}
			launcherStatus := &commonv1.ReplicaStatus{
				Active:    0,
				Succeeded: 0,
				Failed:    0,
			}
			Eventually(func() bool {
				err := testK8sClient.Get(ctx, types.NamespacedName{Namespace: metav1.NamespaceDefault, Name: jobName}, created)
				if err != nil {
					return false
				}
				return ReplicaStatusMatch(created.Status.ReplicaStatuses, kubeflowv1.DeepSpeedReplicaTypeWorker, launcherStatus)
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("Test DeepSpeedJob with Running launcher Pod and Pending worker Pods", func() {
		It("Should contain desired ReplicaStatuses", func() {
			By("By marking an active launcher pod and pending worker pods")

			ctx := context.Background()
			startTime := metav1.Now()
			completionTime := metav1.Now()

			jobName := "test-launcher-running-worker-pending"

			var replicas int32 = 8
			deepspeedJob := newDeepspeedJobWithLauncher(jobName, &replicas, 1, gpuResourceName, &startTime, &completionTime)
			Expect(testK8sClient.Create(ctx, deepspeedJob)).Should(Succeed())

			launcher := reconciler.newLauncherJob(deepspeedJob)
			launcherKey := types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      launcher.GetName(),
			}

			// Create the launcher job related pod
			Eventually(func() error {
				launcherJobCreated := &batchv1.Job{}
				if err := testK8sClient.Get(ctx, launcherKey, launcherJobCreated); err != nil {
					return err
				}
				launcherPod := newLauncherPod(launcherJobCreated, &startTime)
				Expect(testK8sClient.Create(ctx, launcherPod)).Should(Succeed())
				return nil
			}, timeout, interval).Should(BeNil())

			Eventually(func() error {
				launcherCreated := &batchv1.Job{}
				if err := testK8sClient.Get(ctx, launcherKey, launcherCreated); err != nil {
					return err
				}
				if pods, err := reconciler.jobPods(launcherCreated); err != nil {
					return err
				} else {
					for _, pod := range pods {
						Eventually(func() error {
							launcherPodCreated := &corev1.Pod{}
							launcherPodKey := types.NamespacedName{
								Namespace: metav1.NamespaceDefault,
								Name:      pod.GetName(),
							}
							if err := testK8sClient.Get(ctx, launcherPodKey, launcherPodCreated); err != nil {
								return err
							}
							launcherPodCreated.Status.Phase = corev1.PodRunning
							return testK8sClient.Status().Update(ctx, launcherPodCreated)
						}, timeout, interval).Should(BeNil())
					}
				}
				return nil
			}, timeout, interval).Should(BeNil())

			// Update the job status
			Eventually(func() error {
				launcherCreated := &batchv1.Job{}
				if err := testK8sClient.Get(ctx, launcherKey, launcherCreated); err != nil {
					return err
				}
				launcherCreated.Status.Active = int32(1)
				launcher.Status.StartTime = &startTime
				return testK8sClient.Status().Update(ctx, launcherCreated)
			}, timeout, interval).Should(BeNil())

			for i := 0; i < int(replicas); i++ {
				worker := reconciler.newWorker(deepspeedJob, i)
				workerKey := types.NamespacedName{
					Namespace: metav1.NamespaceDefault,
					Name:      worker.GetName(),
				}
				Eventually(func() error {
					workerCreated := &corev1.Pod{}
					if err := testK8sClient.Get(ctx, workerKey, workerCreated); err != nil {
						return err
					}
					workerCreated.Status.Phase = corev1.PodPending
					return testK8sClient.Status().Update(ctx, workerCreated)
				}, timeout, interval).Should(BeNil())
			}

			key := types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      jobName,
			}
			launcherStatus := &commonv1.ReplicaStatus{
				Active:    1,
				Succeeded: 0,
				Failed:    0,
			}
			workerStatus := &commonv1.ReplicaStatus{
				Active:    0,
				Succeeded: 0,
				Failed:    0,
			}
			Eventually(func() bool {
				created := &kubeflowv1.DeepSpeedJob{}
				err := testK8sClient.Get(ctx, key, created)
				if err != nil {
					return false
				}
				fmt.Println(created.Status.ReplicaStatuses)
				return ReplicaStatusMatch(created.Status.ReplicaStatuses, kubeflowv1.DeepSpeedReplicaTypeLauncher,
					launcherStatus) && ReplicaStatusMatch(created.Status.ReplicaStatuses, kubeflowv1.DeepSpeedReplicaTypeWorker,
					workerStatus)
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("Test DeepSpeedJob with Running launcher Pod and Running worker Pods", func() {
		It("Should contain desired ReplicaStatuses", func() {
			By("By creating an active launcher pod and active worker pods")

			ctx := context.Background()
			startTime := metav1.Now()
			completionTime := metav1.Now()

			jobName := "test-launcher-running-worker-running"

			var replicas int32 = 8
			deepspeedJob := newDeepspeedJob(jobName, &replicas, 1, gpuResourceName, &startTime, &completionTime)
			Expect(testK8sClient.Create(ctx, deepspeedJob)).Should(Succeed())

			launcher := reconciler.newLauncherJob(deepspeedJob)
			launcherKey := types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      launcher.GetName(),
			}

			// Create the launcher job related pod
			Eventually(func() error {
				launcherJobCreated := &batchv1.Job{}
				if err := testK8sClient.Get(ctx, launcherKey, launcherJobCreated); err != nil {
					return err
				}
				launcherPod := newLauncherPod(launcherJobCreated, &startTime)
				Expect(testK8sClient.Create(ctx, launcherPod)).Should(Succeed())
				return nil
			}, timeout, interval).Should(BeNil())

			Eventually(func() error {
				launcherCreated := &batchv1.Job{}
				if err := testK8sClient.Get(ctx, launcherKey, launcherCreated); err != nil {
					return err
				}
				if pods, err := reconciler.jobPods(launcherCreated); err != nil {
					return err
				} else {
					for _, pod := range pods {
						Eventually(func() error {
							launcherPodCreated := &corev1.Pod{}
							launcherPodKey := types.NamespacedName{
								Namespace: metav1.NamespaceDefault,
								Name:      pod.GetName(),
							}
							if err := testK8sClient.Get(ctx, launcherPodKey, launcherPodCreated); err != nil {
								return err
							}
							launcherPodCreated.Status.Phase = corev1.PodRunning
							return testK8sClient.Status().Update(ctx, launcherPodCreated)
						}, timeout, interval).Should(BeNil())
					}
				}
				return nil
			}, timeout, interval).Should(BeNil())

			// Update the job status
			Eventually(func() error {
				launcherCreated := &batchv1.Job{}
				if err := testK8sClient.Get(ctx, launcherKey, launcherCreated); err != nil {
					return err
				}
				launcherCreated.Status.Active = int32(1)
				launcher.Status.StartTime = &startTime
				return testK8sClient.Status().Update(ctx, launcherCreated)
			}, timeout, interval).Should(BeNil())

			for i := 0; i < int(replicas); i++ {
				worker := reconciler.newWorker(deepspeedJob, i)
				workerKey := types.NamespacedName{
					Namespace: metav1.NamespaceDefault,
					Name:      worker.GetName(),
				}
				Eventually(func() error {
					workerCreated := &corev1.Pod{}
					if err := testK8sClient.Get(ctx, workerKey, workerCreated); err != nil {
						return err
					}
					workerCreated.Status.Phase = corev1.PodRunning
					return testK8sClient.Status().Update(ctx, workerCreated)
				}, timeout, interval).Should(BeNil())
			}

			key := types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      jobName,
			}
			launcherStatus := &commonv1.ReplicaStatus{
				Active:    1,
				Succeeded: 0,
				Failed:    0,
			}
			workerStatus := &commonv1.ReplicaStatus{
				Active:    8,
				Succeeded: 0,
				Failed:    0,
			}
			Eventually(func() bool {
				created := &kubeflowv1.DeepSpeedJob{}
				err := testK8sClient.Get(ctx, key, created)
				if err != nil {
					return false
				}
				return ReplicaStatusMatch(created.Status.ReplicaStatuses, kubeflowv1.DeepSpeedReplicaTypeLauncher,
					launcherStatus) && ReplicaStatusMatch(created.Status.ReplicaStatuses, kubeflowv1.DeepSpeedReplicaTypeWorker,
					workerStatus)
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("Test DeepSpeedJob with Running worker Pods", func() {
		It("Should contain desired ReplicaStatuses and create a launcher pod", func() {
			By("By creating only active worker pods")

			ctx := context.Background()
			startTime := metav1.Now()
			completionTime := metav1.Now()

			jobName := "test-worker-running"

			var replicas int32 = 16
			deepspeedJob := newDeepspeedJob(jobName, &replicas, 1, gpuResourceName, &startTime, &completionTime)
			Expect(testK8sClient.Create(ctx, deepspeedJob)).Should(Succeed())

			for i := 0; i < int(replicas); i++ {
				worker := reconciler.newWorker(deepspeedJob, i)
				workerKey := types.NamespacedName{
					Namespace: metav1.NamespaceDefault,
					Name:      worker.GetName(),
				}
				Eventually(func() error {
					workerCreated := &corev1.Pod{}
					if err := testK8sClient.Get(ctx, workerKey, workerCreated); err != nil {
						return err
					}
					workerCreated.Status.Phase = corev1.PodRunning
					return testK8sClient.Status().Update(ctx, workerCreated)
				}, timeout, interval).Should(BeNil())
			}

			launcherKey := types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      deepspeedJob.Name + launcherSuffix,
			}
			launcher := &kubeflowv1.DeepSpeedJob{}
			Eventually(func() bool {
				err := testK8sClient.Get(ctx, launcherKey, launcher)
				return err != nil
			}, timeout, interval).Should(BeTrue())

			key := types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      jobName,
			}
			launcherStatus := &commonv1.ReplicaStatus{
				Active:    0,
				Succeeded: 0,
				Failed:    0,
			}
			workerStatus := &commonv1.ReplicaStatus{
				Active:    16,
				Succeeded: 0,
				Failed:    0,
			}
			Eventually(func() bool {
				created := &kubeflowv1.DeepSpeedJob{}
				err := testK8sClient.Get(ctx, key, created)
				if err != nil {
					return false
				}
				return ReplicaStatusMatch(created.Status.ReplicaStatuses, kubeflowv1.DeepSpeedReplicaTypeLauncher,
					launcherStatus) && ReplicaStatusMatch(created.Status.ReplicaStatuses, kubeflowv1.DeepSpeedReplicaTypeWorker,
					workerStatus)
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("DeepSpeedJob not found", func() {
		It("Should do nothing", func() {
			By("Calling Reconcile method")
			jobName := "test-not-exist"

			ctx := context.Background()

			req := ctrl.Request{NamespacedName: types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      jobName,
			}}
			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).Should(BeNil())
		})
	})

	Context("DeepSpeedJob with launcher Job not controlled by itself", func() {
		It("Should return error", func() {
			By("Calling Reconcile method")
			jobName := "test-launcher-orphan"
			testKind := "Job"

			ctx := context.Background()
			startTime := metav1.Now()
			completionTime := metav1.Now()

			deepspeedJob := newDeepspeedJob(jobName, pointer.Int32(64), 1, gpuResourceName, &startTime, &completionTime)
			launcher := reconciler.newLauncherJob(deepspeedJob)
			launcher.Spec.Template.Spec.RestartPolicy = "Never"
			launcher.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath = "/deepspeed/json"
			launcher.OwnerReferences = nil
			Expect(testK8sClient.Create(ctx, launcher)).Should(Succeed())

			Expect(testK8sClient.Create(ctx, deepspeedJob)).Should(Succeed())

			req := ctrl.Request{NamespacedName: types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      deepspeedJob.GetName(),
			}}
			expectedErr := fmt.Errorf(MessageResourceExists, launcher.Name, testKind)
			Eventually(func() error {
				_, err := reconciler.Reconcile(ctx, req)
				return err
			}, timeout, interval).Should(MatchError(expectedErr))
		})
	})

	Context("DeepSpeedJob with worker Pod not controlled by itself", func() {
		It("Should return error", func() {
			By("Calling Reconcile method")
			jobName := "test-worker-orphan"
			testKind := "Pod"

			ctx := context.Background()
			startTime := metav1.Now()
			completionTime := metav1.Now()

			deepspeedJob := newDeepspeedJob(jobName, pointer.Int32(1), 1, gpuResourceName, &startTime, &completionTime)

			for i := 0; i < 1; i++ {
				worker := reconciler.newWorker(deepspeedJob, i)
				worker.Spec.Containers[0].VolumeMounts[0].MountPath = "/deepspeed/json"
				worker.OwnerReferences = nil
				Expect(testK8sClient.Create(ctx, worker)).Should(Succeed())
			}

			Expect(testK8sClient.Create(ctx, deepspeedJob)).Should(Succeed())

			req := ctrl.Request{NamespacedName: types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      deepspeedJob.GetName(),
			}}
			expectedErr := fmt.Errorf(MessageResourceExists, fmt.Sprintf("%s-%d", deepspeedJob.Name+workerSuffix, 0), testKind)
			Eventually(func() error {
				_, err := reconciler.Reconcile(ctx, req)
				return err
			}, timeout, interval).Should(MatchError(expectedErr))
		})
	})

	Context("DeepSpeedJob with ConfigMap not controlled by itself", func() {
		It("Should return error", func() {
			By("Calling Reconcile method")
			jobName := "test-cm-orphan"
			testKind := "ConfigMap"

			ctx := context.Background()
			startTime := metav1.Now()
			completionTime := metav1.Now()

			deepspeedJob := newDeepspeedJob(jobName, pointer.Int32(64), 1, gpuResourceName, &startTime, &completionTime)

			cm := newConfigMap(deepspeedJob, 64)
			cm.OwnerReferences = nil
			Expect(testK8sClient.Create(ctx, cm)).Should(Succeed())

			Expect(testK8sClient.Create(ctx, deepspeedJob)).Should(Succeed())

			req := ctrl.Request{NamespacedName: types.NamespacedName{
				Namespace: metav1.NamespaceDefault,
				Name:      deepspeedJob.GetName(),
			}}
			expectedErr := fmt.Errorf(MessageResourceExists, cm.Name, testKind)
			Eventually(func() error {
				_, err := reconciler.Reconcile(ctx, req)
				return err
			}, timeout, interval).Should(MatchError(expectedErr))
		})
	})

})

func ReplicaStatusMatch(replicaStatuses map[commonv1.ReplicaType]*commonv1.ReplicaStatus,
	replicaType commonv1.ReplicaType, status *commonv1.ReplicaStatus) bool {

	result := true

	if replicaStatuses == nil {
		return false
	}
	if val, exist := replicaStatuses[replicaType]; !exist {
		return false
	} else {
		result = result && (val.Active == status.Active)
		result = result && (val.Succeeded == status.Succeeded)
		result = result && (val.Failed == status.Failed)
	}

	return result
}
