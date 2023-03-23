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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
	schedulerpluginsv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"github.com/kubeflow/common/pkg/controller.v1/common"
	"github.com/kubeflow/common/pkg/controller.v1/control"
	"github.com/kubeflow/common/pkg/controller.v1/expectation"
	commonutil "github.com/kubeflow/common/pkg/util"
	kubeflowv1 "github.com/kubeflow/training-operator/pkg/apis/kubeflow.org/v1"
	trainingoperatorcommon "github.com/kubeflow/training-operator/pkg/common"
	"github.com/kubeflow/training-operator/pkg/common/util"
)

const (
	controllerAgentName     = "deepspeedjob-controller"
	configSuffix            = "-config"
	configVolumeName        = "deepspeed-job-config"
	configMountPath         = "/etc/deepspeed"
	hostfileName            = "hostfile"
	discoverHostsScriptName = "discover_hosts.sh"
	sshAuthSecretSuffix     = "-ssh"
	sshAuthVolume           = "ssh-auth"
	rootSSHPath             = "/root/.ssh"
	launcher                = "launcher"
	worker                  = "worker"
	launcherSuffix          = "-launcher"
	workerSuffix            = "-worker"
	sshPublicKey            = "ssh-publickey"
	sshPrivateKeyFile       = "id_rsa"
	sshPublicKeyFile        = sshPrivateKeyFile + ".pub"
	sshAuthorizedKeysFile   = "authorized_keys"
)

const (
	FailedDeleteJobReason     = "FailedDeleteJob"
	SuccessfulDeleteJobReason = "SuccessfulDeleteJob"

	// ErrResourceExists is used as part of the Event 'reason' when an DeepSpeedJob
	// fails to sync due to dependent resources of the same name already
	// existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to dependent resources already existing.
	MessageResourceExists = "Resource %q of Kind %q already exists and is not managed by DeepSpeedJob"

	// ValidationError is used as part of the Event 'reason' when failed to
	// validate an DeepSpeedJob.
	ValidationError = "ValidationError"

	// podTemplateRestartPolicyReason is the warning reason when the restart
	// policy is set in pod template.
	podTemplateRestartPolicyReason = "SetPodTemplateRestartPolicy"

	// jobBackoffLimitExceededReason is the reason that the k8s job controller
	// uses when the backoff limit is exceeded.
	jobBackoffLimitExceededReason = "BackoffLimitExceeded"
)

var (
	sshVolumeItems = []corev1.KeyToPath{
		{
			Key:  corev1.SSHAuthPrivateKey,
			Path: sshPrivateKeyFile,
			Mode: newInt32(0600),
		},
		{
			Key:  sshPublicKey,
			Path: sshPublicKeyFile,
			Mode: newInt32(0644),
		},
		{
			Key:  sshPublicKey,
			Path: sshAuthorizedKeysFile,
			Mode: newInt32(0644),
		},
	}
	configVolumeItems = []corev1.KeyToPath{
		{
			Key:  hostfileName,
			Path: hostfileName,
			Mode: newInt32(0444),
		},
		{
			Key:  discoverHostsScriptName,
			Path: discoverHostsScriptName,
			Mode: newInt32(0555),
		},
	}

	launcherEnvVars = []corev1.EnvVar{
		{
			Name:  "K_DEEPSPEED_JOB_ROLE",
			Value: launcher,
		},
	}
	workerEnvVars = []corev1.EnvVar{
		{
			Name:  "K_DEEPSPEED_JOB_ROLE",
			Value: worker,
		},
	}
	nvidiaDisableEnvVars = []corev1.EnvVar{
		{Name: "NVIDIA_VISIBLE_DEVICES"},
		{Name: "NVIDIA_DRIVER_CAPABILITIES"},
	}
)

func NewReconciler(mgr manager.Manager, gangSchedulingSetupFunc common.GangSchedulingSetupFunc) *DeepSpeedJobReconciler {
	r := &DeepSpeedJobReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		recorder:  mgr.GetEventRecorderFor(controllerAgentName),
		apiReader: mgr.GetAPIReader(),
		Log:       log.Log,
	}

	cfg := mgr.GetConfig()
	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)
	sharedInformers := informers.NewSharedInformerFactory(kubeClientSet, 0)
	priorityClassInformer := sharedInformers.Scheduling().V1().PriorityClasses()

	r.JobController = common.JobController{
		Controller:                  r,
		Expectations:                expectation.NewControllerExpectations(),
		WorkQueue:                   &util.FakeWorkQueue{},
		Recorder:                    r.recorder,
		KubeClientSet:               kubeClientSet,
		PriorityClassLister:         priorityClassInformer.Lister(),
		PriorityClassInformerSynced: priorityClassInformer.Informer().HasSynced,
		PodControl:                  control.RealPodControl{KubeClient: kubeClientSet, Recorder: r.recorder},
		ServiceControl:              control.RealServiceControl{KubeClient: kubeClientSet, Recorder: r.recorder},
	}

	gangSchedulingSetupFunc(&r.JobController)

	return r
}

// DeepSpeedJobReconciler reconciles a DeepSpeedJob object
type DeepSpeedJobReconciler struct {
	common.JobController
	client.Client
	Scheme    *runtime.Scheme
	recorder  record.EventRecorder
	apiReader client.Reader
	Log       logr.Logger
}

// region implement ControllerInterface

func (jc *DeepSpeedJobReconciler) ControllerName() string {
	return controllerAgentName
}

func (jc *DeepSpeedJobReconciler) DeleteJob(job interface{}) error {
	deepspeedJob, ok := job.(*kubeflowv1.DeepSpeedJob)
	if !ok {
		return fmt.Errorf("%v is not a type of DeepSpeedJob", deepspeedJob)
	}

	log := commonutil.LoggerForJob(deepspeedJob)
	if err := jc.Delete(context.Background(), deepspeedJob); err != nil {
		jc.Recorder.Eventf(deepspeedJob, corev1.EventTypeWarning, FailedDeleteJobReason, "Error deleting: %v", err)
		log.Errorf("failed to delete job %s/%s, %v", deepspeedJob.Namespace, deepspeedJob.Name, err)
		return err
	}

	jc.Recorder.Eventf(deepspeedJob, corev1.EventTypeNormal, SuccessfulDeleteJobReason, "Deleted job: %v", deepspeedJob.Name)
	log.Infof("job %s/%s has been deleted", deepspeedJob.Namespace, deepspeedJob.Name)
	trainingoperatorcommon.DeletedJobsCounterInc(deepspeedJob.Namespace, kubeflowv1.DeepSpeedJobFrameworkName)
	return nil
}

func (jc *DeepSpeedJobReconciler) GetAPIGroupVersionKind() schema.GroupVersionKind {
	return kubeflowv1.GroupVersion.WithKind(kubeflowv1.DeepSpeedJobKind)
}

func (jc *DeepSpeedJobReconciler) GetAPIGroupVersion() schema.GroupVersion {
	return kubeflowv1.GroupVersion
}

func (jc *DeepSpeedJobReconciler) GetDefaultContainerName() string {
	return kubeflowv1.DeepSpeedJobDefaultContainerName
}

func (jc *DeepSpeedJobReconciler) GetDefaultContainerPortName() string {
	return kubeflowv1.DeepSpeedJobDefaultPortName
}

func (jc *DeepSpeedJobReconciler) GetGroupNameLabelValue() string {
	return kubeflowv1.GroupVersion.Group
}

func (jc *DeepSpeedJobReconciler) GetJobFromAPIClient(namespace, name string) (metav1.Object, error) {
	job := &kubeflowv1.DeepSpeedJob{}
	err := jc.apiReader.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, job)
	if err != nil {
		if errors.IsNotFound(err) {
			logrus.Error(err, "DeepSpeedJob not found", "namespace", namespace, "name", name)
		} else {
			logrus.Error(err, "failed to get job from api-server", "namespace", namespace, "name", name)
		}
		return nil, err
	}
	return job, nil
}

func (jc *DeepSpeedJobReconciler) GetJobFromInformerCache(namespace, name string) (metav1.Object, error) {
	job := &kubeflowv1.DeepSpeedJob{}
	err := jc.Get(context.Background(), types.NamespacedName{
		Namespace: namespace, Name: name,
	}, job)
	return job, err
}

func (jc *DeepSpeedJobReconciler) IsMasterRole(replicas map[commonv1.ReplicaType]*commonv1.ReplicaSpec,
	rtype commonv1.ReplicaType, index int) bool {
	return string(rtype) == string(kubeflowv1.DeepSpeedReplicaTypeLauncher)
}

// SetClusterSpec is overridden because no cluster spec is needed for DeepSpeedJob
func (jc *DeepSpeedJobReconciler) SetClusterSpec(job interface{}, podTemplate *corev1.PodTemplateSpec, rtype, index string) error {
	return nil
}

func (jc *DeepSpeedJobReconciler) UpdateJobStatus(job interface{}, replicas map[commonv1.ReplicaType]*commonv1.ReplicaSpec, jobStatus *commonv1.JobStatus) error {
	deepspeedJob, ok := job.(*kubeflowv1.DeepSpeedJob)
	if !ok {
		return fmt.Errorf("%+v is not a type of DeepSpeedJob", job)
	}

	for rtype, spec := range replicas {
		status := jobStatus.ReplicaStatuses[rtype]

		succeeded := status.Succeeded
		expected := *(spec.Replicas) - succeeded
		running := status.Active
		failed := status.Failed

		logrus.Infof("DeepSpeedJob=%s, ReplicaType=%s expected=%d, running=%d, succeeded=%d , failed=%d",
			deepspeedJob.Name, rtype, expected, running, succeeded, failed)

		if rtype == kubeflowv1.DeepSpeedReplicaTypeLauncher {
			if running > 0 {
				msg := fmt.Sprintf("DeepSpeedJob %s is running.", deepspeedJob.Name)
				err := commonutil.UpdateJobConditions(jobStatus, commonv1.JobRunning, commonutil.JobRunningReason, msg)
				if err != nil {
					commonutil.LoggerForJob(deepspeedJob).Infof("Append job condition error: %v", err)
					return err
				}
			}
			// when launcher is succeed, the job is finished.
			if expected == 0 {
				msg := fmt.Sprintf("DeepSpeedJob %s is successfully completed.", deepspeedJob.Name)
				logrus.Info(msg)
				jc.Recorder.Event(deepspeedJob, corev1.EventTypeNormal, commonutil.JobSucceededReason, msg)
				if jobStatus.CompletionTime == nil {
					now := metav1.Now()
					jobStatus.CompletionTime = &now
				}
				err := commonutil.UpdateJobConditions(jobStatus, commonv1.JobSucceeded, commonutil.JobSucceededReason, msg)
				if err != nil {
					commonutil.LoggerForJob(deepspeedJob).Infof("Append job condition error: %v", err)
					return err
				}
				trainingoperatorcommon.SuccessfulJobsCounterInc(deepspeedJob.Namespace, kubeflowv1.DeepSpeedJobFrameworkName)
				return nil
			}
		}
		if failed > 0 {
			if spec.RestartPolicy == commonv1.RestartPolicyExitCode {
				msg := fmt.Sprintf("DeepSpeedJob %s is restarting because %d %s replica(s) failed.", deepspeedJob.Name, failed, rtype)
				jc.Recorder.Event(deepspeedJob, corev1.EventTypeWarning, commonutil.JobRestartingReason, msg)
				err := commonutil.UpdateJobConditions(jobStatus, commonv1.JobRestarting, commonutil.JobRestartingReason, msg)
				if err != nil {
					commonutil.LoggerForJob(deepspeedJob).Infof("Append job condition error: %v", err)
					return err
				}
				trainingoperatorcommon.RestartedJobsCounterInc(deepspeedJob.Namespace, kubeflowv1.DeepSpeedJobFrameworkName)
			} else {
				msg := fmt.Sprintf("DeepSpeedJob %s is failed because %d %s replica(s) failed.", deepspeedJob.Name, failed, rtype)
				jc.Recorder.Event(deepspeedJob, corev1.EventTypeNormal, commonutil.JobFailedReason, msg)
				if jobStatus.CompletionTime == nil {
					now := metav1.Now()
					jobStatus.CompletionTime = &now
				}
				err := commonutil.UpdateJobConditions(jobStatus, commonv1.JobFailed, commonutil.JobFailedReason, msg)
				if err != nil {
					commonutil.LoggerForJob(deepspeedJob).Infof("Append job condition error: %v", err)
					return err
				}
				trainingoperatorcommon.FailedJobsCounterInc(deepspeedJob.Namespace, kubeflowv1.DeepSpeedJobFrameworkName)
			}
		}
	}
	deepspeedJob.Status = *jobStatus.DeepCopy()
	return nil
}

func (jc *DeepSpeedJobReconciler) UpdateJobStatusInApiServer(job interface{}, jobStatus *commonv1.JobStatus) error {
	if jobStatus.ReplicaStatuses == nil {
		jobStatus.ReplicaStatuses = map[commonv1.ReplicaType]*commonv1.ReplicaStatus{}
	}

	deepspeedJob, ok := job.(*kubeflowv1.DeepSpeedJob)
	trainingoperatorcommon.ClearGeneratedFields(&deepspeedJob.ObjectMeta)
	if !ok {
		return fmt.Errorf("%v is not a type of DeepSpeedJob", deepspeedJob)
	}

	startTime := time.Now()
	logger := commonutil.LoggerForJob(deepspeedJob)
	defer func() {
		logger.Infof("Finished updating DeepSpeedJobs Status %q (%v)",
			deepspeedJob.Name, time.Since(startTime))
	}()

	deepspeedJob = deepspeedJob.DeepCopy()
	deepspeedJob.Status = *jobStatus.DeepCopy()

	result := jc.Status().Update(context.Background(), deepspeedJob)

	if result != nil {
		jc.Log.WithValues("deepspeedjob", types.NamespacedName{
			Namespace: deepspeedJob.Namespace,
			Name:      deepspeedJob.Name,
		})
		return result
	}

	return nil
}

// endregion implement ControllerInterface

// SetupWithManager sets up the controller with the Manager.
func (jc *DeepSpeedJobReconciler) SetupWithManager(mgr ctrl.Manager, controllerThreads int) error {
	c, err := controller.New(jc.ControllerName(), mgr, controller.Options{
		Reconciler:              jc,
		MaxConcurrentReconciles: controllerThreads,
	})

	if err != nil {
		return err
	}

	// using onOwnerCreateFunc is easier to set defaults
	if err = c.Watch(&source.Kind{Type: &kubeflowv1.DeepSpeedJob{}}, &handler.EnqueueRequestForObject{},
		predicate.Funcs{CreateFunc: jc.onOwnerCreateFunc()},
	); err != nil {
		return err
	}

	// inject watching for job related pod
	if err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &kubeflowv1.DeepSpeedJob{},
	}, predicate.Funcs{
		CreateFunc: util.OnDependentCreateFunc(jc.Expectations),
		UpdateFunc: util.OnDependentUpdateFunc(&jc.JobController),
		DeleteFunc: util.OnDependentDeleteFunc(jc.Expectations),
	}); err != nil {
		return err
	}
	// inject watching for job related service
	if err = c.Watch(&source.Kind{Type: &corev1.Service{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &kubeflowv1.DeepSpeedJob{},
	}, predicate.Funcs{
		CreateFunc: util.OnDependentCreateFunc(jc.Expectations),
		UpdateFunc: util.OnDependentUpdateFunc(&jc.JobController),
		DeleteFunc: util.OnDependentDeleteFunc(jc.Expectations),
	}); err != nil {
		return err
	}
	// Create generic predicates
	predicates := predicate.Funcs{
		CreateFunc: util.OnDependentCreateFuncGeneric(jc.Expectations),
		UpdateFunc: util.OnDependentUpdateFuncGeneric(&jc.JobController),
		DeleteFunc: util.OnDependentDeleteFuncGeneric(jc.Expectations),
	}
	// inject watching for job related Job
	if err = c.Watch(&source.Kind{Type: &batchv1.Job{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &kubeflowv1.DeepSpeedJob{},
	}, predicates); err != nil {
		return err
	}
	// inject watching for job related ConfigMap
	if err = c.Watch(&source.Kind{Type: &corev1.ConfigMap{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &kubeflowv1.DeepSpeedJob{},
	}, predicates); err != nil {
		return err
	}
	// inject watching for job related Secret
	if err = c.Watch(&source.Kind{Type: &corev1.Secret{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &kubeflowv1.DeepSpeedJob{},
	}, predicates); err != nil {
		return err
	}

	// skip watching volcano PodGroup if volcano PodGroup is not installed
	_, err = mgr.GetRESTMapper().RESTMapping(schema.GroupKind{Group: v1beta1.SchemeGroupVersion.Group, Kind: "PodGroup"},
		v1beta1.SchemeGroupVersion.Version)
	if err == nil {
		// inject watching for job related volcano PodGroup
		if err = c.Watch(&source.Kind{Type: &v1beta1.PodGroup{}}, &handler.EnqueueRequestForOwner{
			IsController: true,
			OwnerType:    &kubeflowv1.DeepSpeedJob{},
		}, predicates); err != nil {
			return err
		}
	}

	// skip watching scheduler-plugins PodGroup if scheduler-plugins PodGroup is not installed
	_, err = mgr.GetRESTMapper().RESTMapping(
		schema.GroupKind{Group: schedulerpluginsv1alpha1.SchemeGroupVersion.Group, Kind: "PodGroup"},
		schedulerpluginsv1alpha1.SchemeGroupVersion.Version)
	if err == nil {
		// inject watching for job related scheduler-plugins PodGroup
		if err = c.Watch(&source.Kind{Type: &schedulerpluginsv1alpha1.PodGroup{}}, &handler.EnqueueRequestForOwner{
			IsController: true,
			OwnerType:    &kubeflowv1.DeepSpeedJob{},
		}, predicates); err != nil {
			return err
		}
	}

	return nil
}

//+kubebuilder:rbac:groups=kubeflow.org,resources=deepspeedjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=kubeflow.org,resources=deepspeedjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=kubeflow.org,resources=deepspeedjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="batch",resources=jobs,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (jc *DeepSpeedJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)
	logger := jc.Log.WithValues(kubeflowv1.DeepSpeedJobSingular, req.NamespacedName)

	job := &kubeflowv1.DeepSpeedJob{}
	err := jc.Get(ctx, req.NamespacedName, job)
	if err != nil {
		logger.Info(err.Error(), "unable to fetch DeepSpeedJob", req.NamespacedName.String())
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// skip for DeepSpeedJob that is being deleted
	if job.GetDeletionTimestamp() != nil {
		return ctrl.Result{}, nil
	}

	// Set defaults to DeepSpeedJob
	jc.Scheme.Default(job)

	if errs := kubeflowv1.ValidateDeepSpeedJob(job); len(errs) != 0 {
		aggErr := errs.ToAggregate()
		msg := truncateMessage(fmt.Sprintf("Found validation errors: %v", aggErr))
		jc.recorder.Event(job, corev1.EventTypeWarning, ValidationError, msg)
		return ctrl.Result{}, aggErr
	}

	// Use common to reconcile the job related pod and service
	err = jc.ReconcileJobs(job, job.Spec.DeepSpeedReplicaSpecs, job.Status, &job.Spec.RunPolicy)
	if err != nil {
		logrus.Warnf("Reconcile DeepSpeedJob error %v", err)
		return ctrl.Result{}, err
	}

	t, err := util.DurationUntilExpireTime(&job.Spec.RunPolicy, job.Status)
	if err != nil {
		logrus.Warnf("Reconcile DeepSpeedJob Job error %v", err)
		return ctrl.Result{}, err
	}
	if t >= 0 {
		return ctrl.Result{Requeue: true, RequeueAfter: t}, nil
	}

	return ctrl.Result{}, nil
}

// onOwnerCreateFunc modify creation condition.
func (jc *DeepSpeedJobReconciler) onOwnerCreateFunc() func(event.CreateEvent) bool {
	return func(e event.CreateEvent) bool {
		job, ok := e.Object.(*kubeflowv1.DeepSpeedJob)
		if !ok {
			return true
		}

		jc.Scheme.Default(job)
		msg := fmt.Sprintf("DeepSpeedJob %s/%s is created.", job.Namespace, e.Object.GetName())
		logrus.Info(msg)
		trainingoperatorcommon.CreatedJobsCounterInc(job.Namespace, kubeflowv1.DeepSpeedJobFrameworkName)
		if err := commonutil.UpdateJobConditions(&job.Status, commonv1.JobCreated, deepspeedJobCreatedReason, msg); err != nil {
			log.Log.Error(err, "append job condition error")
			return false
		}
		return true
	}
}

// region launcher

// getLauncherJob gets the launcher Job controlled by this DeepSpeedJob.
func (jc *DeepSpeedJobReconciler) getLauncherJob(job *kubeflowv1.DeepSpeedJob) (*batchv1.Job, error) {
	launcher := &batchv1.Job{}
	NamespacedName := types.NamespacedName{Namespace: job.Namespace, Name: job.Name + launcherSuffix}
	err := jc.Get(context.Background(), NamespacedName, launcher)
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		// If an error occurs during Get, we'll requeue the item so we can
		// attempt processing again later. This could have been caused by a
		// temporary network failure, or any other transient reason.
		return nil, err
	}

	// If the launcher is not controlled by this DeepSpeedJob resource, we should log
	// a warning to the event recorder and return.
	if !metav1.IsControlledBy(launcher, job) {
		msg := fmt.Sprintf(MessageResourceExists, launcher.Name, launcher.Kind)
		jc.recorder.Event(job, corev1.EventTypeWarning, ErrResourceExists, msg)
		return launcher, fmt.Errorf(msg)
	}

	return launcher, nil
}

func (jc *DeepSpeedJobReconciler) newLauncherJob(job *kubeflowv1.DeepSpeedJob) *batchv1.Job {
	genericLabels := jc.GenLabels(job.GetName())
	labels := defaultLauncherLabels(genericLabels)

	masterRole := jc.IsMasterRole(job.Spec.DeepSpeedReplicaSpecs, kubeflowv1.DeepSpeedReplicaTypeLauncher, 0)
	if masterRole {
		labels[commonv1.JobRoleLabel] = "master"
	}

	kjob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name + launcherSuffix,
			Namespace: job.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, kubeflowv1.SchemeGroupVersion.WithKind(kubeflowv1.DeepSpeedJobKind)),
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: job.Spec.RunPolicy.TTLSecondsAfterFinished,
			ActiveDeadlineSeconds:   job.Spec.RunPolicy.ActiveDeadlineSeconds,
			BackoffLimit:            job.Spec.RunPolicy.BackoffLimit,
			Template:                jc.newLauncherPodTemplate(job),
		},
	}
	return kjob
}

// newLauncherPodTemplate creates a new launcher Job for an DeepSpeedJob resource. It also sets
// the appropriate OwnerReferences on the resource so handleObject can discover
// the DeepSpeedJob resource that 'owns' it.
func (jc *DeepSpeedJobReconciler) newLauncherPodTemplate(job *kubeflowv1.DeepSpeedJob) corev1.PodTemplateSpec {
	launcherName := job.Name + launcherSuffix

	genericLabels := jc.GenLabels(job.GetName())
	labels := defaultLauncherLabels(genericLabels)

	masterRole := jc.IsMasterRole(job.Spec.DeepSpeedReplicaSpecs, kubeflowv1.DeepSpeedReplicaTypeLauncher, 0)
	if masterRole {
		labels[commonv1.JobRoleLabel] = "master"
	}

	podTemplate := job.Spec.DeepSpeedReplicaSpecs[kubeflowv1.DeepSpeedReplicaTypeLauncher].Template.DeepCopy()
	// copy the labels and annotations to pod from PodTemplate
	if len(podTemplate.Annotations) == 0 {
		podTemplate.Annotations = make(map[string]string)
	}
	for key, value := range defaultAnnotations() {
		podTemplate.Annotations[key] = value
	}
	if len(podTemplate.Labels) == 0 {
		podTemplate.Labels = make(map[string]string)
	}
	for key, value := range labels {
		podTemplate.Labels[key] = value
	}
	podTemplate.Spec.Hostname = launcherName
	podTemplate.Spec.Subdomain = job.Name + workerSuffix // Matches workers' Service name.
	if podTemplate.Spec.HostNetwork {
		// Allows resolution of worker hostnames without needing to include the
		// namespace or cluster domain.
		podTemplate.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}
	container := &podTemplate.Spec.Containers[0]
	container.Env = append(container.Env, launcherEnvVars...)

	container.Env = append(container.Env,
		// We overwrite these environment variables so that users will not
		// be mistakenly using GPU resources for launcher due to potential
		// issues with scheduler/container technologies.
		nvidiaDisableEnvVars...)
	jc.setupSSHOnPod(&podTemplate.Spec, job)

	// Submit a warning event if the user specifies restart policy for
	// the pod template. We recommend to set it from the replica level.
	if podTemplate.Spec.RestartPolicy != "" {
		errMsg := "Restart policy in pod template overridden by restart policy in replica spec"
		klog.Warning(errMsg)
		jc.recorder.Event(job, corev1.EventTypeWarning, podTemplateRestartPolicyReason, errMsg)
	}
	setRestartPolicy(podTemplate, job.Spec.DeepSpeedReplicaSpecs[kubeflowv1.DeepSpeedReplicaTypeLauncher])

	podTemplate.Spec.Volumes = append(podTemplate.Spec.Volumes,
		corev1.Volume{
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: job.Name + configSuffix,
					},
					Items: configVolumeItems,
				},
			},
		})
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      configVolumeName,
		MountPath: configMountPath,
	})

	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      podTemplate.Labels,
			Annotations: podTemplate.Annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, kubeflowv1.SchemeGroupVersion.WithKind(kubeflowv1.DeepSpeedJobKind)),
			},
		},
		Spec: podTemplate.Spec,
	}
}

// endregion launcher

// region workers

// getOrCreateWorker gets the worker Pods controlled by this DeepSpeedJob, or creates workers if any doesn't exist.
func (jc *DeepSpeedJobReconciler) getOrCreateWorker(job *kubeflowv1.DeepSpeedJob) ([]*corev1.Pod, error) {
	var workerPods []*corev1.Pod
	worker := job.Spec.DeepSpeedReplicaSpecs[kubeflowv1.DeepSpeedReplicaTypeWorker]
	if worker == nil {
		return workerPods, nil
	}

	// Remove Pods when replicas are scaled down
	genericLabels := jc.GenLabels(job.GetName())
	workerLabels := defaultWorkerLabels(genericLabels)
	selector, err := labels.ValidatedSelectorFromSet(workerLabels)

	if err != nil {
		return nil, err
	}
	podFullList := &corev1.PodList{}
	err = jc.List(context.Background(), podFullList, client.MatchingLabelsSelector{Selector: selector}, client.InNamespace(job.Namespace))
	if err != nil {
		return nil, err
	}
	// scale down
	if len(podFullList.Items) > int(*worker.Replicas) {
		for _, pod := range podFullList.Items {
			indexStr, ok := pod.Labels[commonv1.ReplicaIndexLabel]
			if !ok {
				return nil, err
			}
			index, err := strconv.Atoi(indexStr)
			if err == nil {
				if index >= int(*worker.Replicas) {
					err = jc.KubeClientSet.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}

	for i := 0; i < int(*worker.Replicas); i++ {
		pod := &corev1.Pod{}
		NamespacedName := types.NamespacedName{Namespace: job.Namespace, Name: workerName(job, i)}
		err := jc.Get(context.Background(), NamespacedName, pod)

		// If the worker Pod doesn't exist, we'll create it.
		if errors.IsNotFound(err) {
			worker := jc.newWorker(job, i)
			pod, err = jc.KubeClientSet.CoreV1().Pods(job.Namespace).Create(context.TODO(), worker, metav1.CreateOptions{})
		}
		// If an error occurs during Get/Create, we'll requeue the item so we
		// can attempt processing again later. This could have been caused by a
		// temporary network failure, or any other transient reason.
		if err != nil {
			jc.recorder.Eventf(job, corev1.EventTypeWarning, deepspeedJobFailedReason, "worker pod created failed: %v", err)
			return nil, err
		}
		// If the worker is not controlled by this DeepSpeedJob resource, we should log
		// a warning to the event recorder and return.
		if pod != nil && !metav1.IsControlledBy(pod, job) {
			msg := fmt.Sprintf(MessageResourceExists, pod.Name, pod.Kind)
			jc.recorder.Event(job, corev1.EventTypeWarning, ErrResourceExists, msg)
			return nil, fmt.Errorf(msg)
		}
		workerPods = append(workerPods, pod)
	}

	return workerPods, nil
}

// newWorker creates a new worker Pod for an DeepSpeedJob resource. It also
// sets the appropriate OwnerReferences on the resource so handleObject can
// discover the DeepSpeedJob resource that 'owns' it.
func (jc *DeepSpeedJobReconciler) newWorker(job *kubeflowv1.DeepSpeedJob, index int) *corev1.Pod {
	name := workerName(job, index)
	genericLabels := jc.GenLabels(job.GetName())
	labels := defaultWorkerLabels(genericLabels)
	podTemplate := job.Spec.DeepSpeedReplicaSpecs[kubeflowv1.DeepSpeedReplicaTypeWorker].Template.DeepCopy()

	// keep the labels which are set in PodTemplate
	if len(podTemplate.Annotations) == 0 {
		podTemplate.Annotations = make(map[string]string)
	}
	for key, value := range defaultAnnotations() {
		podTemplate.Annotations[key] = value
	}
	if len(podTemplate.Labels) == 0 {
		podTemplate.Labels = make(map[string]string)
	}
	for key, value := range labels {
		podTemplate.Labels[key] = value
	}
	podTemplate.Labels[commonv1.ReplicaIndexLabel] = strconv.Itoa(index)

	podTemplate.Spec.Hostname = name
	podTemplate.Spec.Subdomain = job.Name + workerSuffix // Matches workers' Service name.
	if podTemplate.Spec.HostNetwork {
		// Allows resolution of worker hostnames without needing to include the
		// namespace or cluster domain.
		podTemplate.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}
	setRestartPolicy(podTemplate, job.Spec.DeepSpeedReplicaSpecs[kubeflowv1.DeepSpeedReplicaTypeWorker])

	container := &podTemplate.Spec.Containers[0]
	if len(container.Command) == 0 && len(container.Args) == 0 {
		// TODO: invest more into sshd
		// container.Command = []string{"/usr/sbin/sshd", "-De"}
		container.Command = []string{"/bin/sh", "-c"}
		container.Args = []string{"mkdir -p /run/sshd; /usr/sbin/sshd -De"}
	}
	container.Env = append(container.Env, workerEnvVars...)
	jc.setupSSHOnPod(&podTemplate.Spec, job)

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   job.Namespace,
			Labels:      podTemplate.Labels,
			Annotations: podTemplate.Annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, kubeflowv1.SchemeGroupVersion.WithKind(kubeflowv1.DeepSpeedJobKind)),
			},
		},
		Spec: podTemplate.Spec,
	}
}

// endregion workers

// region hostfile

// getOrCreateConfigMap gets the ConfigMap controlled by this DeepSpeedJob,
// or creates one if it doesn't exist.
func (jc *DeepSpeedJobReconciler) getOrCreateConfigMap(job *kubeflowv1.DeepSpeedJob) (*corev1.ConfigMap, error) {
	newCM := newConfigMap(job, workerReplicas(job))
	podList, err := jc.getRunningWorkerPods(job)
	if err != nil {
		return nil, err
	}
	updateDiscoverHostsInConfigMap(newCM, job, podList)

	cm := &corev1.ConfigMap{}
	NamespacedName := types.NamespacedName{Namespace: job.Namespace, Name: job.Name + configSuffix}
	err = jc.Get(context.Background(), NamespacedName, cm)
	// If the ConfigMap doesn't exist, we'll create it.
	if errors.IsNotFound(err) {
		cm, err = jc.KubeClientSet.CoreV1().ConfigMaps(job.Namespace).Create(context.Background(), newCM, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}

	// If the ConfigMap is not controlled by this DeepSpeedJob resource, we
	// should log a warning to the event recorder and return.
	if !metav1.IsControlledBy(cm, job) {
		msg := fmt.Sprintf(MessageResourceExists, cm.Name, cm.Kind)
		jc.recorder.Event(job, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	// If the ConfigMap is changed, update it
	if !equality.Semantic.DeepEqual(cm.Data, newCM.Data) {
		cm, err = jc.KubeClientSet.CoreV1().ConfigMaps(job.Namespace).Update(context.Background(), newCM, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}
	}

	return cm, nil
}

// getRunningWorkerPods get all worker Pods with Running phase controlled by this DeepSpeedJob.
func (jc *DeepSpeedJobReconciler) getRunningWorkerPods(job *kubeflowv1.DeepSpeedJob) ([]*corev1.Pod, error) {
	genericLabels := jc.GenLabels(job.GetName())
	workerLabels := defaultWorkerLabels(genericLabels)
	selector, err := labels.ValidatedSelectorFromSet(workerLabels)

	if err != nil {
		return nil, err
	}

	podFullList := &corev1.PodList{}
	err = jc.List(context.Background(), podFullList, client.MatchingLabelsSelector{Selector: selector}, client.InNamespace(job.Namespace))
	if err != nil {
		return nil, err
	}
	// Only running Pods should be included within the `discover_hosts.sh` script.
	var podList []corev1.Pod
	for idx, pod := range podFullList.Items {
		if pod.Status.Phase == corev1.PodRunning {
			podList = append(podList, podFullList.Items[idx])
		}
	}

	var filter util.ObjectFilterFunction = func(obj metav1.Object) bool {
		return metav1.IsControlledBy(obj, job)
	}

	return util.ConvertPodListWithFilter(podList, filter), nil
}

// newConfigMap creates a new ConfigMap containing configurations for an DeepSpeedJob
// resource. It also sets the appropriate OwnerReferences on the resource so
// handleObject can discover the DeepSpeedJob resource that 'owns' it.
func newConfigMap(job *kubeflowv1.DeepSpeedJob, workerReplicas int32) *corev1.ConfigMap {
	var buffer bytes.Buffer
	workersService := job.Name + workerSuffix
	slots := 1
	if job.Spec.SlotsPerWorker != nil {
		slots = int(*job.Spec.SlotsPerWorker)
	}
	for i := 0; i < int(workerReplicas); i++ {
		buffer.WriteString(fmt.Sprintf("%s%s-%d.%s.%s.svc slots=%d\n", job.Name, workerSuffix, i, workersService, job.Namespace, slots))
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name + configSuffix,
			Namespace: job.Namespace,
			Labels: map[string]string{
				"app": job.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, kubeflowv1.GroupVersion.WithKind(kubeflowv1.DeepSpeedJobKind)),
			},
		},
		Data: map[string]string{
			hostfileName: buffer.String(),
		},
	}
}

// updateDiscoverHostsInConfigMap updates the ConfigMap if the content of `discover_hosts.sh` changes.
func updateDiscoverHostsInConfigMap(configMap *corev1.ConfigMap, job *kubeflowv1.DeepSpeedJob, runningPods []*corev1.Pod) {
	// Sort the slice of Pods to make sure the order of entries in `discover_hosts.sh` is maintained.
	sort.Slice(runningPods, func(i, j int) bool {
		return runningPods[i].Name < runningPods[j].Name
	})

	var buffer bytes.Buffer
	buffer.WriteString("#!/bin/sh\n")
	workersService := job.Name + workerSuffix
	for _, p := range runningPods {
		buffer.WriteString(fmt.Sprintf("echo %s.%s.%s.svc\n", p.Name, workersService, p.Namespace))
	}

	configMap.Data[discoverHostsScriptName] = buffer.String()
}

// endregion hostfile

// region ssh

// getOrCreateSSHAuthSecret gets the Secret holding the SSH auth for this job,
// or create one if it doesn't exist.
func (jc *DeepSpeedJobReconciler) getOrCreateSSHAuthSecret(job *kubeflowv1.DeepSpeedJob) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	NamespacedName := types.NamespacedName{Namespace: job.Namespace, Name: job.Name + sshAuthSecretSuffix}
	err := jc.Get(context.Background(), NamespacedName, secret)

	if errors.IsNotFound(err) {
		secret, err := newSSHAuthSecret(job)
		if err != nil {
			return nil, err
		}
		return jc.KubeClientSet.CoreV1().Secrets(job.Namespace).Create(context.TODO(), secret, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(secret, job) {
		msg := fmt.Sprintf(MessageResourceExists, secret.Name, secret.Kind)
		jc.recorder.Event(job, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}
	newSecret, err := newSSHAuthSecret(job)
	if err != nil {
		return nil, fmt.Errorf("generating new secret: %w", err)
	}
	hasKeys := keysFromData(secret.Data)
	wantKeys := keysFromData(newSecret.Data)
	if !equality.Semantic.DeepEqual(hasKeys, wantKeys) {
		secret := secret.DeepCopy()
		secret.Data = newSecret.Data
		return jc.KubeClientSet.CoreV1().Secrets(secret.Namespace).Update(context.TODO(), secret, metav1.UpdateOptions{})
	}
	return secret, nil
}

// newSSHAuthSecret creates a new Secret that holds SSH auth: a private Key
// and its public key version.
func newSSHAuthSecret(job *kubeflowv1.DeepSpeedJob) (*corev1.Secret, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating private SSH key: %w", err)
	}
	privateDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("converting private SSH key to DER format: %w", err)
	}
	privatePEM := pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: privateDER,
	})

	publicKey, err := ssh.NewPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("generating public SSH key: %w", err)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name + sshAuthSecretSuffix,
			Namespace: job.Namespace,
			Labels: map[string]string{
				"app": job.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, kubeflowv1.SchemeGroupVersion.WithKind(kubeflowv1.DeepSpeedJobKind)),
			},
		},
		Type: corev1.SecretTypeSSHAuth,
		Data: map[string][]byte{
			corev1.SSHAuthPrivateKey: privatePEM,
			sshPublicKey:             ssh.MarshalAuthorizedKey(publicKey),
		},
	}, nil
}

func (jc *DeepSpeedJobReconciler) setupSSHOnPod(podSpec *corev1.PodSpec, job *kubeflowv1.DeepSpeedJob) {
	mainContainer := &podSpec.Containers[0]
	podSpec.Volumes = append(podSpec.Volumes,
		corev1.Volume{
			Name: sshAuthVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: job.Name + sshAuthSecretSuffix,
					Items:      sshVolumeItems,
				},
			},
		})

	mainContainer.VolumeMounts = append(mainContainer.VolumeMounts,
		corev1.VolumeMount{
			Name:      sshAuthVolume,
			MountPath: job.Spec.SSHAuthMountPath,
		})
}

// endregion ssh

// updateDeepSpeedJobStatus updates DeepSpeedJob.Status.Conditions and DeepSpeedJob.Status.ReplicaStatuses
func (jc *DeepSpeedJobReconciler) updateDeepSpeedJobStatus(job *kubeflowv1.DeepSpeedJob, launcher *batchv1.Job, workers []*corev1.Pod) error {
	launcherPods, err := jc.jobPods(launcher)
	if err != nil {
		return fmt.Errorf("checking launcher pods running: %w", err)
	}
	// Job.status.Active accounts for Pending and Running pods. Count running pods
	// from the lister instead.
	runningLauncherPodsCnt := countRunningPods(launcherPods)
	if launcher != nil {
		initializeDeepSpeedJobStatuses(job, kubeflowv1.DeepSpeedReplicaTypeLauncher)
		launcherStatus := job.Status.ReplicaStatuses[kubeflowv1.DeepSpeedReplicaTypeLauncher]
		launcherStatus.Failed = launcher.Status.Failed
		if isJobSucceeded(launcher) {
			launcherStatus.Succeeded = 1
			msg := fmt.Sprintf("DeepSpeedJob %s/%s successfully completed.", job.Namespace, job.Name)
			jc.recorder.Event(job, corev1.EventTypeNormal, deepspeedJobSucceededReason, msg)
			if job.Status.CompletionTime == nil {
				job.Status.CompletionTime = launcher.Status.CompletionTime
			}
			updateDeepSpeedJobConditions(job, commonv1.JobSucceeded, corev1.ConditionTrue, deepspeedJobSucceededReason, msg)
			trainingoperatorcommon.SuccessfulJobsCounterInc(job.Namespace, kubeflowv1.DeepSpeedJobFrameworkName)
		} else if isJobFailed(launcher) {
			jc.updateDeepSpeedJobFailedStatus(job, launcher, launcherPods)
		} else {
			job.Status.ReplicaStatuses[kubeflowv1.DeepSpeedReplicaTypeLauncher].Active = int32(runningLauncherPodsCnt)
		}
	}

	var (
		running = 0
		evict   = 0
	)

	initializeDeepSpeedJobStatuses(job, kubeflowv1.DeepSpeedReplicaTypeWorker)
	for i := 0; i < len(workers); i++ {
		switch workers[i].Status.Phase {
		case corev1.PodFailed:
			job.Status.ReplicaStatuses[kubeflowv1.DeepSpeedReplicaTypeWorker].Failed += 1
			if workers[i].Status.Reason == "Evicted" {
				evict += 1
			}
		case corev1.PodSucceeded:
			job.Status.ReplicaStatuses[kubeflowv1.DeepSpeedReplicaTypeWorker].Succeeded += 1
		case corev1.PodRunning:
			running += 1
			job.Status.ReplicaStatuses[kubeflowv1.DeepSpeedReplicaTypeWorker].Active += 1
		}
	}
	if evict > 0 {
		msg := fmt.Sprintf("%d/%d workers are evicted", evict, len(workers))
		klog.Infof("DeepSpeedJob <%s/%s>: %v", job.Namespace, job.Name, msg)
		updateDeepSpeedJobConditions(job, commonv1.JobFailed, corev1.ConditionTrue, deepspeedJobEvict, msg)
		jc.recorder.Event(job, corev1.EventTypeWarning, deepspeedJobEvict, msg)
	}

	if launcher != nil && runningLauncherPodsCnt >= 1 && running == len(workers) {
		msg := fmt.Sprintf("DeepSpeedJob %s/%s is running.", job.Namespace, job.Name)
		updateDeepSpeedJobConditions(job, commonv1.JobRunning, corev1.ConditionTrue, deepspeedJobRunningReason, msg)
		jc.recorder.Eventf(job, corev1.EventTypeNormal, "DeepSpeedJobRunning", "DeepSpeedJob %s/%s is running", job.Namespace, job.Name)
	}

	return nil
}

func (jc *DeepSpeedJobReconciler) updateDeepSpeedJobFailedStatus(job *kubeflowv1.DeepSpeedJob, launcher *batchv1.Job, launcherPods []*corev1.Pod) {
	jobFailedCond := getJobCondition(launcher, batchv1.JobFailed)
	reason := jobFailedCond.Reason
	if reason == "" {
		reason = deepspeedJobFailedReason
	}
	msg := jobFailedCond.Message
	if msg == "" {
		msg = fmt.Sprintf("DeepSpeedJob %s/%s has failed", job.Namespace, job.Name)
	}
	if reason == jobBackoffLimitExceededReason {
		// Concatenate the reason and message from the last failed Pod.
		var lastFailedPod *corev1.Pod
		for _, p := range launcherPods {
			if isPodFailed(p) && (lastFailedPod == nil || lastFailedPod.CreationTimestamp.Before(&p.CreationTimestamp)) {
				lastFailedPod = p
			}
		}
		if lastFailedPod != nil {
			reason += "/" + lastFailedPod.Status.Reason
			msg += ": " + lastFailedPod.Status.Message
			msg = truncateMessage(msg)
		}
	}
	jc.recorder.Event(job, corev1.EventTypeWarning, reason, msg)
	if job.Status.CompletionTime == nil {
		now := metav1.Now()
		job.Status.CompletionTime = &now
	}
	updateDeepSpeedJobConditions(job, commonv1.JobFailed, corev1.ConditionTrue, reason, msg)
	trainingoperatorcommon.FailedJobsCounterInc(job.Namespace, kubeflowv1.DeepSpeedJobFrameworkName)
}

// jobPods get the Pods of a Job
func (jc *DeepSpeedJobReconciler) jobPods(job *batchv1.Job) ([]*corev1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(job.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("parsing Pod selector: %w", err)
	}

	pods := &corev1.PodList{}
	err = jc.List(context.Background(), pods, client.MatchingLabelsSelector{Selector: selector}, client.InNamespace(job.Namespace))
	if err != nil {
		return nil, fmt.Errorf("obtaining pods: %w", err)
	}

	var filter util.ObjectFilterFunction = func(obj metav1.Object) bool {
		return metav1.IsControlledBy(obj, job)
	}

	return util.ConvertPodListWithFilter(pods.Items, filter), nil
}

// countRunningPods returns the number of Pods that are in Running status
func countRunningPods(pods []*corev1.Pod) int {
	running := 0
	for _, p := range pods {
		if isPodRunning(p) {
			running++
		}
	}
	return running
}

// region status

func isJobFinished(j *batchv1.Job) bool {
	return isJobSucceeded(j) || isJobFailed(j)
}

func isJobFailed(j *batchv1.Job) bool {
	c := getJobCondition(j, batchv1.JobFailed)
	return c != nil && c.Status == corev1.ConditionTrue
}

func isJobSucceeded(j *batchv1.Job) bool {
	c := getJobCondition(j, batchv1.JobComplete)
	return c != nil && c.Status == corev1.ConditionTrue
}

func getJobCondition(j *batchv1.Job, condition batchv1.JobConditionType) *batchv1.JobCondition {
	for _, c := range j.Status.Conditions {
		if c.Type == condition {
			return &c
		}
	}
	return nil
}

func isPodRunning(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodRunning
}

func isPodFailed(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodFailed
}

// endregion

// workerName returns the pod name of the ${index}th worker of a DeepSpeedJob
func workerName(job *kubeflowv1.DeepSpeedJob, index int) string {
	return fmt.Sprintf("%s%s-%d", job.Name, workerSuffix, index)
}

func setRestartPolicy(podTemplateSpec *corev1.PodTemplateSpec, spec *commonv1.ReplicaSpec) {
	if spec.RestartPolicy == commonv1.RestartPolicyExitCode {
		podTemplateSpec.Spec.RestartPolicy = corev1.RestartPolicyNever
	} else {
		podTemplateSpec.Spec.RestartPolicy = corev1.RestartPolicy(spec.RestartPolicy)
	}
}

func defaultAnnotations() map[string]string {
	return map[string]string{
		"sidecar.istio.io/inject": "false",
	}
}

func defaultReplicaLabels(genericLabels map[string]string, roleLabelVal string) map[string]string {
	replicaLabels := map[string]string{}
	for k, v := range genericLabels {
		replicaLabels[k] = v
	}

	replicaLabels[commonv1.ReplicaTypeLabel] = roleLabelVal
	return replicaLabels
}

func defaultWorkerLabels(genericLabels map[string]string) map[string]string {
	return defaultReplicaLabels(genericLabels, worker)
}

func defaultLauncherLabels(genericLabels map[string]string) map[string]string {
	return defaultReplicaLabels(genericLabels, launcher)
}

func workerReplicas(job *kubeflowv1.DeepSpeedJob) int32 {
	workerSpec := job.Spec.DeepSpeedReplicaSpecs[kubeflowv1.DeepSpeedReplicaTypeWorker]
	if workerSpec != nil && workerSpec.Replicas != nil {
		return *workerSpec.Replicas
	}
	return 0
}
