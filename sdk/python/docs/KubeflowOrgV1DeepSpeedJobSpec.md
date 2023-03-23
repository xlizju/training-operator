# KubeflowOrgV1DeepSpeedJobSpec

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**deepspeed_replica_specs** | [**dict(str, V1ReplicaSpec)**](V1ReplicaSpec.md) | DeepSpeedReplicaSpecs contains maps from &#x60;DeepSpeedReplicaType&#x60; to &#x60;ReplicaSpec&#x60; that specify the DeepSpeed replicas to run. | 
**run_policy** | [**V1RunPolicy**](V1RunPolicy.md) |  | [optional] 
**slots_per_worker** | **int** | Specifies the number of slots per worker used in hostfile. Defaults to 1. | [optional] 
**ssh_auth_mount_path** | **str** | SSHAuthMountPath is the directory where SSH keys are mounted. Defaults to \&quot;/root/.ssh\&quot;. | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


