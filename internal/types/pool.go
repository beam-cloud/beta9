package types

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	WorkerPoolSchemaGroupName    = "beam.cloud"
	WorkerPoolSchemaGroupVersion = "v1"
	WorkerPoolResourcePlural     = "workerpools"
	WorkerPoolResourceSingular   = "workerpool"
)

// WorkerPoolResource is the Go representation of the WorkerPool custom resource
type WorkerPoolResource struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec WorkerPoolSpec `json:"spec,omitempty"`
}

// Gets a new WorkerPoolResource struct.
// A helper function that defines the Name, APIVersion, and Kind of the resource.
func NewWorkerPoolResource(name string) WorkerPoolResource {
	return WorkerPoolResource{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		TypeMeta: metav1.TypeMeta{
			APIVersion: fmt.Sprintf("%v/%v", WorkerPoolSchemaGroupName, WorkerPoolSchemaGroupVersion),
			Kind:       WorkerPoolResourceSingular,
		},
	}
}

type WorkerPoolSpec struct {
	JobSpec    JobSpec              `json:"jobSpec,omitempty"`
	PoolSizing runtime.RawExtension `json:"poolSizing,omitempty"`
}

type JobSpec struct {
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

type WorkerPoolStatus struct {
	CurrentWorkers int `json:"currentWorkers"`
}

// WorkerPoolList is a list of WorkerPool custom resources
type WorkerPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkerPoolResource `json:"items"`
}

func (in *WorkerPoolResource) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *WorkerPoolResource) DeepCopy() *WorkerPoolResource {
	if in == nil {
		return nil
	}
	out := new(WorkerPoolResource)
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()

	out.Spec.JobSpec.NodeSelector = make(map[string]string, len(in.Spec.JobSpec.NodeSelector))
	for key, val := range in.Spec.JobSpec.NodeSelector {
		out.Spec.JobSpec.NodeSelector[key] = val
	}

	out.Spec.PoolSizing = runtime.RawExtension{
		Raw: append([]byte(nil), in.Spec.PoolSizing.Raw...),
	}

	return out
}

func (in *WorkerPoolList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *WorkerPoolList) DeepCopy() *WorkerPoolList {
	if in == nil {
		return nil
	}
	out := new(WorkerPoolList)
	out.TypeMeta = in.TypeMeta
	out.ListMeta = *in.ListMeta.DeepCopy()
	if in.Items != nil {
		out.Items = make([]WorkerPoolResource, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopy()
		}
	}
	return out
}
