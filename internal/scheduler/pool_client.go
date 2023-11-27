package scheduler

import (
	"context"

	"github.com/beam-cloud/beam/internal/types"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

func init() {
	// Register the WorkerPool custom resource with the Kubernetes scheme
	schemeBuilder := runtime.NewSchemeBuilder(func(s *runtime.Scheme) error {
		// Registers WorkerPoolList as-is
		s.AddKnownTypes(
			schema.GroupVersion{
				Group:   types.WorkerPoolSchemaGroupName,
				Version: types.WorkerPoolSchemaGroupVersion,
			},
			&types.WorkerPoolList{},
		)

		// Registers WorkerPoolResource as WorkerPool
		s.AddKnownTypeWithName(
			schema.GroupVersionKind{
				Kind:    "WorkerPool",
				Group:   types.WorkerPoolSchemaGroupName,
				Version: types.WorkerPoolSchemaGroupVersion,
			},
			&types.WorkerPoolResource{},
		)

		return nil
	})

	schemeBuilder.AddToScheme(scheme.Scheme)
}

type WorkerPoolClient struct {
	restClient rest.Interface
}

func NewWorkerPoolClient(cfg *rest.Config) (*WorkerPoolClient, error) {
	cfg.GroupVersion = &schema.GroupVersion{Group: types.WorkerPoolSchemaGroupName, Version: types.WorkerPoolSchemaGroupVersion}
	cfg.APIPath = "/apis"
	cfg.NegotiatedSerializer = serializer.NewCodecFactory(scheme.Scheme)

	restClient, err := rest.RESTClientFor(cfg)
	if err != nil {
		return nil, err
	}

	return &WorkerPoolClient{restClient: restClient}, nil
}

func (c *WorkerPoolClient) GetWorkerPool(namespace, name string) (*types.WorkerPoolResource, error) {
	result := &types.WorkerPoolResource{}
	err := c.restClient.Get().
		Namespace(namespace).
		Resource(types.WorkerPoolResourcePlural).
		Name(name).
		Do(context.Background()).
		Into(result)
	return result, err
}
