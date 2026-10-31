package repository_services

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func (s *WorkerRepositoryService) authorizeWorkerNetwork(ctx context.Context, info *auth.AuthInfo, prefix string) error {
	workerID := info.Token.WorkerID
	value, known := s.workerNetworkScopes.Load(workerID)
	if !known {
		worker, err := s.workerRepo.GetWorkerById(workerID)
		if err != nil || worker == nil {
			return fmt.Errorf("worker is unavailable")
		}
		node := worker.MachineId
		if node == "" {
			if s.workerNodeResolver != nil {
				node, err = s.workerNodeResolver(ctx, workerID)
			} else {
				node, err = s.kubernetesWorkerNode(ctx, workerID)
			}
			if err != nil || node == "" {
				return fmt.Errorf("worker node is unavailable")
			}
		}
		value = common.WorkerNetworkPrefix(s.appConfig.ClusterName, node)
		s.workerNetworkScopes.Store(workerID, value)
	}
	if prefix != value.(string) && prefix != value.(string)+":slot_pool" {
		return fmt.Errorf("network is not assigned to worker")
	}
	return nil
}

func (s *WorkerRepositoryService) kubernetesWorkerNode(ctx context.Context, workerID string) (string, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return "", err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", err
	}
	namespace := s.appConfig.Worker.Namespace
	if namespace == "" {
		namespace = "beta9"
	}
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: scheduler.Beta9WorkerLabelIDKey + "=" + workerID})
	if err != nil {
		return "", err
	}
	node := ""
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil || pod.Spec.NodeName == "" {
			continue
		}
		if node != "" && node != pod.Spec.NodeName {
			return "", fmt.Errorf("worker has conflicting node assignments")
		}
		node = pod.Spec.NodeName
	}
	return node, nil
}

func workerNetworkRecordPrefix(ctx context.Context) string {
	info, ok := auth.AuthInfoFromContext(ctx)
	if !ok || info == nil || info.Token == nil {
		return ""
	}
	return "worker:" + info.Token.WorkerID + ":"
}

func workerNetworkRecord(ctx context.Context, containerID string) string {
	return workerNetworkRecordPrefix(ctx) + strings.TrimPrefix(containerID, workerNetworkRecordPrefix(ctx))
}

// Node address allocation is shared; each worker owns only its own records.
func (s *WorkerRepositoryService) scopedNetworkPrefix(ctx context.Context, prefix string) string {
	info, ok := auth.AuthInfoFromContext(ctx)
	if ok && info != nil && info.Token != nil && info.Token.TokenType == types.TokenTypeWorkerPrivate {
		return "workspace:" + info.Workspace.ExternalId + ":" + prefix
	}
	return prefix
}
