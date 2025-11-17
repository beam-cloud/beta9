"""Cluster bootstrap logic for installing Beta9."""

import base64
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, Any

import boto3
from kubernetes import client, config as k8s_config
import structlog
import yaml

from models.config import InfraModel, CloudProvider


logger = structlog.get_logger()


def bootstrap_beta9(infra: InfraModel, terraform_outputs: Dict[str, Any]) -> None:
    """
    Bootstrap Beta9 into the provisioned cluster.

    Steps:
    1. Generate kubeconfig for the cluster
    2. Apply baseline infrastructure (ingress, cert-manager, etc.)
    3. Install Beta9 Helm charts with computed configuration

    Args:
        infra: Infrastructure model
        terraform_outputs: Outputs from Terraform apply

    Raises:
        Exception: If bootstrap fails
    """
    logger.info("Starting cluster bootstrap", environment_id=infra.environment_id)

    # Generate kubeconfig
    kubeconfig_path = _generate_kubeconfig(infra, terraform_outputs)

    # Load kubeconfig
    k8s_config.load_kube_config(config_file=str(kubeconfig_path))

    # Wait for cluster to be ready
    _wait_for_cluster_ready()

    # Apply baseline infrastructure
    _apply_baseline_infrastructure(infra, kubeconfig_path)

    # Install Beta9
    _install_beta9(infra, terraform_outputs, kubeconfig_path)

    logger.info("Cluster bootstrap complete", environment_id=infra.environment_id)


def _generate_kubeconfig(infra: InfraModel, outputs: Dict[str, Any]) -> Path:
    """
    Generate kubeconfig for the cluster.

    Args:
        infra: Infrastructure model
        outputs: Terraform outputs

    Returns:
        Path to kubeconfig file
    """
    if infra.cloud_provider == CloudProvider.AWS:
        return _generate_eks_kubeconfig(infra, outputs)
    elif infra.cloud_provider == CloudProvider.GCP:
        return _generate_gke_kubeconfig(infra, outputs)
    else:
        raise ValueError(f"Unsupported cloud provider: {infra.cloud_provider}")


def _generate_eks_kubeconfig(infra: InfraModel, outputs: Dict[str, Any]) -> Path:
    """
    Generate kubeconfig for EKS cluster.

    Uses AWS CLI to generate kubeconfig with IAM authentication.

    Args:
        infra: Infrastructure model
        outputs: Terraform outputs

    Returns:
        Path to kubeconfig file
    """
    cluster_name = outputs["cluster_name"]
    cluster_endpoint = outputs["cluster_endpoint"]
    cluster_ca = outputs["cluster_certificate_authority_data"]

    # Create temporary kubeconfig file
    kubeconfig_path = Path(tempfile.gettempdir()) / f"kubeconfig-{infra.environment_id}"

    # Use AWS CLI to generate kubeconfig
    # This handles IAM authentication automatically
    cmd = [
        "aws",
        "eks",
        "update-kubeconfig",
        "--name",
        cluster_name,
        "--region",
        infra.region,
        "--kubeconfig",
        str(kubeconfig_path),
    ]

    if infra.role_arn:
        cmd.extend(["--role-arn", infra.role_arn])

    logger.info("Generating EKS kubeconfig", cluster_name=cluster_name)

    result = subprocess.run(cmd, capture_output=True, check=True)

    logger.info("EKS kubeconfig generated", path=str(kubeconfig_path))

    return kubeconfig_path


def _generate_gke_kubeconfig(infra: InfraModel, outputs: Dict[str, Any]) -> Path:
    """
    Generate kubeconfig for GKE cluster.

    Args:
        infra: Infrastructure model
        outputs: Terraform outputs

    Returns:
        Path to kubeconfig file
    """
    # TODO: Implement GKE kubeconfig generation
    raise NotImplementedError("GKE kubeconfig generation not yet implemented")


def _wait_for_cluster_ready(timeout: int = 300) -> None:
    """
    Wait for cluster to be ready.

    Args:
        timeout: Timeout in seconds

    Raises:
        Exception: If cluster is not ready within timeout
    """
    import time

    logger.info("Waiting for cluster to be ready")

    v1 = client.CoreV1Api()

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Check if we can list nodes
            nodes = v1.list_node()

            if len(nodes.items) > 0:
                ready_nodes = sum(
                    1
                    for node in nodes.items
                    if any(
                        condition.type == "Ready" and condition.status == "True"
                        for condition in node.status.conditions
                    )
                )

                logger.info(
                    "Cluster status",
                    total_nodes=len(nodes.items),
                    ready_nodes=ready_nodes,
                )

                if ready_nodes > 0:
                    logger.info("Cluster is ready")
                    return

        except Exception as e:
            logger.debug("Cluster not ready yet", error=str(e))

        time.sleep(10)

    raise Exception(f"Cluster not ready after {timeout} seconds")


def _apply_baseline_infrastructure(infra: InfraModel, kubeconfig_path: Path) -> None:
    """
    Apply baseline infrastructure to the cluster.

    Installs:
    - Ingress controller (nginx-ingress)
    - cert-manager
    - metrics-server

    Args:
        infra: Infrastructure model
        kubeconfig_path: Path to kubeconfig
    """
    logger.info("Applying baseline infrastructure")

    # Install nginx-ingress
    _helm_install(
        release_name="nginx-ingress",
        chart="ingress-nginx/ingress-nginx",
        namespace="ingress-nginx",
        create_namespace=True,
        kubeconfig=kubeconfig_path,
        repo_url="https://kubernetes.github.io/ingress-nginx",
        values={
            "controller": {
                "service": {
                    "annotations": {
                        # AWS ELB annotations
                        "service.beta.kubernetes.io/aws-load-balancer-type": "nlb",
                    }
                }
            }
        },
    )

    # Install cert-manager
    _helm_install(
        release_name="cert-manager",
        chart="jetstack/cert-manager",
        namespace="cert-manager",
        create_namespace=True,
        kubeconfig=kubeconfig_path,
        repo_url="https://charts.jetstack.io",
        values={
            "installCRDs": True,
        },
    )

    # Install metrics-server
    _helm_install(
        release_name="metrics-server",
        chart="metrics-server/metrics-server",
        namespace="kube-system",
        kubeconfig=kubeconfig_path,
        repo_url="https://kubernetes-sigs.github.io/metrics-server/",
    )

    logger.info("Baseline infrastructure applied")


def _install_beta9(infra: InfraModel, outputs: Dict[str, Any], kubeconfig_path: Path) -> None:
    """
    Install Beta9 Helm charts.

    Args:
        infra: Infrastructure model
        outputs: Terraform outputs
        kubeconfig_path: Path to kubeconfig
    """
    logger.info("Installing Beta9")

    # Fetch RDS password from AWS Secrets Manager
    db_password = _fetch_rds_password(infra, outputs)

    # Build Beta9 Helm values
    values = {
        "global": {
            "domain": infra.beta9_domain,
            "version": infra.beta9_version,
        },
        "database": {
            "host": outputs["rds_endpoint"].split(":")[0],  # Remove port
            "port": 5432,
            "name": "beta9",
            "username": "beta9admin",
            "password": db_password,
        },
        "redis": {
            "host": outputs["redis_endpoint"],
            "port": outputs["redis_port"],
        },
        "storage": {
            "type": "s3",
            "s3": {
                "bucket": outputs["bucket_name"],
                "region": infra.region,
            },
        },
        "workers": {
            "enabled": True,
            "gpu": {
                "enabled": infra.gpu_enabled,
                "type": infra.gpu_type,
            },
        },
        "metrics": {
            "enabled": True,
        },
    }

    # Note: Assuming Beta9 charts are available locally or in a registry
    # In production, this should point to the actual Beta9 Helm repo
    chart_path = Path(__file__).parent.parent.parent / "deploy" / "charts" / "beta9"

    if not chart_path.exists():
        logger.warning(
            "Beta9 chart not found locally, skipping installation",
            chart_path=str(chart_path),
        )
        return

    _helm_install(
        release_name="beta9",
        chart=str(chart_path),
        namespace="beta9-system",
        create_namespace=True,
        kubeconfig=kubeconfig_path,
        values=values,
    )

    logger.info("Beta9 installed")


def _fetch_rds_password(infra: InfraModel, outputs: Dict[str, Any]) -> str:
    """
    Fetch RDS password from AWS Secrets Manager.

    Args:
        infra: Infrastructure model
        outputs: Terraform outputs

    Returns:
        Database password
    """
    secret_arn = outputs["rds_master_user_secret_arn"]

    # Assume role if needed
    if infra.role_arn:
        sts = boto3.client("sts")
        assumed_role = sts.assume_role(
            RoleArn=infra.role_arn,
            RoleSessionName=f"beta9-secrets-{infra.environment_id}",
            ExternalId=infra.external_id,
        )
        credentials = assumed_role["Credentials"]

        secrets_client = boto3.client(
            "secretsmanager",
            region_name=infra.region,
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
    else:
        secrets_client = boto3.client("secretsmanager", region_name=infra.region)

    # Get secret value
    import json

    response = secrets_client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response["SecretString"])

    return secret["password"]


def _helm_install(
    release_name: str,
    chart: str,
    namespace: str,
    kubeconfig: Path,
    create_namespace: bool = False,
    repo_url: str = None,
    values: Dict[str, Any] = None,
) -> None:
    """
    Install a Helm chart.

    Args:
        release_name: Helm release name
        chart: Chart name or path
        namespace: Kubernetes namespace
        kubeconfig: Path to kubeconfig
        create_namespace: Whether to create namespace
        repo_url: Helm repository URL (for adding repo)
        values: Helm values dictionary
    """
    # Add repo if specified
    if repo_url:
        repo_name = chart.split("/")[0]
        subprocess.run(
            ["helm", "repo", "add", repo_name, repo_url],
            capture_output=True,
            check=True,
        )
        subprocess.run(["helm", "repo", "update"], capture_output=True, check=True)

    # Build helm install command
    cmd = [
        "helm",
        "install",
        release_name,
        chart,
        "--namespace",
        namespace,
        "--kubeconfig",
        str(kubeconfig),
        "--wait",
        "--timeout",
        "10m",
    ]

    if create_namespace:
        cmd.append("--create-namespace")

    # Add values if specified
    if values:
        # Write values to temporary file
        values_file = Path(tempfile.gettempdir()) / f"helm-values-{release_name}.yaml"
        with open(values_file, "w") as f:
            yaml.dump(values, f)

        cmd.extend(["--values", str(values_file)])

    logger.info(
        "Installing Helm chart",
        release=release_name,
        chart=chart,
        namespace=namespace,
    )

    result = subprocess.run(cmd, capture_output=True, check=True)

    logger.info("Helm chart installed", release=release_name)
