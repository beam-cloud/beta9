"""CDKTF executor for synthesizing and applying infrastructure."""

import json
import subprocess
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional

from cdktf import App

from models.config import InfraModel, CloudProvider
from .aws_stack import Beta9AwsStack
from .state import ensure_state_backend


@dataclass
class ApplyResult:
    """Result of infrastructure apply operation."""

    success: bool
    outputs: Dict[str, Any]
    error: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None


@dataclass
class DestroyResult:
    """Result of infrastructure destroy operation."""

    success: bool
    error: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None


class CDKTFExecutor:
    """Executor for CDKTF operations."""

    def __init__(self, workdir: Path):
        """
        Initialize CDKTF executor.

        Args:
            workdir: Working directory for CDKTF operations
        """
        self.workdir = workdir
        self.workdir.mkdir(parents=True, exist_ok=True)

    def synth(self, infra: InfraModel) -> None:
        """
        Synthesize infrastructure to Terraform JSON.

        Args:
            infra: Infrastructure model

        Raises:
            Exception: If synthesis fails
        """
        # Create CDKTF app
        app = App(outdir=str(self.workdir / "cdktf.out"))

        # Create stack based on cloud provider
        if infra.cloud_provider == CloudProvider.AWS:
            Beta9AwsStack(app, "beta9-aws", infra)
        elif infra.cloud_provider == CloudProvider.GCP:
            # TODO: Implement GCP stack
            raise NotImplementedError("GCP stack not yet implemented")
        else:
            raise ValueError(f"Unsupported cloud provider: {infra.cloud_provider}")

        # Synthesize
        app.synth()

        print(f"Synthesized infrastructure to {self.workdir / 'cdktf.out'}")

    def apply(self, infra: InfraModel, auto_approve: bool = True) -> ApplyResult:
        """
        Apply infrastructure changes.

        Args:
            infra: Infrastructure model
            auto_approve: Whether to auto-approve changes

        Returns:
            ApplyResult with outputs or error
        """
        try:
            # Ensure state backend exists
            ensure_state_backend(infra)

            # Synthesize
            self.synth(infra)

            # Get stack directory
            stack_dir = self.workdir / "cdktf.out" / "stacks" / "beta9-aws"

            # Initialize Terraform
            self._run_terraform(["init"], cwd=stack_dir)

            # Apply changes
            cmd = ["apply", "-json"]
            if auto_approve:
                cmd.append("-auto-approve")

            self._run_terraform(cmd, cwd=stack_dir)

            # Get outputs
            outputs = self._get_outputs(stack_dir)

            return ApplyResult(success=True, outputs=outputs)

        except subprocess.CalledProcessError as e:
            return ApplyResult(
                success=False,
                outputs={},
                error=f"Terraform command failed: {e.cmd}",
                error_details={
                    "returncode": e.returncode,
                    "stdout": e.stdout.decode() if e.stdout else None,
                    "stderr": e.stderr.decode() if e.stderr else None,
                },
            )
        except Exception as e:
            return ApplyResult(
                success=False,
                outputs={},
                error=str(e),
                error_details={"type": type(e).__name__},
            )

    def destroy(self, infra: InfraModel, auto_approve: bool = True) -> DestroyResult:
        """
        Destroy infrastructure.

        Args:
            infra: Infrastructure model
            auto_approve: Whether to auto-approve destruction

        Returns:
            DestroyResult with error if failed
        """
        try:
            # Synthesize (needed to get current configuration)
            self.synth(infra)

            # Get stack directory
            stack_dir = self.workdir / "cdktf.out" / "stacks" / "beta9-aws"

            # Initialize Terraform (in case state changed)
            self._run_terraform(["init"], cwd=stack_dir)

            # Destroy infrastructure
            cmd = ["destroy", "-json"]
            if auto_approve:
                cmd.append("-auto-approve")

            self._run_terraform(cmd, cwd=stack_dir)

            return DestroyResult(success=True)

        except subprocess.CalledProcessError as e:
            return DestroyResult(
                success=False,
                error=f"Terraform command failed: {e.cmd}",
                error_details={
                    "returncode": e.returncode,
                    "stdout": e.stdout.decode() if e.stdout else None,
                    "stderr": e.stderr.decode() if e.stderr else None,
                },
            )
        except Exception as e:
            return DestroyResult(
                success=False,
                error=str(e),
                error_details={"type": type(e).__name__},
            )

    def _run_terraform(self, args: list[str], cwd: Path) -> subprocess.CompletedProcess:
        """
        Run terraform command.

        Args:
            args: Terraform command arguments
            cwd: Working directory

        Returns:
            Completed process

        Raises:
            subprocess.CalledProcessError: If command fails
        """
        # Check if terraform is available
        if not shutil.which("terraform"):
            raise RuntimeError("terraform command not found. Please install Terraform.")

        cmd = ["terraform"] + args

        print(f"Running: {' '.join(cmd)} in {cwd}")

        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            check=True,
            env={**subprocess.os.environ, "TF_IN_AUTOMATION": "1"},
        )

        return result

    def _get_outputs(self, stack_dir: Path) -> Dict[str, Any]:
        """
        Get Terraform outputs.

        Args:
            stack_dir: Stack directory

        Returns:
            Dictionary of outputs
        """
        result = self._run_terraform(["output", "-json"], cwd=stack_dir)
        outputs_raw = json.loads(result.stdout)

        # Extract values from Terraform output format
        outputs = {}
        for key, value in outputs_raw.items():
            outputs[key] = value.get("value")

        return outputs

    def cleanup(self) -> None:
        """Remove working directory."""
        if self.workdir.exists():
            shutil.rmtree(self.workdir)
            print(f"Cleaned up working directory: {self.workdir}")


# Convenience functions
def synth_infra(infra: InfraModel, workdir: Path) -> None:
    """
    Synthesize infrastructure to Terraform JSON.

    Args:
        infra: Infrastructure model
        workdir: Working directory
    """
    executor = CDKTFExecutor(workdir)
    executor.synth(infra)


def apply_infra(infra: InfraModel, workdir: Path) -> ApplyResult:
    """
    Apply infrastructure changes.

    Args:
        infra: Infrastructure model
        workdir: Working directory

    Returns:
        ApplyResult
    """
    executor = CDKTFExecutor(workdir)
    return executor.apply(infra)


def destroy_infra(infra: InfraModel, workdir: Path) -> DestroyResult:
    """
    Destroy infrastructure.

    Args:
        infra: Infrastructure model
        workdir: Working directory

    Returns:
        DestroyResult
    """
    executor = CDKTFExecutor(workdir)
    result = executor.destroy(infra)

    # Cleanup workdir after successful destroy
    if result.success:
        executor.cleanup()

    return result
