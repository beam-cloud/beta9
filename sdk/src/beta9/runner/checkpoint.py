import importlib
import time
from pathlib import Path

from beta9.runner.common import config, workers_ready

CHECKPOINT_SIGNAL_FILE = "/criu/READY_FOR_CHECKPOINT"
CHECKPOINT_COMPLETE_FILE = "/criu/CHECKPOINT_COMPLETE"
CHECKPOINT_CONTAINER_ID_FILE = "/criu/CONTAINER_ID"
CHECKPOINT_CONTAINER_HOSTNAME_FILE = "/criu/CONTAINER_HOSTNAME"


def _reload_config():
    # Once we have set the checkpoint signal file, wait for checkpoint to be complete before reloading the config
    while not Path(CHECKPOINT_COMPLETE_FILE).exists():
        time.sleep(1)

    # Reload config that may have changed during restore
    config.container_id = Path(CHECKPOINT_CONTAINER_ID_FILE).read_text()
    config.container_hostname = Path(CHECKPOINT_CONTAINER_HOSTNAME_FILE).read_text()
    print(f"container_id: {config.container_id}")
    print(f"container_hostname: {config.container_hostname}")


def wait_for_checkpoint():
    with workers_ready.get_lock():
        workers_ready.value += 1

    if workers_ready.value == config.workers:
        Path(CHECKPOINT_SIGNAL_FILE).touch(exist_ok=True)
        return _reload_config()

    while True:
        with workers_ready.get_lock():
            if workers_ready.value == config.workers:
                break
        time.sleep(1)

    return _reload_config()


def run_generic_checkpoint_condition():
    print("run_generic_checkpoint_condition")
    print(config.checkpoint_condition)
    if config.checkpoint_condition is None:
        return

    module, func = config.checkpoint_condition.split(":")
    target_module = importlib.import_module(module)
    method = getattr(target_module, func)

    while True:
        try:
            if method():
                break
        except Exception as e:
            print(f"Error in checkpoint condition: {e}")

        print("Waiting for checkpoint condition to be met...")
        time.sleep(1)

    """
    Considerations:
    1. Do we need to block the main process from receiving requests or starting its main processing job? (what happens if we checkpoint in the middle of a request?)
    2. How do we make sure that we don't reload the checkpoint condition after it has been checkpointed?
    3. How do we make sure that we don't load the checkpoint after it has been loaded?
    
    When we start up the container, we already know if its attempting a checkpoint or starting from a checkpoint.
    If its attempting a checkpoint, we disable the ingress/egress traffic and wait for the checkpoint condition to be met.
    If its starting from a checkpoint, we can just keep the ingress/egress traffic running before we start from the checkpoint.
    
    Asssuming the process is does not require any network access, for example a training job, 
    we can actually add a helper that the USER can use to check and block their process until its checkpointed.
    """

    Path(CHECKPOINT_SIGNAL_FILE).touch(exist_ok=True)
    return _reload_config()


if __name__ == "__main__":
    run_generic_checkpoint_condition()
