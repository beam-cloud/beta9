from beta9.runner.container import task_status
from beta9.type import TaskStatus


def test_pod_task_status_tracks_entrypoint_exit_code():
    assert task_status(0, killed=False) is TaskStatus.Complete
    assert task_status(1, killed=False) is TaskStatus.Error
    assert task_status(137, killed=False) is TaskStatus.Error


def test_terminated_pod_task_is_cancelled():
    assert task_status(-9, killed=True) is TaskStatus.Cancelled


def test_successful_exit_wins_over_late_termination_signal():
    assert task_status(0, killed=True) is TaskStatus.Complete
