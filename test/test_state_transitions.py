
import pytest
from dagger.abstract.task import AbstractTask, TaskState
from dagger.abstract.datum import AbstractDatum, DatumState
from dagger.core import MemoryDatum


class MinimalTask(AbstractTask):
    """Minimal AbstractTask for testing state transitions."""
    def _initialize_outputs(self, output_dict):
        return {"output": MemoryDatum(parent=self)}

    def _quickhash(self):
        return self.identifier

    def _collect_inputs(self):
        return {k: v.pointer for k, v in self.inputs.items()}

    def _run_logic(self, collected_inputs):
        self["output"].populate(f"result of task {self.identifier}")

    def _interrupt_cleanup(self):
        return

    def _fail_cleanup(self):
        return


####################################
# TaskState transitions
####################################

def test_task_initial_state():
    t = MinimalTask("t")
    assert t.state == TaskState.WAITING


@pytest.mark.parametrize("from_state,to_state", [
    (TaskState.WAITING, TaskState.RUNNING),
    (TaskState.WAITING, TaskState.COMPLETE),
    (TaskState.RUNNING, TaskState.COMPLETE),
    (TaskState.RUNNING, TaskState.FAILED),
    (TaskState.RUNNING, TaskState.WAITING),
    (TaskState.COMPLETE, TaskState.WAITING),
    (TaskState.FAILED, TaskState.WAITING),
])
def test_task_valid_transitions(from_state, to_state):
    t = MinimalTask("t")
    t._state = from_state
    t.state = to_state
    assert t.state == to_state


@pytest.mark.parametrize("from_state,to_state", [
    (TaskState.WAITING, TaskState.FAILED),
    (TaskState.COMPLETE, TaskState.RUNNING),
    (TaskState.COMPLETE, TaskState.FAILED),
    (TaskState.FAILED, TaskState.RUNNING),
    (TaskState.FAILED, TaskState.COMPLETE),
])
def test_task_invalid_transitions(from_state, to_state):
    t = MinimalTask("t")
    t._state = from_state
    with pytest.raises(ValueError, match="Invalid TaskState transition"):
        t.state = to_state


@pytest.mark.parametrize("same_state", list(TaskState))
def test_task_self_transitions(same_state):
    t = MinimalTask("t")
    t._state = same_state
    t.state = same_state
    assert t.state == same_state


####################################
# DatumState transitions
####################################

def test_datum_initial_state():
    d = MemoryDatum()
    assert d.state == DatumState.EMPTY


@pytest.mark.parametrize("from_state,to_state", [
    (DatumState.EMPTY, DatumState.POPULATED),
    (DatumState.POPULATED, DatumState.AVAILABLE),
    (DatumState.POPULATED, DatumState.EMPTY),
    (DatumState.AVAILABLE, DatumState.POPULATED),
    (DatumState.AVAILABLE, DatumState.EMPTY),
])
def test_datum_valid_transitions(from_state, to_state):
    d = MemoryDatum()
    d._state = from_state
    d.state = to_state
    assert d.state == to_state


def test_datum_invalid_empty_to_available():
    d = MemoryDatum()
    assert d.state == DatumState.EMPTY
    with pytest.raises(ValueError, match="Invalid DatumState transition"):
        d.state = DatumState.AVAILABLE


@pytest.mark.parametrize("same_state", list(DatumState))
def test_datum_self_transitions(same_state):
    d = MemoryDatum()
    d._state = same_state
    d.state = same_state
    assert d.state == same_state
