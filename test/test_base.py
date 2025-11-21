
from dagger.base import Task, TaskState, WorkflowManager

class MinimalTask(Task):
    """
    A minimal/do-nothing implementation of base.Task,
    strictly for testing purposes.
    """
    def run(self):
        return

    def terminate(self):
        return


class MinimalManager(WorkflowManager):
    """
    A minimal/do-nothing implementation of base.WorkflowManager,
    strictly for testing purposes.
    """
    def run(self):
        return


def test_base_task():
    # Construct two tasks; one depends on the other
    t1 = MinimalTask("t1")
    t2 = MinimalTask("t2", dependencies=[t1])

    # Check basic properties of the constructed tasks
    assert t2.identifier == "t2"
    assert t2.dependencies[0].identifier == "t1"

def test_base_manager():

    # Construct a workflow of 10 tasks,
    # chained together (i.e., linear graph)
    t0 = MinimalTask("t0")
    t = t0
    for i in range(9):
        t_new = MinimalTask(f"t{i+1}",
                            dependencies=[t]
                            )
        t = t_new
    m = MinimalManager(t)

    # Check basic properties of the constructed DAG
    assert m.root_task.identifier == "t9"
    assert m.validate_dag()

    # Introduce a loop into the workflow.
    # Ensure it fails to validate.
    t0.dependencies = [t]
    assert (not m.validate_dag())




