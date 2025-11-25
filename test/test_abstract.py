
from dagger.abstract import AbstractTask, TaskState, AbstractManager

class MinimalTask(AbstractTask):
    """
    A minimal/do-nothing implementation of base.AbstractTask,
    strictly for testing purposes.
    """
    def _run_logic(self):
        return

    def _interrupt_cleanup(self):
        return

    def _fail_cleanup(self):
        return

    def _check_complete_logic(self):
        return False


####################################
# TASK CONSTRUCTION AND COMPLETION
####################################
def test_base_task():
    # Construct two tasks; one depends on the other
    t1 = MinimalTask("t1")
    t2 = MinimalTask("t2", dependencies=[t1])

    # Check basic properties of the constructed tasks
    assert t2.identifier == "t2"
    assert t2.dependencies[0].identifier == "t1"

    # If we run t1, then t2 should be ready
    assert not t2.is_ready()
    t1.run()
    assert t1.state == TaskState.COMPLETE
    assert t2.is_ready()


#####################
# TASK FAILURE 
#####################
class ExceptionTask(MinimalTask):
    """
    This task simply raises an exception
    when we run it.
    """
    def _run_logic(self):
        raise Exception("Exception Task!")
        return

    def _fail_cleanup(self):
        self.failed = True

    def _check_complete_logic(self):
        return False

def test_base_task_failure():
    # When we run this task, it should go into
    # a FAILED state
    te = ExceptionTask("te")
    try:
        te.run()
    except:
        assert te.failed
        assert te.state == TaskState.FAILED
    else:
        assert False

#####################
# TASK INTERRUPTION
#####################
class InterruptTask(MinimalTask):
    """
    This task simply raises a KeyboardInterrupt
    when we run it.
    """
    def _run_logic(self):
        raise KeyboardInterrupt("Keyboard interrupt!")
        return

    def _interrupt_cleanup(self):
        self.interrupted = True
        return
    
    def _check_complete_logic(self):
        return False


def test_base_task_interrupt():
    # When we run this task, it should go back into
    # a WAITING state
    ti = InterruptTask("ti")
    ti.run()
    assert ti.interrupted
    assert ti.state == TaskState.WAITING

###############################
# WORKFLOW MANAGER
###############################
class MinimalManager(AbstractManager):
    """
    A minimal/do-nothing implementation of base.AbstractManager,
    strictly for testing purposes.
    """
    def run(self):
        return

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
    try:
        m.validate_dag()
    except:
        assert False
    
    # If we manually set t8's state to COMPLETE,
    # the enforce_incomplete() method should
    # reset it to WAITING.
    t8 = t.dependencies[0]
    assert t8.identifier == "t8"
    t8.state = TaskState.COMPLETE
    m.enforce_incomplete()
    assert t8.state == TaskState.WAITING

    # Introduce a loop into the workflow.
    # Ensure it fails to validate.
    t0.dependencies = [t]
    try:
        m.validate_dag()
    except ValueError:
        assert True
    else:
        assert False

