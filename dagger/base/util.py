

from uuid import uuid4
from enum import Enum
import datetime

DAGGER_START_FLAG = "__DAGGER_START__"
DAGGER_END_FLAG = "__DAGGER_END__"

"""
    This enum encodes mutually exclusive and exhaustive
    states for Tasks. Under normal DAG execution, a Task
    will undergo the following transitions:
   
    NOT_RUN -> RUNNING -> (COMPLETE *or* FAILED)
    
    Note that these TaskStates are meant to represent a
    "local" concept of task completion; they should be 
    a function of (A) the task itself, (B) its input data
    and (C) its output data. For example, it could be
    based on the timestamps of input and output files.
    (The Controller, in contrast, uses a "global" 
    concept of completion by ensuring all upstream 
    tasks are complete.)
"""
class TaskState(Enum):
    COMPLETE = 0
    RUNNING = 1
    NOT_RUN = 2
    FAILED = 3

class TaskFailedException(Exception):
    pass

def now_timestamp():
    return datetime.datetime.now().isoformat()

def min_timestamp():
    return datetime.datetime.min.isoformat()

def generate_uid():
    return uuid4()


