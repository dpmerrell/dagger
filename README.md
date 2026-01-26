# Dagger

A minimalist DAG (Directed Acyclic Graph) engine for building workflow managers in Python.

## Overview

Dagger is a lightweight, extensible framework for orchestrating computational workflows. It provides abstract interfaces that can be extended to run workflows across different environments:

- Python functions executing in RAM
- Shell scripts running in a cluster
- Containerized services on cloud platforms
- Custom implementations for your specific needs

## Installation

Requires Python 3.10+

```bash
pip install -e .
```

### Dependencies

- `loky` - Reusable process pool executor for multiprocessing
- `pathvalidate` - Cross-platform filepath validation
- `pytest` - Testing framework

## Core Concepts

### Datum

A `Datum` represents a piece of data passed between tasks. It follows a three-state lifecycle:

```
EMPTY → POPULATED → AVAILABLE
```

- **EMPTY**: No data pointer exists
- **POPULATED**: Contains a pointer to data (file path, object reference, etc.)
- **AVAILABLE**: Data has been verified to exist

Built-in implementations:
- `MemoryDatum` - Python objects stored in memory
- `FileDatum` - Files on the local filesystem

### Task

A `Task` represents a unit of computational work with defined inputs and outputs. Tasks follow a state machine:

```
WAITING → RUNNING → COMPLETE
                 ↘ FAILED
```

Built-in implementations:
- `FunctionTask` - Executes a Python function with in-memory I/O
- `PklTask` - Executes a Python function with pickle file I/O
- `ScriptTask` - Executes shell commands with file I/O

### WorkflowManager

The `WorkflowManager` orchestrates DAG execution:
1. Initializes workflow state
2. Launches ready tasks (dependencies complete, resources available)
3. Monitors running tasks
4. Handles completion and failure

## Quick Start

### Example 1: In-Memory Function Pipeline

```python
from dagger.core import FunctionTask, MemoryDatum, WorkflowManager

# Define functions that return dictionaries
def add_one(x=0):
    return {"output": x + 1}

def double(x=0):
    return {"output": x * 2}

def multiply(x=0, y=0):
    return {"output": x * y}

# Create input datum
x = MemoryDatum(pointer=3)

# Build a diamond DAG:
#       t0 (add_one)
#      /  \
#    t1    t2
#     \    /
#       t3
t0 = FunctionTask("t0", add_one,
                  inputs={"x": x},
                  outputs={"output": None})

t1 = FunctionTask("t1", add_one,
                  inputs={"x": t0["output"]},
                  outputs={"output": None})

t2 = FunctionTask("t2", double,
                  inputs={"x": t0["output"]},
                  outputs={"output": None})

t3 = FunctionTask("t3", multiply,
                  inputs={"x": t1["output"], "y": t2["output"]},
                  outputs={"output": None})

# Run the workflow
manager = WorkflowManager(t3)
manager.run()

# Access results
print(t3["output"].pointer)  # 40
```

### Example 2: Shell Script Pipeline

```python
from dagger.core import ScriptTask, FileDatum, WorkflowManager

# Create input files
input1 = FileDatum(pointer="input1.txt")
input2 = FileDatum(pointer="input2.txt")

# Define a script task
# Use {key} placeholders for inputs and outputs
concat_script = """
    cat {file1} {file2} > {result}
"""

task = ScriptTask("concat",
                  inputs={"file1": input1, "file2": input2},
                  outputs={"result": "output.txt"},
                  script=concat_script)

manager = WorkflowManager(task)
manager.run()
```

### Example 3: Pickle File Pipeline

```python
from dagger.core import PklTask, MemoryDatum, WorkflowManager

def process_data(data=None):
    return {"result": [x * 2 for x in data]}

input_data = MemoryDatum(pointer=[1, 2, 3, 4, 5])

task = PklTask("process",
               inputs={"data": input_data},
               outputs={"result": "output.pkl"})

manager = WorkflowManager(task)
manager.run()
```

## Resource Management

Tasks can declare resource requirements. The `WorkflowManager` respects these constraints when scheduling:

```python
task = FunctionTask("heavy_task", my_function,
                    inputs={...},
                    outputs={...},
                    resources={"gpu": 1, "memory_gb": 16})

manager = WorkflowManager(root_task,
                          resources={"gpu": 4, "memory_gb": 64})
manager.run()
```

## Architecture

```
dagger/
├── abstract/           # Abstract base classes (interfaces)
│   ├── datum.py        # AbstractDatum
│   ├── task.py         # AbstractTask
│   ├── workflow_manager.py  # AbstractManager
│   ├── communicator.py # AbstractCommunicator
│   ├── list_datum.py   # DatumList for collections
│   └── helpers.py      # DAG utilities
│
└── core/               # Concrete implementations
    ├── datum.py        # MemoryDatum, FileDatum
    ├── task.py         # FunctionTask, PklTask, ScriptTask
    └── workflow_manager.py  # WorkflowManager
```

## Extending Dagger

### Custom Datum Types

Implement these abstract methods:

```python
from dagger.abstract import AbstractDatum

class MyDatum(AbstractDatum):
    def _validate_format_logic(self) -> bool:
        """Validate the pointer format."""
        pass

    def _verify_available_logic(self) -> bool:
        """Check if the data actually exists."""
        pass

    def _clear_logic(self):
        """Delete/clear the underlying data."""
        pass

    def _quickhash(self):
        """Return a hash for change detection."""
        pass
```

### Custom Task Types

Implement these abstract methods:

```python
from dagger.abstract import AbstractTask

class MyTask(AbstractTask):
    def _initialize_outputs(self, output_dict) -> dict:
        """Convert output specs to Datum objects."""
        pass

    def _collect_inputs(self) -> dict:
        """Transform input Datums to task-specific format."""
        pass

    def _run_logic(self, collected_inputs):
        """Execute the task's computation."""
        pass

    def _quickhash(self):
        """Return a hash for change detection."""
        pass

    def _interrupt_cleanup(self):
        """Clean up after interruption."""
        pass

    def _fail_cleanup(self):
        """Clean up after failure."""
        pass
```

## Running Tests

```bash
pytest test/
```

## License

(c) David Merrell 2025
