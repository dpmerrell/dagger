"""
    abstract/helpers.py
    (c) David Merrell 2025

    Helper functions for the abstract base classes. 

    Many of these functions are meant to be used in 
    methods for Task, WorkflowManager, and Datum
    implementations.
"""

from collections import defaultdict

###################################################
# Task helpers
###################################################
def collect_dependencies(input_dict, dependency_ls):
    """
    Collect all dependencies of a task, given 
    (A) a dictionary of input Datums and
    (B) a list of explicit Task dependencies.
    """
    parents = [inp.parent for inp in input_dict.values()]
    deps = set(dependency_ls) | set((p for p in parents if p is not None))
    return list(deps)


################################################
# Workflow manager helpers
################################################
def cycle_exists(end_task):
    """
    Use DFS to detect cycles in task dependencies.
    ancestors: list of task IDs.
    visited: set of task IDs.
    """
    return _rec_cycle_exists(end_task, [], set())

def _rec_cycle_exists(task, ancestors, visited):
    """
    Use DFS to detect cycles in task dependencies.
    ancestors: list of task IDs.
    visited: set of task IDs.
    """
    # Base case: check if this task is in
    # its own ancestors (cycle exists)
    if task.identifier in ancestors:
        return True

    # Base case: check if this task has already
    # been visited/explored
    if task.identifier in visited:
        return False

    # Recursive case: add this task to the
    # ancestor stack and explore its
    # dependencies.
    ancestors.append(task.identifier)
    for d in task.dependencies:
        if _rec_cycle_exists(d, ancestors, visited):
            return True
    # If none of the dependencies led to 
    # a cycle, remove this task from ancestors
    # and mark it as visited/explored
    ancestors.pop()
    visited.add(task.identifier)
    return False

def construct_adj_list(end_task):
    """
    Construct an 'adjacency list' representation of 
    the task DAG
    """
    adj_list = defaultdict(set)

    _rec_construct_adj_list(end_task, adj_list, set())
    adj_list = {k: list(v) for k, v in adj_list.items()}
    adj_list[end_task] = []

    return adj_list

def _rec_construct_adj_list(task, adj_list, visited):
    """
    Recursive core of `construct_adj_list`
    """
    # Recurrent case: has dependencies
    for dep in task.dependencies:
        adj_list[dep].add(task)
        if dep not in visited:
            _rec_construct_adj_list(dep, adj_list, visited)

    visited.add(task)


#########################################
# Resource helpers
#########################################

def resources_available(res, query):
    """
    Return a bool indicating whether available resources (`res`)
    are sufficient to satisfy a demand (`query`).

    Whenever `res` is missing a key, we assume it has *infinite*
    supply of that key.

    Whenever `query` is missing a key, we assume it requires *zero* of
    that resource.
    """
    # If any of the query resources exceed
    # the available resources, then return False
    for qk, qv in query.items():
        if qk in res.keys():
            if res[qk] < qv:
                return False

    return True

def decrement_resources(res, decrement):
    """
    Update the resources dictionary with a 'decrement'.
    Return the updated resources dictionary.
    """
    for dk, dv in decrement.items():
        if dk in res.keys():
            res[dk] -= dv
    return res

def increment_resources(res, increment):
    """
    Update the resources dictionary with an 'increment'.
    Return the updated resources dictionary.
    """
    for ik, iv in increment.items():
        if ik in res.keys():
            res[ik] += iv
    return res


