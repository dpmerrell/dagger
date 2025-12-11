"""
    core/helpers.py
    (c) David Merrell 2025

    Helper functions for the "core" implementation.

    Many of these functions are meant to be used in 
    methods for Task, WorkflowManager, and Datum
    implementations.
"""

from collections import defaultdict

def collect_dependencies(input_dict, dependency_ls):
    """
    Collect all dependencies of a task, given 
    (A) a dictionary of input Datums and
    (B) a list of explicit Task dependencies.
    """
    parents = [inp.parent for inp in input_dict.values()]
    deps = set(dependency_ls) | set((p for p in parents if p is not None))
    return list(deps)


def construct_edge_lists(task, start_node="START"):
    """
    Construct an 'edge-list' representation of 
    the task DAG; include an artificial "START"
    node
    """
    edge_sets = defaultdict(set)
    _construct_edge_sets(task, edge_sets, visited=set(), 
                                          start_node=start_node)
    edge_sets = {k: list(v) for k, v in edge_sets.items()}
    return edge_sets


def _construct_edge_sets(task, edge_sets, visited=set(), start_node="START"):
    """
    Recursive core of `construct_edge_lists`
    """
    # base case: no dependencies
    if len(task.dependencies) == 0:
        edge_sets[start_node].add(task)

    # Recurrent case: has dependencies
    for dep in task.dependencies:
        if dep not in visited:
            edge_sets[dep].add(task)
            _construct_edge_sets(dep, edge_sets, visited=visited)

    visited.add(task)

