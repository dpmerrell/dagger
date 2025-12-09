"""
    core/helpers.py
    (c) David Merrell 2025

    Helper functions for the "core" implementation.

    Many of these functions are meant to be used in 
    methods for Task, WorkflowManager, and Datum
    implementations.
"""

def collect_dependencies(input_dict, dependency_ls):
    """
    Collect all dependencies of a task, given 
    (A) a dictionary of input Datums and
    (B) a list of explicit Task dependencies.
    """
    parents = [inp.parent for inp in input_dict.values()]
    deps = set(dependency_ls) | set((p for p in parents if p is not None))
    return list(deps)
