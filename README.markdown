# dagger

A minimalist DAG workflow engine.

Build a `Workflow` of `Task`s that pass `Datum`s to each other.
And then run it with a `Controller`.

Designed to be as generic as possible, but easily extended to new platforms and environments.
For example, you could extend `dagger` to run workflows...
* ...of python functions on your local host
* ...of shell scripts on a cluster
* ...of containerized services on a cloud platform
* etc.
