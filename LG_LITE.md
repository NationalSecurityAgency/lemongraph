# LG Lite

LG-Lite facilitates recursive batch job processing across multiple disparate systems. It implements the core functionality of LemonGrenade, but with fewer moving parts and reduced resource requirements.

# Quickstart

* install LG reqs
* run server:
	* `python3 -mLemonGraph.server -s`
* run example adapters:
	* `python3 examples/foo.py`
	* `python3 examples/bar.py`
	* `python3 examples/baz.py`
* submit some example jobs:
	* `python3 examples/submit.py 10`
* see also:
    * [async_adapters.py](examples/async_adapters.py)
    * [async_submit.py](examples/async_submit.py)
    * `test_lg_lite` in [test.py](test.py)

# Design

LG Lite extends LemonGraph and continues its disposable-central-index theme - no critical global saved state. As such, it collects no metrics and maintains no knowledge of adapter activity.

## Server

* maintains individual jobs as graphs
* maintains indexes of which adapters potentially have outstanding work
* responsible for round-robin-ing adapter requests through jobs, according to job priority

## Jobs

Jobs themselves are created/updated/deleted via the older `/graph` endpoint, and:

* can be individually enabled/disabled
* have a priority - low: _0_, high: _255_, default: _100_
* track task status across all adapters/queries
* declare zero or more adapter/query pair flows, each of which:
	* may have autotasking enabled (by default) or disabled:
		* uses logID bookmark to autogenerate tasks and feed pre-task queue of chains as job graph grows
		* may use LGQL template to further restrict query
	* may be manually driven
		* may use LGQL template to further restrict query
	* may be disabled (or re-enabled) - while disabled, flows:
		* can still be manually driven
		* will accept task results
		* will generate tasks to drain pre-task queue
		* will not exercise autotasking
	* may have a flow-specific default task timeout (_60_ seconds, use _0_ to disable automatic retry)
	* may have a flow-specific default task size upper limit (_200_)

## Adapters

* poll server for new tasks from any job
	* may request a specific query pattern
	* may provide a list of tasks to ignore
	* may provide a task timeout (seconds), after which a task may be reissued:
		* default is flow default, which defaults to _60_ seconds
		* use _0_ to disable automatic retry
	* may provide an upper limit for task size
	    * default is flow default, which defaults to 200 records
		* ignored when re-issuing existing tasks
	* are responsible for pausing before retrying if no tasks were available

* can touch tasks
	* updates task timestamp
	* if configured, timeout is extended

* post task results
	* updates task timestamp
	* task will be marked as _done_ unless otherwise specified

## Tasks

* are generated for adapters on demand
* contain one or more snapshots of node/edge chains matched by the associated adapter's query pattern
* have a _timestamp_ that can be updated manually, or automatically when [re]issued or results are posted
* have a _timeout_ after which _active_ tasks will be reissued (default _60_ seconds, set to _0_ to disable)
* have a _retries_ field that holds the number of times a task has been issued
* have a _details_ field that holds free-form data - intended use is to indicate why a task is in a particular state
* have _state_, which can be one of:
	* _active_ - initial state - task will be reissued if _timeout_ is non-zero and _timeout_ seconds have elapsed since its _timestamp_ was updated
	* _idle_ - set manually to prevent task from being automatically reissued
	* _done_ - set automatically (unless prevented) for _active_/_idle_ tasks that receive results
	* _retry_ - set manually to queue task for immediate reissue and promotion to _active_
	* _error_ - set manually to indicate task processing encountered an error
	* _void_ - set manually to indicate task is ignored
	* _deleted_ - pseudostate, set manually to delete task from job
