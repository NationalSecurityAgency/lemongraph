# LG Lite

This aims to re-implement the core functionality of LemonGrenade with fewer moving parts.

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
* responsible for round-robin-ing through jobs, according to job priority

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
	* may provide a task timeout (seconds), after which a task may be re-issued:
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
