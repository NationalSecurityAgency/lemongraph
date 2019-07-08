# LG Lite

This aims to re-implement the core functionality of LemonGrenade with fewer moving parts.

# Quickstart

* install LG reqs
* install python's `requests` module
* run server:
	* `python -mLemonGraph.server`
* run example adapters:
	* `python examples/foo.py`
	* `python examples/bar.py`
* submit some example jobs:
	* `python examples/submit.py 10`

# Design

LG Lite extends LemonGraph and continues its disposable-central-index theme - no critical global saved state. As such, it collects no metrics and maintains no knowledge of adapter activity.

## Server

* maintains individual jobs as graphs, which internally track adapter progress
* maintains indexes of which adapters potentially have outstanding work
* responsible for round-robin-ing through adapter queries as well as jobs

## Adapters

* poll for new tasks
	* may request a specific query pattern
	* may provide a blacklist of tasks
	* may provide a minimum age (seconds):
		* tasks issued less than _N_ seconds ago will not be returned
	* are responsible for pausing before retrying if no tasks were available

* can touch tasks
	* updates task timestamp
	* keeps them from being re-issued prematurely, if minimum age is used above

* post task results
	* task will be consumed (deleted) unless otherwise specified
	* updates task timestamp unless consumed

## Jobs

Jobs themselves are created/updated/deleted via the existing `/graph` endpoint, and:

* have a priority - low: _0_, high: _255_, default: _100_
* can be enabled/disabled entirely
* declare zero or more adapter/query pairs, each of which:
	* track outstanding tasks
	* track length of pre-task queue of chains
	* may use LGQL templating to further extend query
	* may be disabled (or re-enabled):
		* if disabled, accepts results, can be manually driven, but won't issue additional autotasks
	* may have autotasking enabled or disabled:
		* uses logID bookmark to autogenerate tasks
	* may be manually driven, optionally using LGQL templating to extend
