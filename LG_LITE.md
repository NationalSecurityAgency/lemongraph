# LG Lite

This aims to re-impliment the core functionality of LemonGrenade with fewer moving parts.

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

# REST API

## /lg

* __GET__ - Return number of jobs that might have work, per query per adapter

## /lg/config/{job_uuid}

* __GET__ - Return _job\_uuid_'s config/status per query per adapter
* __POST__ - Update one or more adapter configs
	* payload (see below):

			{
				"ADAPTER1": config,
				"ADAPTER2": config
			}

## /lg/config/{job_uuid}/{adapter}

* __GET__ - Return _job\_uuid_'s config/status per query for _adapter_
* __POST__ - Update _adapter_'s config
	* payload - two styles:
		* set/update primary query config:
			* __query__: if present, sets as primary flow, any missing fields are inherited from previous primary, and previous primary's autotasking is disabled
			* __filter__: query filter to append when generating new tasks
			* __enabled__: enable/disable this query entirely
			* __autotask__: enable/disable autotasking for this query (use of _pos_ as bookmark against logID to generate tasks)
			* __pos__: current logID bookmark for this query for this job

					{
						"query": string,
						"filter": string,
						"enabled": boolean,
						"autotask": boolean,
						"pos": integer
					}

		* update zero or more secondary queries (see above, except _query_ must be present for each, and primary query is left as is):

				[
					config,
				]

## /lg/adapter/{adapter}

* __GET__/__POST__ - Return task for _adapter_ based on job priority and parameters and/or posted body
	* parameters/payload - all fields are optional:
		* __query__ - limit tasks to be for a specific _query_ or queries (else round-robins through available)
		* __limit__ - limit task size to _limit_ records (__200__, does not apply to re-issued tasks)
		* __min\_age__ - skip tasks if touched less than _min\_age_ seconds ago (__60__)
		* __blacklist__ - skip provided list of tasks

				{
					"query": string,
					"limit": integer,
					"min_age": integer,
					"blacklist": list
				}

## /lg/adapter/{adapter}/{job_uuid}

* __POST__ - Manually exercise _adapter_ against _job\_uuid_, optionally using a query filter
	* returns number of chains injected into task queue
	* parameters/payload:
		* __query__ - optionally supply a specific _query_ (defaults to _adapter_'s primary query)
		* __filter__ - optionally supply additional query _filter_ to restrict (__None__)

				{
					"query": string,
					"filter": string
				}

## /lg/adapter/{adapter}/{job_uuid}/{task_uuid}

* __GET__ - Update task timestamp, return task
* __HEAD__ - Update task timestamp
* __DELETE__ - Delete task
* __POST__ - Post task results, and delete task (default) or update task timestamp
	* payload:
		* __consume__: if true, delete task successful ingest, else just update task timestamp (default: _true_)
		* __nodes__: list of node objects
		* __edges__: list of edge objects
		* __chains__: list of node[/edge/node]* chains
		* __meta__: job graph metadata
		* __adapters__: job's adapters - see `/lg/config/{job_uuid}` endpoint above

				{
					"consume": boolean,
					"nodes": list,
					"edges": list,
					"chains": list,
					"meta": dict,
					"adapters": dict
				}
