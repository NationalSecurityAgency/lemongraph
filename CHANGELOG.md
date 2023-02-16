# Changelog

## [3.4.2] - 2023-02-15
- update Dockerfile to force test stage to build
- update alpine to 3.17
- update to latest LMDB

## [3.4.1] - 2022-08-22
- add 'Access-Control-Allow-Origin: *' HTTP header to responses if 'Origin' header is present in request

## [3.4.0] - 2022-05-16
- http client automatcially reconnects if disconnected - makes tests pass for macOS
- promote 'retry' task state to real state instead of marking as active with a very short timeout
- add 'idle' and 'void' tasks states to mark them as 'maybe do later' and 'will not do', respectively

## [3.3.0] - 2022-04-22
- allow full task contents to be pulled via /lg/task/<job_uuid>
- allow filtering by node/edge IDs on /lg/task/<job_uuid>
- add node/edge count, enabled-ness, and priority to job info object in /lg/delta/<job_uuid> response

## [3.2.12] - 2022-04-12
- fix node/edge indexing for queries having implicit nodes/edges

## [3.2.11] - 2022-04-05
- correct docs for tasks set to retry
- update python -> python3 in docs
- fix uptime on /lg/status for python2

## [3.2.10] - 2022-03-29
- fix Dockerfile and .dockerignore for downstream integration

## [3.2.9] - 2022-03-25
- merged Dockerfile.* into single Dockerfile
- added __version__ attribute to LemonGraph, and /lg/status endpoint

## [3.2.8] - 2022-03-18
- properly close graph collection index on worker process exit

## [3.2.7] - 2022-03-17
- fix bug affecting streaming graph view introduced in 6865502b

## [3.2.6] - 2022-03-15
- Reapply d3 transformation on graph refresh

## [3.2.5] - 2022-03-15
- Updated Dockerfiles: alpine 3.12 => 3.14, debian 10 => 11

## [3.2.4] - 2022-03-15
- Fix logic bug on /lg/delta/<job> where marked/filtered nodes were not getting updated correctly

## [3.2.3] - 2022-03-14
- Allow time-based uuids to be passed to created_before/created_after parameters

## [3.2.2] - 2022-03-08
- Fixed binary graph upload endpoint
- Cleaned up window resize on /view/<uuid> so it nolonger alters graph zoom
- Fixed issue on /view/<uuid> where streaming graph updates could cause forcegraph physics to go bananas

## [3.2.1] - 2022-02-26
- Fixed newly introduced bug with node filtering on /view/<uuid> endpoint

## [3.2.0] - 2022-02-26
- Added -C server option to prevent static content caching (for testing purposes)
- Added /lg/delta/<uuid> endpoint for pulling changes to a job graph
- Added new default d3v4a style to /view/<uuid> endpoint that uses above job delta endpoint (pass `style=d3v4` to get previous)

## [3.1.0] - 2021-10-20
- Reworked event loop, added support for HTTP/1.1 keepalive

## [3.0.1] - 2021-09-02
- Fix bug introduced in 3.0.0 where 'seed', 'depth', or 'cost' keys present in job meta would cause a crash

## [3.0.0] - 2021-08-31

- Breaking change in server:
  - edges no longer have an initial implicit cost of 1.0
  - therefore, depth calculations no longer cascade through edges with no explicitly assigned cost
  - shortest path in algorithms.py no longer defaults to an edge cost of 1.0

## [2.1.0] - 2021-07-01

- Added feature to query language to allow streaming queries to trigger on any field value change.

## [2.0.1] - 2021-06-15

- Fix bug where fields requested by lg_lite adapters might not actually get included.

## [2.0.0] - 2021-06-11

- Breaking change in server - log commit timestamp into `last_modified` field for nodes and edges.

## [1.2.1] - 2021-05-17

- Fixed bug on `MatchLGQL.reduce()` and `/lg/test` endpoint

## [1.2.0] - 2021-04-16

- Added /lg/test endpoint for parsing/checking LGQL query patterns

## [1.1.0] - 2021-04-13

- Update to latest LMDB
- In D3 view:
  - Resize SVG viewport as window is resized
  - Suspend polling when 'Pause' is clicked, resume when 'Continue' is clicked
  - Added [json-view](https://github.com/pgrabovets/json-view) as an external dependency, so we can:
  - Pop up rendered JSON properties and highlight node/edge when clicked

## [1.0.3] - 2021-04-06

- Allow node/edge filtering on D3 view

## [1.0.2] - 2021-04-06

- Fixed/improved error handling for REST endpoints
- Fixed javascript crash in D3 view when encountering null property values
- Suppressed error when attemping to pull tasks on an LG-Lite job that hasn't issued any
- Cleaned up some docs

## [1.0.1] - 2021-03-31

- Fixed timestamp/timeout bug
- Fixed for Python 2 on macOS - use select.select() instead of select.poll()
- Example adapter cleanup

## [1.0.0] - 2021-03-30

- Lots of __breaking changes__ - old graph binaries are incompatible
  - internal changes:
    - uint encoding is more compact
      - then: first byte determined number of remaining bytes
      - now: number of up-to-8 consecutively set MSB bits determines number of remaining bytes
    - uuids now get packed as binary, such that they sort by embedded timestamp
  - lg-lite is largely rewritten
    - lg tasks are persistent by default now, but get marked as 'done' upon receipt of results
      - task state can be set to 'active', 'done', 'error', 'retry', or 'delete'/'deleted'
        - 'retry' is a pseudo-state that sets task state, timestamp, and timeout such that it is immediately available for reissue
        - setting to task state to 'delete' or 'deleted' will remove task entirely
    - lg tasks have moved
      - from: `/lg/adapter/<ADAPTER>/<JOB_UUID>/<TASK_UUID>`
      - to: `/lg/task/<JOB_UUID>/<TASK_UUID>`
    - lg job task list can be pulled or bulk-updated via: `/lg/task/<JOB_UUID>`
      - can be filtered by task uuid[s], adapter[s], and state[s]
    - lg tasks have an internal timeout field now
      - set via initial task fetch and subsequent updates
    - lg adapter interface:
      - __limit__ and __timeout__ fields have been added to per-job adapter configs
        - adapters may still override on task fetch
      - task fetch:
        - __blacklist__ is now __ignore__
        - __min_age__ mechanism is replaced with task-specific timeout
      - task results - to allow multiple results for a given task:
        - replace old: `"consume": false`
        - with new: `"state": "active"` (defaults to: `"done"`)
        - by default, tasks not in __active__ state will reject task results
          - this behavior can be overridden - see [RESTAPI.md](RESTAPI.md#lgtaskjob_uuidtask_uuid)
    - internal timestamps and timeouts are stored as tenths of a second now instead of seconds
- Additional changes include:
  - http server supports unix domain sockets, making testing rest endpoints easier
  - example clients can all connect on unix domain sockets
  - D3 graph view updates:
    - uses curved instead of straight lines for edge links now
      - multiple edges between the same nodes get distinct paths
      - loopback edges use arcs
    - graphs poll server once per second and will automatically load new graph data
