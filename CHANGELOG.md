# Changelog

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
