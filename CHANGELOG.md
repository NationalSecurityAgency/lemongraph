# Changelog

## [1.0.1]

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
