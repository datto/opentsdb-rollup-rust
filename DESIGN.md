
Components
==========

There are three major components of the system:

* the Consumer Workers, which receive and parse metrics from kafka and sends them to the rollup workers,
* the Rollup Workers, which add metrics to rollup windows and push rollups to kafka, and
* the Coordinator, which directs checkpoints.

Components communicate via Rust's mpsc channels.

Consumer Worker
---------------

Each consumer worker has a kafka consumer. The consumer worker polls from the kafka consumer,
parses the messages from telegraf format into individual metric objects, then sends the metrics
to the rollup workers.

Consumers compute a hash of the metric's "key" (metric name plus tags) to determine which worker
to send to, so that each metric consistently goes to one worker and one window.

Rollup Workers
--------------

Rollup workers receive metrics from the consumer and add their values to their corresponding
in-memory windows. They also keep track of event time, and write rolled up via a kafka producer
when the event time crosses the window closing time.

As mentioned above, each worker only handles part of all the metrics, partitioned by a hash of the
metric's key.

Coordinator
-----------

The coordinator runs on the main thread. It sets up and starts all of the other workers.
After that, it usually just sleeps, but will wake up every so often
to perform a checkpoint (see below). It also listens for an interrupt signal (ctrl+c or sigterm)
to gracefully exit the program and do a checkpoint on its way out.

Checkpointing and Reliability
=============================

There are a few reliability concerns with this system:

* Windows are frequently accessed to add new metrics into them, so they should be stored in memory.
  However this leaves us vulnerable to losing data if the program exits.
* Losing one or two metrics isn't too bad. However there are windows open for very long terms (ex.
  24h). Crashing and losing state too often would mean those windows would frequently get invalid
  rollups.
* Restoring the state can't be done solely via rolling back kafka offsets since the current values
  of the window also needs to be restored.
* The entire state of all windows is huge, due to the high cardinality of metrics we ingest.
  Writing out the entire state thus takes a significant amount of time.

To preserve state in the face of failure, this system writes out checkpoint at regular intervals; a file
containing all of the current windows as well as the kafka offsets. If the process exits, the program
can start over at state of the latest checkpoint. No state will be lost since it's preserved on disk, and
while windows may be resubmitted, OpenTSDB will update data points for duplicate keys at the same timestamp.

Gathering and writing the state is a bit complicated when multiple threads are involved. The coordinator's
main job is to manage this.

Checkpointing
-------------

Checkpointing follows a pause-write-unpause cycle.

When the coordinator determines that it is time to checkpoint, the following steps happen:

1. The coordinator sends a pause message to each consumer worker. The consumers receive that message,
   sends a reply to the coordinator indicating that it is now pauses, and stops ingest, waiting for
   the coordinator to resume it.
2. Once all consumer workers are paused, the coordinator sends a message to each rollup worker asking
   for their state. The coordinator uses the same channel as the metrics, so any in-flight metrics will
   be processed before handing over the state.
   
   To prevent copying the windows in memory, the rollup workers store their state in a Rust atomic reference
   counter (Arc). Usually the rollup windows have lock-free mutable access to the state, since it has the only
   reference to the state. The rollup window provides the coordinator another reference to the state, allowing
   the coordinator to read it without copying, but also locks the rollup window from updating it for the duration
   of the checkpoint.
3. The coordinator merges each individual state into one state and writes it out to disk.
   
   The "merging" is done via a custom serializer that iterates and writes each rollup's state in sequence, to avoid
   copying.
   
   To do an atomic replacement of the previous checkpoint, the coordinator writes to an intermediate file, flushes it,
   then renames the intermediate file over the old checkpoint, which is an atomic operation on most filesystems.
4. Once writing is complete, the coordinator drops its references to the state (allowing the rollup workers mutable
   access again) and sends a resume message to each of the consumer workers, resuming the flow of data.

TODO
====

Checkpoint design issues
------------------------

* What to do with kafka offsets? Let kafka autocommit do its own thing while we track offsets ourselves in the
  checkpoints? Commit offsets only as part of checkpointing? (Resolved: commit during checkpointing)
* Need to figure out how to start workers at a specific kafka offset. Easy with manual partition assignment,
  but then we forego kafka's automatic balancing (which might take relative load into account?). Can assign
  offsets when receiving assigned partitions too, but must only happen for first read, and can be race-y if
  a rebalance happens early on. (Resolved: Manual partition assignment, distributing partitions evenly among
  threads)
* For automatic balancing, how does kafka know where new consumers should start? If it's current offsets,
  need to ensure duplicate data isn't written, esp. when committing only at checkpoints. (Resolved: use
  manual assignment)
* For manual balancing, how to detect and handle new partitions being added? (Resolved: restart consumer, since
  it should not happen too often)

Error Handling
--------------

This is currently a bit sloppy. Lots of environmental issues will cause panics when they should cause more user-
friendly errors instead, mostly because errors in worker threads would need to be passed to the main thread.

The panic behavior is set to `abort` in the `Cargo.toml` file so that panics in the worker threads immediately
exit the process; otherwise the main thread would have to poll its children threads, which is inconvenient.

IO errors during checkpointing do _not_ cause a panic; the error is simply logged and ignored, and ingest
is resumed.
