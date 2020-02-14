This is a program for computing aggregates of OpenTSDB metrics and storing them using OpenTSDB's rollup feature.

This reads metrics in Telegraf format from one or more Kafka topics, and writes the resulting rollups to a Kafka topic, which can be ingested via [opentsdb-rpc-kafka](https://github.com/OpenTSDB/opentsdb-rpc-kafka).

Usage
-----

Run with `--help` for up-to-date parameters.

This program is meant to be run as a persistent service, under a daemon that can restart it as needed (ex. systemd). Datto runs this as an Apache Yarn service, so that the service can be relocated to another server if necessary.

If adding a new partition to a topic, _stop the rollup program first_. The program assigns partitions to workers on start, so if you add a new partition while it's running, it won't read from that partition.

Don't run multiple instances of this program over the same topic; they will get different conflicting results.

This program supports multiple threads, though it does not support multiple processes or machines per se. However different processes and/or machines can handle different kafka topics. Datto logs metrics into different topics based on their datacenter, so we have an individual service for rolling up each.

Checkpointing
-------------

The program provides an exactly-once processing guarentee, meaning if the program closes unexpectedly and is restarted, state should not be lost. It does this by occasionally writing out the rollup state as checkpoints, containing the currently gathered aggregates and the Kafka offsets at the time. When restarting, the program will read the state from the checkpoint and start over at the point the checkpoint was taken.

The program supports multiple checkpoint backends. At the moment, the two supported backends are `fs`, which writes the checkpoint to a file on a local disk, and `hdfs`, which writes the checkpoint to a file on an Apache HDFS filesystem (tied to the `hdfs` cargo feature).

With a highly-available checkpoint backend like `hdfs` and a highly available job scheduler like Apache Yarn, rollups using this program can qualify as a highly-available service.
