kng
===

`kng` is a kafka client wrapper lib for Go that is geared towards facilitating
high throughput message production and consumption.

It wraps both [segmentio/kafka-go](https://github.com/segmentio/kafka-go) and
[Shopify/sarama](https://github.com/Shopify/sarama) libs for different
functionality.

## Special bits

The publisher does a lot of stuff under the hood which dramatically improves
write performance.

* Automatically creates a dedicated publisher for the given topic IF a 
publisher does not already exist.
* Starts a background publisher in a goroutine that will clear its queue on
an interval defined by `PublishInterval`.
* Publisher goroutine will be stopped if it is idle for longer than 
`WorkerIdleTimeout`.
* To avoid kafka from rejecting a batch containing too many messages, the
publisher will automatically divide the batch into "sub-batches" (whose
size is defined by `DefaultSubBatchSize`.

## Why multiple libs?

The segment lib works great for consuming and producing (and has internal
batching mechanisms) but doesn't work as well when dealing with topic
management and other administrative tasks.

We also prefer the `kafka-go` interface but some things just work better with
the `sarama` lib.

## Name

`kng` is "kafka next generation" which is _mildly_ poking fun at the late 90's
software naming pattern that included "next generation" in its name (such as
`syslog-ng`) :smiley:
