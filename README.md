# th2-common-mq-batcher-go

Provides structures for batching th2-common's entities. Each batch is sent when its size is equal or exceed the size configured by `BatchSizeBytes` option or flush timeout is over configured by `FlushMillis` option

## Transport message batcher

`MqBatcher[MessageArguments]` created by the `batcher.NewMessageBatcher` method allows to batch and send messages via MQ in th2 transport protocol

### Configuration

The Batcher configuration include the options
* *Book* __(required)__ - th2 book name.
* *Group* __(required)__ - th2 session group name.
* *Protocol* __(optional)__ - default value for protocol. If it isn't specified and you don't specify `batcher.MessageArguments.Protocol` passed into the `MqBatcher[MessageArguments].Send` method, messages will be send with empty protocol.
* *BatchSizeBytes* __(1048576 by default)__ - max batch size. 
* *FlushMillis* __(1000 by default)__ - max time interval in milliseconds between sending.

```go
import (
	"github.com/th2-net/th2-common-mq-batcher-go/pkg/batcher"
)

config := batcher.MqMessageBatcherConfig{
	MqBatcherConfig: batcher.MqBatcherConfig{
		BatchSizeBytes: 1048576,
		FlushMillis:    1000,
		Book:           "book",
	},
	Protocol: "protocol",
	Group:    "group",
}
```

