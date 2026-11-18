package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestReceivePartitionBatchSelectsOnlyOnePartitionAndPreservesOthers(t *testing.T) {
	client := &Client{maxPoll: 8, pending: []*kgo.Record{
		{Topic: "tasks", Partition: 2, Offset: 10, Value: []byte("a")},
		{Topic: "tasks", Partition: 4, Offset: 20, Value: []byte("b")},
		{Topic: "tasks", Partition: 2, Offset: 11, Value: []byte("c")},
	}}
	batch, err := client.ReceivePartitionBatch(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	messages := batch.Messages()
	if len(messages) != 2 || messages[0].Partition != 2 || messages[0].Offset != 10 || messages[1].Offset != 11 {
		t.Fatalf("messages=%+v", messages)
	}
	if len(client.pending) != 1 || client.pending[0].Partition != 4 || client.pending[0].Offset != 20 {
		t.Fatalf("pending=%+v", client.pending)
	}
	batch.(*deliveryBatch).settle(true)
	if client.outstanding {
		t.Fatal("settled batch remained outstanding")
	}
}

func TestReceivePartitionBatchRejectsUnboundedLimit(t *testing.T) {
	client := &Client{}
	if _, err := client.ReceivePartitionBatch(context.Background(), 9); err == nil {
		t.Fatal("partition batch larger than eight was accepted")
	}
}
