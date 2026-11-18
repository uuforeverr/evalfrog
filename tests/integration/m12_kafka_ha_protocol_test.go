//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/uu999/evalfrog/internal/eventing"
)

const kafkaHAExperimentEnv = "EVALFROG_KAFKA_HA_EXPERIMENT"

func TestM12KafkaThreeBrokerProtocol(t *testing.T) {
	if os.Getenv(kafkaHAExperimentEnv) != "1" {
		t.Skip("set EVALFROG_KAFKA_HA_EXPERIMENT=1 and mount /var/run/docker.sock to run broker fault injection")
	}
	if _, err := os.Stat("/var/run/docker.sock"); err != nil {
		t.Fatalf("Docker socket is required for broker fault injection: %v", err)
	}
	harness := newM5HarnessWithTimeout(t, 10*time.Minute)
	environment := newKafkaFaultEnvironment(t, harness, "ha-protocol")
	if environment.configuration.Kafka.ReplicationFactor != 3 || environment.configuration.Kafka.MinISR != 2 {
		t.Fatalf("HA experiment requires replication_factor=3 and min_isr=2, got %d/%d",
			environment.configuration.Kafka.ReplicationFactor, environment.configuration.Kafka.MinISR)
	}
	for broker := 1; broker <= 3; broker++ {
		broker := broker
		t.Cleanup(func() { _ = dockerContainerAction(context.Background(), kafkaHAContainer(broker), "start") })
	}
	topic := environment.configuration.Kafka.TopicPrefix + "." + environment.configuration.Kafka.Topics.SandboxTask.Name
	leader := waitForKafkaISR(t, environment.configuration.Kafka.Brokers, topic, 3, time.Minute)

	t.Run("one_broker_down_acks_all_continues", func(t *testing.T) {
		follower := 1
		if int32(follower) == leader {
			follower = 2
		}
		mustDockerContainerAction(t, kafkaHAContainer(follower), "stop")
		waitForKafkaISR(t, environment.configuration.Kafka.Brokers, topic, 2, time.Minute)
		publishOneTaskOutbox(t, harness, environment, "ha-one-down", true)
		mustDockerContainerAction(t, kafkaHAContainer(follower), "start")
		leader = waitForKafkaISR(t, environment.configuration.Kafka.Brokers, topic, 3, time.Minute)
	})

	t.Run("leader_kill_election_acks_all_continues", func(t *testing.T) {
		oldLeader := int(leader)
		mustDockerContainerAction(t, kafkaHAContainer(oldLeader), "stop")
		newLeader := waitForKafkaISR(t, environment.configuration.Kafka.Brokers, topic, 2, time.Minute)
		if newLeader == leader {
			t.Fatalf("leader did not change after broker %d stopped", oldLeader)
		}
		publishOneTaskOutbox(t, harness, environment, "ha-leader-kill", true)
		mustDockerContainerAction(t, kafkaHAContainer(oldLeader), "start")
		leader = waitForKafkaISR(t, environment.configuration.Kafka.Brokers, topic, 3, time.Minute)
	})

	t.Run("two_brokers_down_rejects_then_outbox_drains", func(t *testing.T) {
		cases := prepareKafkaFaultCases(t, harness, 1, "ha-two-down")
		keep := int(leader)
		stopped := make([]int, 0, 2)
		for broker := 1; broker <= 3; broker++ {
			if broker == keep {
				continue
			}
			mustDockerContainerAction(t, kafkaHAContainer(broker), "stop")
			stopped = append(stopped, broker)
		}
		relay, err := eventing.NewTaskRelay(harness.store, environment.publisher, "ha-no-quorum", 1, time.Second, 0)
		if err != nil {
			t.Fatal(err)
		}
		publishContext, cancel := context.WithTimeout(harness.ctx, 8*time.Second)
		published, publishErr := relay.RelayOnce(publishContext)
		cancel()
		if publishErr == nil || published != 0 {
			t.Fatalf("two-broker outage published=%d err=%v", published, publishErr)
		}
		assertTaskOutboxUnpublished(t, harness, cases[0].task.TaskID)

		mustDockerContainerAction(t, kafkaHAContainer(stopped[0]), "start")
		waitForKafkaISR(t, environment.configuration.Kafka.Brokers, topic, 2, time.Minute)
		time.Sleep(1100 * time.Millisecond)
		drain, err := eventing.NewTaskRelay(harness.store, environment.publisher, "ha-quorum-restored", 1, time.Second, 0)
		if err != nil {
			t.Fatal(err)
		}
		relayExactly(t, harness.ctx, drain, 1)
		assertTaskOutboxPublished(t, harness, cases[0].task.TaskID)
		mustDockerContainerAction(t, kafkaHAContainer(stopped[1]), "start")
		waitForKafkaISR(t, environment.configuration.Kafka.Brokers, topic, 3, time.Minute)
	})
}

func publishOneTaskOutbox(t *testing.T, harness *m5Harness, environment *kafkaFaultEnvironment, prefix string, expectPublished bool) {
	t.Helper()
	cases := prepareKafkaFaultCases(t, harness, 1, prefix)
	relay, err := eventing.NewTaskRelay(harness.store, environment.publisher, prefix+"-relay", 1, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	published, err := relay.RelayOnce(harness.ctx)
	if err != nil || published != 1 {
		t.Fatalf("%s published=%d err=%v", prefix, published, err)
	}
	if expectPublished {
		assertTaskOutboxPublished(t, harness, cases[0].task.TaskID)
	}
}

func assertTaskOutboxUnpublished(t *testing.T, harness *m5Harness, taskID string) {
	t.Helper()
	var published bool
	if err := harness.client.Pool().QueryRow(harness.ctx, `
		SELECT published_at IS NOT NULL FROM node_task_outbox WHERE task_id=UUID_TO_BIN($1)`, taskID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatalf("task %s was marked published without Kafka quorum", taskID)
	}
}

func assertTaskOutboxPublished(t *testing.T, harness *m5Harness, taskID string) {
	t.Helper()
	var published bool
	if err := harness.client.Pool().QueryRow(harness.ctx, `
		SELECT published_at IS NOT NULL FROM node_task_outbox WHERE task_id=UUID_TO_BIN($1)`, taskID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if !published {
		t.Fatalf("task %s was not marked published after Kafka acknowledgement", taskID)
	}
}

func waitForKafkaISR(t *testing.T, brokers []string, topic string, expected int, timeout time.Duration) int32 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastLeader int32 = -1
	var lastReplicas, lastISR int
	for time.Now().Before(deadline) {
		leader, replicas, isr, err := kafkaTopicState(t.Context(), brokers, topic)
		if err == nil {
			lastLeader, lastReplicas, lastISR = leader, replicas, isr
			if replicas == 3 && isr == expected && leader >= 1 {
				return leader
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("topic %s leader=%d replicas=%d isr=%d, want replicas=3 isr=%d", topic, lastLeader, lastReplicas, lastISR, expected)
	return -1
}

func kafkaTopicState(ctx context.Context, brokers []string, topic string) (int32, int, int, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return -1, 0, 0, err
	}
	defer client.Close()
	request := kmsg.NewPtrMetadataRequest()
	request.AllowAutoTopicCreation = false
	requestTopic := kmsg.NewMetadataRequestTopic()
	requestTopic.Topic = &topic
	request.Topics = append(request.Topics, requestTopic)
	raw, err := request.RequestWith(ctx, client)
	if err != nil || len(raw.Topics) != 1 || len(raw.Topics[0].Partitions) == 0 {
		return -1, 0, 0, fmt.Errorf("metadata topic unavailable: %w", err)
	}
	partition := raw.Topics[0].Partitions[0]
	return partition.Leader, len(partition.Replicas), len(partition.ISR), nil
}

func kafkaHAContainer(broker int) string {
	return fmt.Sprintf("evalfrog-kafka-ha-kafka-%d-1", broker)
}

func mustDockerContainerAction(t *testing.T, container, action string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := dockerContainerAction(ctx, container, action); err != nil {
		t.Fatal(err)
	}
}

func dockerContainerAction(ctx context.Context, container, action string) error {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", "/var/run/docker.sock")
		},
	}
	client := &http.Client{Transport: transport}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://docker/containers/%s/%s?t=10", container, action), nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("Docker %s %s: %w", action, container, err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent || response.StatusCode == http.StatusNotModified {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	return fmt.Errorf("Docker %s %s returned %s: %s", action, container, response.Status, body)
}
