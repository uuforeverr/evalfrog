//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/uu999/evalfrog/internal/adapters/kafka"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/platform/config"
	enginepkg "github.com/uu999/evalfrog/internal/runtime/engine"
)

const kafkaReliabilityExperimentEnv = "EVALFROG_KAFKA_RELIABILITY_EXPERIMENT"

// TestM12KafkaReliabilityFaultWindows exercises one-node workflows at the ten
// durability boundaries in the current Engine-direct-dispatch architecture.
// Width is intentionally fixed at one: these cases prove crash-window
// invariants, while wide-workflow batching is covered by a separate experiment.
func TestM12KafkaReliabilityFaultWindows(t *testing.T) {
	if os.Getenv(kafkaReliabilityExperimentEnv) != "1" {
		t.Skip("set EVALFROG_KAFKA_RELIABILITY_EXPERIMENT=1 to run Kafka reliability fault windows")
	}
	repetitions := positiveEnvInt(t, kafkaFaultSamplesEnv, 10)
	leaseDuration := durationEnv(t, kafkaFaultLeaseEnv, 2*time.Second)

	t.Run("F1_task_outbox_committed_before_publish", func(t *testing.T) {
		runKafkaFaultF1(t, repetitions)
	})
	t.Run("F2_task_published_before_outbox_mark", func(t *testing.T) {
		runKafkaFaultF2(t, repetitions)
	})
	t.Run("F3_worker_poll_before_claim", func(t *testing.T) {
		runKafkaFaultPollBeforeClaim(t, repetitions)
	})
	t.Run("F4_claim_committed_before_kafka_ack", func(t *testing.T) {
		runKafkaFaultClaimBeforeAck(t, repetitions)
	})
	t.Run("F5_kafka_ack_before_complete", func(t *testing.T) {
		records := runKafkaFaultF3(t, repetitions, leaseDuration)
		for index := range records {
			records[index].window = "F5"
		}
	})
	t.Run("F6_complete_committed_before_runtime_publish", func(t *testing.T) {
		runKafkaFaultCompleteBeforeRuntimePublish(t, repetitions)
	})
	t.Run("F7_runtime_event_published_before_outbox_mark", func(t *testing.T) {
		records := runKafkaFaultF4(t, repetitions)
		for index := range records {
			records[index].window = "F7"
		}
	})
	t.Run("F8_engine_inbox_insert_rolled_back", func(t *testing.T) {
		runKafkaFaultInboxRollback(t, repetitions)
	})
	t.Run("F9_recovery_attempt_rejects_old_worker_result", func(t *testing.T) {
		records := runKafkaFaultF3(t, repetitions, leaseDuration)
		for index := range records {
			records[index].window = "F9"
		}
	})
	t.Run("F10_rebalance_during_poll_claim_ack", func(t *testing.T) {
		runKafkaFaultRebalanceGate(t, repetitions)
	})
}

func runKafkaFaultPollBeforeClaim(t *testing.T, samples int) {
	harness := newM5HarnessWithTimeout(t, 10*time.Minute)
	environment := newKafkaFaultEnvironment(t, harness, "m12-f3")
	cases := prepareKafkaFaultCases(t, harness, samples, "m12-f3")
	relayTaskCases(t, harness, environment, samples, "m12-f3-relay")
	byAttempt := faultCasesByAttempt(cases)
	current := environment.taskConsumer
	for iteration := range cases {
		delivery, err := current.Receive(harness.ctx)
		if err != nil {
			t.Fatal(err)
		}
		first, err := eventing.ParseTaskMessage(delivery.Payload())
		if err != nil {
			t.Fatal(err)
		}
		// Simulate termination after Poll but before the authoritative Claim.
		delivery.Nack()
		current.Close()
		current = reopenFaultTaskConsumer(t, environment, fmt.Sprintf("m12-f3-restart-%d", iteration))
		redelivery, err := current.Receive(harness.ctx)
		if err != nil {
			t.Fatal(err)
		}
		message, err := eventing.ParseTaskMessage(redelivery.Payload())
		if err != nil {
			t.Fatal(err)
		}
		if message.AttemptID != first.AttemptID {
			t.Fatalf("uncommitted attempt %s redelivered as %s", first.AttemptID, message.AttemptID)
		}
		index := byAttempt[message.AttemptID]
		lease, err := harness.coordinator.Claim(harness.ctx, claimCommand(cases[index].task, "m12-f3-worker", time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if err = redelivery.Ack(harness.ctx); err != nil {
			t.Fatal(err)
		}
		if _, err = harness.coordinator.Complete(harness.ctx, completionCommand(cases[index].task, lease, "m12-f3-complete")); err != nil {
			t.Fatal(err)
		}
	}
	current.Close()
	finishFaultCases(t, harness, environment, cases, samples, "m12-f3-runtime")
}

func runKafkaFaultClaimBeforeAck(t *testing.T, samples int) {
	harness := newM5HarnessWithTimeout(t, 10*time.Minute)
	environment := newKafkaFaultEnvironment(t, harness, "m12-f4")
	cases := prepareKafkaFaultCases(t, harness, samples, "m12-f4")
	relayTaskCases(t, harness, environment, samples, "m12-f4-relay")
	byAttempt := faultCasesByAttempt(cases)
	current := environment.taskConsumer
	for iteration := range cases {
		delivery, err := current.Receive(harness.ctx)
		if err != nil {
			t.Fatal(err)
		}
		message, err := eventing.ParseTaskMessage(delivery.Payload())
		if err != nil {
			t.Fatal(err)
		}
		index, ok := byAttempt[message.AttemptID]
		if !ok {
			t.Fatalf("unexpected attempt %s", message.AttemptID)
		}
		lease, err := harness.coordinator.Claim(harness.ctx, claimCommand(cases[index].task, "m12-f4-worker", time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		cases[index].lease = lease
		// The database Claim commits, but the Kafka offset does not.
		delivery.Nack()
		current.Close()
		current = reopenFaultTaskConsumer(t, environment, fmt.Sprintf("m12-f4-restart-%d", iteration))
		redelivery, err := current.Receive(harness.ctx)
		if err != nil {
			t.Fatal(err)
		}
		redelivered, err := eventing.ParseTaskMessage(redelivery.Payload())
		if err != nil {
			t.Fatal(err)
		}
		if redelivered.AttemptID != message.AttemptID {
			t.Fatalf("uncommitted attempt %s redelivered as %s", message.AttemptID, redelivered.AttemptID)
		}
		lease, err = harness.coordinator.Claim(harness.ctx, claimCommand(cases[index].task, "m12-f4-worker", time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if lease.Token != cases[index].lease.Token {
			t.Fatalf("duplicate delivery changed lease token for attempt %s", message.AttemptID)
		}
		if err = redelivery.Ack(harness.ctx); err != nil {
			t.Fatal(err)
		}
		if _, err = harness.coordinator.Complete(harness.ctx, completionCommand(cases[index].task, lease, "m12-f4-complete")); err != nil {
			t.Fatal(err)
		}
	}
	current.Close()
	finishFaultCases(t, harness, environment, cases, samples, "m12-f4-runtime")
}

func runKafkaFaultCompleteBeforeRuntimePublish(t *testing.T, samples int) {
	harness := newM5HarnessWithTimeout(t, 10*time.Minute)
	environment := newKafkaFaultEnvironment(t, harness, "m12-f6")
	cases := prepareKafkaFaultCases(t, harness, samples, "m12-f6")
	relayTaskCases(t, harness, environment, samples, "m12-f6-task")
	claimAndAckWithoutComplete(t, harness, environment.taskConsumer, cases, time.Minute)
	for index := range cases {
		applied, err := harness.coordinator.Complete(harness.ctx, completionCommand(cases[index].task, cases[index].lease, "m12-f6-complete"))
		if err != nil || !applied {
			t.Fatalf("completion applied=%v err=%v", applied, err)
		}
		var unpublished int
		if err = harness.client.Pool().QueryRow(harness.ctx, `
			SELECT count(*) FROM outbox_events
			WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND event_type=$3 AND published_at IS NULL`,
			harness.projectID, cases[index].run.ID, eventing.AttemptCompleted).Scan(&unpublished); err != nil || unpublished != 1 {
			t.Fatalf("unpublished completion events=%d err=%v", unpublished, err)
		}
	}
	finishFaultCases(t, harness, environment, cases, samples, "m12-f6-runtime")
}

func runKafkaFaultInboxRollback(t *testing.T, samples int) {
	harness := newM5HarnessWithTimeout(t, 10*time.Minute)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	rollback := errors.New("injected transaction rollback")
	for index := 0; index < samples; index++ {
		run := harness.createTestRun(t, workflow.ID, snapshot.ID, fmt.Sprintf("m12-f8-%d-%s", index, uuid.NewString()))
		base := harness.event(t, eventing.RunCreated, run.ID)
		event := base
		event.EventID = uuid.NewString()
		err := harness.store.WithRunTransaction(harness.ctx, event, func(tx enginepkg.RunTransaction) error {
			accepted, acceptErr := tx.AcceptInbox(harness.ctx, enginepkg.ConsumerName, event)
			if acceptErr != nil || !accepted {
				t.Fatalf("accept inbox accepted=%v err=%v", accepted, acceptErr)
			}
			return rollback
		})
		if !errors.Is(err, rollback) {
			t.Fatalf("rollback error=%v", err)
		}
		var rows int
		if err = harness.client.Pool().QueryRow(harness.ctx, `
			SELECT count(*) FROM inbox_events WHERE consumer_name=$1 AND event_id=UUID_TO_BIN($2)`,
			enginepkg.ConsumerName, event.EventID).Scan(&rows); err != nil || rows != 0 {
			t.Fatalf("rolled-back inbox rows=%d err=%v", rows, err)
		}
		// The same event remains admissible after rollback; the committed retry
		// then becomes the sole Inbox row.
		if err = harness.store.WithRunTransaction(harness.ctx, event, func(tx enginepkg.RunTransaction) error {
			accepted, acceptErr := tx.AcceptInbox(harness.ctx, enginepkg.ConsumerName, event)
			if acceptErr != nil || !accepted {
				return fmt.Errorf("retry accepted=%v: %w", accepted, acceptErr)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err = harness.client.Pool().QueryRow(harness.ctx, `
			SELECT count(*) FROM inbox_events WHERE consumer_name=$1 AND event_id=UUID_TO_BIN($2)`,
			enginepkg.ConsumerName, event.EventID).Scan(&rows); err != nil || rows != 1 {
			t.Fatalf("committed inbox rows=%d err=%v", rows, err)
		}
	}
}

func runKafkaFaultRebalanceGate(t *testing.T, samples int) {
	harness := newM5HarnessWithTimeout(t, 10*time.Minute)
	environment := newKafkaFaultEnvironment(t, harness, "m12-f10")
	cases := prepareKafkaFaultCases(t, harness, samples, "m12-f10")
	relayTaskCases(t, harness, environment, samples, "m12-f10-relay")
	byAttempt := faultCasesByAttempt(cases)
	current := environment.taskConsumer

	for iteration := 0; iteration < samples; iteration++ {
		delivery, err := current.Receive(harness.ctx)
		if err != nil {
			t.Fatal(err)
		}
		next, err := kafka.OpenConsumer(environment.configuration.Kafka,
			fmt.Sprintf("%s-rebalance-%d", environment.identifier, iteration), environment.taskGroup,
			[]config.KafkaTopicConfig{environment.configuration.Kafka.Topics.SandboxTask}, 1)
		if err != nil {
			t.Fatal(err)
		}
		result := make(chan deliveryResult, 1)
		pollContext, cancel := context.WithTimeout(harness.ctx, 30*time.Second)
		go func() {
			value, receiveErr := next.Receive(pollContext)
			result <- deliveryResult{delivery: value, err: receiveErr}
		}()
		// The second member requests a rebalance while the first member still
		// holds BlockRebalanceOnPoll across Poll -> Claim/ACK.
		time.Sleep(250 * time.Millisecond)
		delivery.Nack()
		current.Close()
		outcome := <-result
		cancel()
		if outcome.err != nil {
			next.Close()
			t.Fatal(outcome.err)
		}
		message, err := eventing.ParseTaskMessage(outcome.delivery.Payload())
		if err != nil {
			next.Close()
			t.Fatal(err)
		}
		index, ok := byAttempt[message.AttemptID]
		if !ok {
			next.Close()
			t.Fatalf("unexpected rebalanced attempt %s", message.AttemptID)
		}
		lease, err := harness.coordinator.Claim(harness.ctx, claimCommand(cases[index].task, "m12-f10-worker", time.Minute))
		if err != nil {
			next.Close()
			t.Fatal(err)
		}
		if err = outcome.delivery.Ack(harness.ctx); err != nil {
			next.Close()
			t.Fatal(err)
		}
		if _, err = harness.coordinator.Complete(harness.ctx, completionCommand(cases[index].task, lease, "m12-f10-complete")); err != nil {
			next.Close()
			t.Fatal(err)
		}
		current = next
	}
	current.Close()
	finishFaultCases(t, harness, environment, cases, samples, "m12-f10-runtime")
}

type deliveryResult struct {
	delivery eventing.Delivery
	err      error
}

func relayTaskCases(t *testing.T, harness *m5Harness, environment *kafkaFaultEnvironment, samples int, owner string) {
	t.Helper()
	relay, err := eventing.NewTaskRelay(harness.store, environment.publisher, owner, samples, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	relayExactly(t, harness.ctx, relay, samples)
}

func finishFaultCases(t *testing.T, harness *m5Harness, environment *kafkaFaultEnvironment, cases []kafkaFaultCase, samples int, owner string) {
	t.Helper()
	relay, err := eventing.NewRelay(harness.store, environment.publisher, owner, samples, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	relayExactly(t, harness.ctx, relay, samples)
	events := consumeRuntimeEvents(t, harness, environment.eventConsumer, samples, eventing.AttemptCompleted)
	for index := range cases {
		cases[index].eventID = events[cases[index].run.ID]
		cases[index].terminalAt = time.Now().UTC()
		verifySuccessfulCase(t, harness, &cases[index], 1)
	}
}

func faultCasesByAttempt(cases []kafkaFaultCase) map[string]int {
	result := make(map[string]int, len(cases))
	for index := range cases {
		result[cases[index].task.AttemptID] = index
	}
	return result
}

func reopenFaultTaskConsumer(t *testing.T, environment *kafkaFaultEnvironment, clientID string) *kafka.Client {
	t.Helper()
	consumer, err := kafka.OpenConsumer(environment.configuration.Kafka, clientID, environment.taskGroup,
		[]config.KafkaTopicConfig{environment.configuration.Kafka.Topics.SandboxTask}, 100)
	if err != nil {
		t.Fatal(err)
	}
	return consumer
}
