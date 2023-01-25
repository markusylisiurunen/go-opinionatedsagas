package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/markusylisiurunen/go-opinionatedevents"
	"github.com/markusylisiurunen/go-opinionatedsagas"

	_ "github.com/lib/pq"
)

const (
	connectionString string = "postgres://postgres:password@localhost:6632/dev?sslmode=disable"
)

// messages
// ---

// step: charge customer
type chargeCustomerTask struct {
	CustomerUUID string `json:"customer_uuid"`
	Amount       int64  `json:"amount"`
}

func (t *chargeCustomerTask) TaskName() string { return "tasks.charge_customer" }

type rollbackChargeCustomerTask struct {
	ChargeUUID string `json:"charge_uuid"`
}

func (t *rollbackChargeCustomerTask) TaskName() string { return "tasks.rollback_charge_customer" }

// step: send receipt
type sendReceiptTask struct {
	CustomerUUID string `json:"customer_uuid"`
	Amount       int64  `json:"amount"`
}

func (t *sendReceiptTask) TaskName() string { return "tasks.send_receipt" }

// handlers
// ---

type result = opinionatedevents.ResultContainer

func handleChargeCustomerTask(ctx context.Context, msg *chargeCustomerTask) (result, *rollbackChargeCustomerTask, *sendReceiptTask) {
	fmt.Println("handle charge customer task was invoked")
	rollback := &rollbackChargeCustomerTask{ChargeUUID: uuid.NewString()}
	next := &sendReceiptTask{CustomerUUID: msg.CustomerUUID, Amount: msg.Amount}
	return opinionatedevents.SuccessResult(), rollback, next
}

func handleRollbackChargeCustomerTask(ctx context.Context, msg *rollbackChargeCustomerTask) result {
	fmt.Println("rollback charge customer task was invoked")
	return opinionatedevents.SuccessResult()
}

func handleSendReceiptTask(ctx context.Context, msg *sendReceiptTask) (result, any, any) {
	fmt.Println("handle send receipt task was invoked")
	if rand.Intn(100) < 50 {
		fmt.Println("simulated error from sending a receipt")
		return opinionatedevents.ErrorResult(errors.New(""), time.Second), nil, nil
	}
	return opinionatedevents.SuccessResult(), nil, nil
}

// register steps
// ---

func registerSaga(ctx context.Context, receiver *opinionatedevents.Receiver, publisher *opinionatedevents.Publisher) error {
	saga := opinionatedsagas.NewSaga(receiver, publisher, "tasks")
	saga.AddStep(&opinionatedsagas.Step{
		HandleFunc:     handleChargeCustomerTask,
		CompensateFunc: handleRollbackChargeCustomerTask,
	})
	saga.AddStep(&opinionatedsagas.Step{
		HandleFunc: handleSendReceiptTask,
	})
	if err := saga.RegisterHandlers(); err != nil {
		return err
	}
	// start triggering the saga
	go func(ctx context.Context) {
		i := -1
		for {
			i += 1
			d := 1 * time.Second
			if i > 0 {
				d = 10 * time.Second
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(d):
				task := &chargeCustomerTask{
					CustomerUUID: uuid.NewString(),
					Amount:       1299,
				}
				if err := saga.Trigger(ctx, task); err != nil {
					fmt.Printf("%s\n", err)
					continue
				}
				fmt.Println("charge customer message published")
				continue
			}
		}
	}(ctx)
	return nil
}

// main
// ---

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	// seed the random number generator
	rand.Seed(time.Now().UnixNano())
	// configure the publisher
	destination, err := opinionatedevents.NewPostgresDestination(connectionString,
		opinionatedevents.PostgresDestinationWithTopicToQueues("tasks", "tasks"),
		opinionatedevents.PostgresDestinationWithTableName("events"),
		opinionatedevents.PostgresDestinationWithColumnNames(map[string]string{
			"deliver_at":        "event_deliver_at",
			"delivery_attempts": "event_delivery_attempts",
			"id":                "event_id",
			"name":              "event_name",
			"payload":           "event_payload",
			"published_at":      "event_published_at",
			"queue":             "event_queue",
			"status":            "event_status",
			"topic":             "event_topic",
			"uuid":              "event_uuid",
		}),
	)
	if err != nil {
		panic(err)
	}
	publisher, err := opinionatedevents.NewPublisher(
		opinionatedevents.PublisherWithSyncBridge(destination),
	)
	if err != nil {
		panic(err)
	}
	// configure the receiver
	receiver, err := opinionatedevents.NewReceiver()
	if err != nil {
		panic(err)
	}
	_, err = opinionatedevents.MakeReceiveFromPostgres(ctx, receiver, connectionString,
		opinionatedevents.ReceiveFromPostgresWithQueues("tasks"),
		opinionatedevents.ReceiveFromPostgresWithTableName("events"),
		opinionatedevents.ReceiveFromPostgresWithColumnNames(map[string]string{
			"deliver_at":        "event_deliver_at",
			"delivery_attempts": "event_delivery_attempts",
			"id":                "event_id",
			"name":              "event_name",
			"payload":           "event_payload",
			"published_at":      "event_published_at",
			"queue":             "event_queue",
			"status":            "event_status",
			"topic":             "event_topic",
			"uuid":              "event_uuid",
		}),
		opinionatedevents.ReceiveFromPostgresWithIntervalTrigger(5*time.Second),
		opinionatedevents.ReceiveFromPostgresWithNotifyTrigger("__events"),
	)
	if err != nil {
		panic(err)
	}
	// register the handlers for the saga
	if err := registerSaga(ctx, receiver, publisher); err != nil {
		panic(err)
	}
	// kill the service gracefully
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	time.Sleep(1 * time.Second)
}
