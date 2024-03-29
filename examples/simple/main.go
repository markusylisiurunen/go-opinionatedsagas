package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	sagas "github.com/markusylisiurunen/go-opinionatedsagas"

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

func (t *chargeCustomerTask) TaskName() string { return "charge_customer" }

type rollbackChargeCustomerTask struct {
	ChargeUUID string `json:"charge_uuid"`
}

func (t *rollbackChargeCustomerTask) TaskName() string { return "rollback_charge_customer" }

// step: send receipt
type sendReceiptTask struct {
	CustomerUUID string `json:"customer_uuid"`
	Amount       int64  `json:"amount"`
}

func (t *sendReceiptTask) TaskName() string { return "send_receipt" }

// handlers
// ---

func handleChargeCustomerTask(ctx context.Context, msg *chargeCustomerTask) (*rollbackChargeCustomerTask, *sendReceiptTask, error) {
	fmt.Println("handle charge customer task was invoked")
	rollback := &rollbackChargeCustomerTask{ChargeUUID: uuid.NewString()}
	next := &sendReceiptTask{CustomerUUID: msg.CustomerUUID, Amount: msg.Amount}
	return rollback, next, nil
}

func handleRollbackChargeCustomerTask(ctx context.Context, msg *rollbackChargeCustomerTask) error {
	fmt.Println("rollback charge customer task was invoked")
	return nil
}

func handleSendReceiptTask(ctx context.Context, msg *sendReceiptTask) error {
	fmt.Println("handle send receipt task was invoked")
	if rand.Intn(100) < 50 {
		fmt.Println("simulated error from sending a receipt")
		return errors.New("something happened")
	}
	return nil
}

// register steps
// ---

func registerSaga(ctx context.Context) error {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return err
	}
	saga, err := sagas.NewSaga(ctx, &sagas.SagaOpts{
		ConnectionString: connectionString,
		DB:               db,
		Schema:           "opinionatedsagas_test_01",
	})
	if err != nil {
		return err
	}
	saga.AddStep(&sagas.Step{
		HandleFunc:     handleChargeCustomerTask,
		CompensateFunc: handleRollbackChargeCustomerTask,
	})
	saga.AddStep(&sagas.Step{
		MaxAttempts: 1,
		HandleFunc:  handleSendReceiptTask,
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
	// register the handlers for the saga
	if err := registerSaga(ctx); err != nil {
		panic(err)
	}
	// kill the service gracefully
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	time.Sleep(1 * time.Second)
}
