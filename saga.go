package opinionatedsagas

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	events "github.com/markusylisiurunen/go-opinionatedevents"

	_ "github.com/lib/pq"
)

type Saga struct {
	db     *sql.DB
	schema string

	publisher *events.Publisher
	receiver  *events.Receiver
	queue     string

	steps []*Step
}

type SagaOpts struct {
	ConnectionString string
	Schema           string

	//FIXME: support passing a *sql.DB
	DB *sql.DB
}

func (o *SagaOpts) getSchema() string {
	if o.Schema != "" {
		return o.Schema
	}
	return "opinionatedsagas"
}

func NewSaga(ctx context.Context, opts *SagaOpts) (*Saga, error) {
	// initialise the database connection (if needed)
	var db *sql.DB
	if opts.DB != nil {
		db = opts.DB
	} else {
		if opts.ConnectionString == "" {
			return nil, errors.New("either a connection string or a *sql.DB must be provided")
		}
		_db, err := sql.Open("postgres", opts.ConnectionString)
		if err != nil {
			return nil, err
		}
		if err := _db.Ping(); err != nil {
			return nil, err
		}
		db = _db
	}
	if err := migrate(db, opts.getSchema()); err != nil {
		return nil, err
	}
	// initialise the publisher
	destination, err := events.NewPostgresDestination(opts.ConnectionString,
		events.PostgresDestinationWithTopicToQueues("tasks", "tasks"),
		events.PostgresDestinationWithTableName(fmt.Sprintf("%s.events", opts.getSchema())),
	)
	if err != nil {
		return nil, err
	}
	publisher, err := events.NewPublisher(events.PublisherWithSyncBridge(destination))
	if err != nil {
		return nil, err
	}
	// initialise the receiver
	receiver, err := events.NewReceiver()
	if err != nil {
		return nil, err
	}
	_, err = events.MakeReceiveFromPostgres(ctx, receiver, opts.ConnectionString,
		events.ReceiveFromPostgresWithQueues("tasks"),
		events.ReceiveFromPostgresWithTableName(fmt.Sprintf("%s.events", opts.getSchema())),
		events.ReceiveFromPostgresWithIntervalTrigger(5*time.Second),
		events.ReceiveFromPostgresWithNotifyTrigger("__events"),
	)
	if err != nil {
		return nil, err
	}
	// construct the saga
	saga := &Saga{
		db:        db,
		publisher: publisher,
		queue:     "tasks",
		receiver:  receiver,
		schema:    opts.getSchema(),
		steps:     []*Step{},
	}
	return saga, nil
}

func (s *Saga) AddStep(step *Step) {
	step.init(s.db, s.schema, s.publisher, s.receiver, s.queue)
	s.steps = append(s.steps, step)
}

func (s *Saga) RegisterHandlers() error {
	if len(s.steps) == 0 {
		return nil
	}
	for _, step := range s.steps {
		if err := step.isValid(); err != nil {
			return err
		}
	}
	for _, step := range s.steps {
		if err := step.mountHandleFunc(); err != nil {
			return err
		}
		if err := step.mountCompensateFunc(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Saga) TriggerTx(ctx context.Context, tx *sql.Tx, task task) error {
	if len(s.steps) == 0 || !s.steps[0].isForTask(task) {
		return errors.New("the task must match to the first step's argument type")
	}
	msg, err := newTaskMessage(task).toOpinionatedMessage()
	if err != nil {
		return err
	}
	return s.publisher.Publish(events.WithTx(ctx, tx), msg)
}

func (s *Saga) Trigger(ctx context.Context, task task) error {
	tx, err := s.db.Begin()
	defer tx.Rollback() //nolint the error is not relevant
	if err != nil {
		return err
	}
	if err := s.TriggerTx(ctx, tx, task); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
