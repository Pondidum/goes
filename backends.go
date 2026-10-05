package goes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"uuid"
)

func NewEventStore(backend backend) *eventstore {
	return &eventstore{backend: backend}
}

type eventstore struct {
	backend backend
}

func (s *eventstore) Write(ctx context.Context, aggregate Aggregate) error {
	state := aggregate.state()
	pendingCount := len(state.pendingEvents)

	if pendingCount == 0 {
		return nil
	}

	unit, err := s.backend.startUnitOfWork(ctx)
	if err != nil {
		return err
	}
	defer unit.cancel()

	if err := unit.validateSequence(ctx, state.id, state.sequence); err != nil {
		return err
	}

	if err := unit.writeEvents(ctx, state.pendingEvents); err != nil {
		return err
	}
	// unit.projectEvents(...)

	state.sequence = state.pendingEvents[pendingCount-1].Sequence
	state.pendingEvents = nil

	if err := unit.complete(); err != nil {
		return err
	}

	state.sequence = state.pendingEvents[pendingCount-1].Sequence
	state.pendingEvents = nil

	return nil
}

type backend interface {
	startUnitOfWork(ctx context.Context) (unitOfWork, error)
}

type unitOfWork interface {
	validateSequence(ctx context.Context, aggregateID uuid.UUID, memorySequence int) error
	writeEvents(ctx context.Context, events []EventDescriptor) error
	projectEvents( /**/ )

	complete() error
	cancel() error
}

func NewSqliteBackend(db *sql.DB, enableTransactions bool) *sqliteBackend {
	return &sqliteBackend{
		db:           db,
		transactions: enableTransactions,
	}
}

type sqliteBackend struct {
	transactions bool
	db           *sql.DB
}

func (b *sqliteBackend) Init(ctx context.Context) error {
	// maybe move into the unitofwork as a sqlite specific method
	createTables := strings.TrimSpace(`
create table if not exists events(
	event_id integer primary key autoincrement,
	aggregate_id text not null,
	aggregate_type text not null,
	sequence integer not null,
	timestamp timestamp not null,
	event_type text not null,
	event_data text not null,
	constraint aggregate_sequence unique(aggregate_id, sequence) on conflict rollback
);`)

	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, createTables); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (b *sqliteBackend) startUnitOfWork(ctx context.Context) (unitOfWork, error) {
	if !b.transactions {
		return &bareUow{db: b.db}, nil
	}

	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	return &transactionUow{tx: tx, inner: bareUow{db: tx}}, nil
}

type transactionUow struct {
	tx    *sql.Tx
	inner unitOfWork
}

func (uow *transactionUow) complete() error {
	return uow.tx.Commit()
}

func (uow *transactionUow) cancel() error {
	return uow.tx.Rollback()
}

func (uow *transactionUow) validateSequence(ctx context.Context, aggregateID uuid.UUID, memorySequence int) error {
	return uow.inner.validateSequence(ctx, aggregateID, memorySequence)
}

func (uow *transactionUow) writeEvents(ctx context.Context, events []EventDescriptor) error {
	return uow.inner.writeEvents(ctx, events)
}
func (uow *transactionUow) projectEvents() {
	uow.inner.projectEvents()
}

type bareUow struct {
	db Queryable
}

func (uow bareUow) complete() error { return nil }
func (uow bareUow) cancel() error   { return nil }

func (uow bareUow) validateSequence(ctx context.Context, aggregateID uuid.UUID, memorySequence int) error {
	var dbSequence sql.NullInt64
	stmt := "select max(sequence) from events where aggregate_id = ?"
	row := uow.db.QueryRowContext(ctx, stmt, aggregateID)

	if err := row.Scan(&dbSequence); errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if dbSequence.Valid && dbSequence.Int64 > int64(memorySequence) {
		return fmt.Errorf("aggregate has new events in the database. db: %v, memory: %v", dbSequence, memorySequence)
	}

	return nil
}

func (uow bareUow) writeEvents(ctx context.Context, events []EventDescriptor) error {
	stmt := strings.TrimSpace(`
		insert into events (aggregate_id, aggregate_type, sequence, timestamp, event_type, event_data)
		values (?, ?, ?, ?, ?, ?)
	`)

	insertEvent, err := uow.db.PrepareContext(ctx, stmt)
	if err != nil {
		return err
	}
	defer insertEvent.Close()

	for _, e := range events {
		_, err = insertEvent.ExecContext(ctx, e.AggregateID, e.AggregateType, e.Sequence, e.Timestamp, e.EventType, e.json)
		if err != nil {
			return err
		}
	}

	return nil
}

func (uow bareUow) projectEvents() {}

type Queryable interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}
