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
	backend     backend
	projections []projection
}

type projection interface {
	viewID(aggregate Aggregate) string
	name() string
	project(ctx context.Context, view []byte, events []EventDescriptor) error
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

	if err := s.runProjections(ctx, unit, aggregate); err != nil {
		return err
	}

	if err := unit.complete(); err != nil {
		return err
	}

	state.sequence = state.pendingEvents[pendingCount-1].Sequence
	state.pendingEvents = nil

	return nil
}

func (s *eventstore) runProjections(ctx context.Context, unit unitOfWork, aggregate Aggregate) error {

	for _, projection := range s.projections {
		viewID := projection.viewID(aggregate) // could be the aggreateID, or something else!

		// name=tags_in_use, id=1111-2222-333
		// name=all_recipies, id=1111-2222-333
		view, err := unit.loadView(ctx, projection.name(), viewID)
		if err != nil {
			return err
		}

		if err := projection.project(ctx, view, aggregate.state().pendingEvents); err != nil {
			return err
		}

		if err := unit.saveView(ctx, projection.name(), viewID, view); err != nil {
			return err
		}
	}

	return nil
}

type backend interface {
	startUnitOfWork(ctx context.Context) (unitOfWork, error)
}

type unitOfWork interface {
	validateSequence(ctx context.Context, aggregateID uuid.UUID, memorySequence int) error
	writeEvents(ctx context.Context, events []EventDescriptor) error

	loadView(ctx context.Context, viewType string, viewID string) ([]byte, error)
	saveView(ctx context.Context, viewType string, viewID string, view []byte) error

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
);

create table if not exists views(
	view_type text not null,
	view_id text not null,
	view text not null,
	primary key(view_type, view_id)
);
`)

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

func (uow *transactionUow) loadView(ctx context.Context, viewType string, viewID string) ([]byte, error) {
	return uow.inner.loadView(ctx, viewType, viewID)
}

func (uow *transactionUow) saveView(ctx context.Context, viewType string, viewID string, view []byte) error {
	return uow.inner.saveView(ctx, viewType, viewID, view)
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

func (uow bareUow) loadView(ctx context.Context, viewType string, viewID string) ([]byte, error) {
	stmt := `select view from views where view_type = @type and view_id = @id`
	row := uow.db.QueryRowContext(ctx, stmt, sql.Named("type", viewType), sql.Named("id", viewID))

	var viewJson []byte
	err := row.Scan(&viewJson)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	return viewJson, nil
}

func (uow bareUow) saveView(ctx context.Context, viewType string, viewID string, view []byte) error {
	stmt := strings.TrimSpace(`
	insert into
		views(view_type, view_id, view)
		values(@type, @id, @view)
		on conflict(view_type, view) do update set view = excluded.view
	`)

	_, err := uow.db.ExecContext(ctx, stmt,
		sql.Named("type", viewType),
		sql.Named("id", viewID),
		sql.Named("view", view))

	return err
}

type Queryable interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}
