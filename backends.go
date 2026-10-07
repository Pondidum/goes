package goes

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"uuid"
)

func NewEventStore(backend Backend) *eventstore {
	return &eventstore{backend: backend}
}

type eventstore struct {
	backend     Backend
	projections []projection
}

type projection interface {
	ViewID(aggregate Aggregate) string
	Name() string
	Project(ctx context.Context, view []byte, aggregate Aggregate, events []EventDescriptor) ([]byte, error)
}

func (s *eventstore) RegisterProjection(projection projection) {
	s.projections = append(s.projections, projection)
}

func (s *eventstore) Write(ctx context.Context, aggregate Aggregate) error {
	state := aggregate.state()
	pendingCount := len(state.pendingEvents)

	if pendingCount == 0 {
		return nil
	}

	unit, err := s.backend.StartUnitOfWork(ctx)
	if err != nil {
		return err
	}
	defer unit.Cancel()

	if err := unit.ValidateSequence(ctx, state.id, state.sequence); err != nil {
		return err
	}

	if err := unit.WriteEvents(ctx, state.pendingEvents); err != nil {
		return err
	}

	if err := s.runProjections(ctx, unit, aggregate); err != nil {
		return err
	}

	if err := unit.Complete(); err != nil {
		return err
	}

	state.sequence = state.pendingEvents[pendingCount-1].Sequence
	state.pendingEvents = nil

	return nil
}

func (s *eventstore) runProjections(ctx context.Context, unit UnitOfWork, aggregate Aggregate) error {

	for _, projection := range s.projections {
		viewID := projection.ViewID(aggregate) // could be the aggreateID, or something else!

		// name=tags_in_use, id=1111-2222-333
		// name=all_recipies, id=1111-2222-333
		view, err := unit.LoadView(ctx, projection.Name(), viewID)
		if err != nil {
			return err
		}

		view, err = projection.Project(ctx, view, aggregate, aggregate.state().pendingEvents)
		if err != nil {
			return err
		}

		if err := unit.SaveView(ctx, projection.Name(), viewID, view); err != nil {
			return err
		}
	}

	return nil
}

type Backend interface {
	StartUnitOfWork(ctx context.Context) (UnitOfWork, error)
}

type UnitOfWork interface {
	ValidateSequence(ctx context.Context, aggregateID uuid.UUID, memorySequence int) error
	WriteEvents(ctx context.Context, events []EventDescriptor) error

	LoadView(ctx context.Context, viewType string, viewID string) ([]byte, error)
	SaveView(ctx context.Context, viewType string, viewID string, view []byte) error

	Complete() error
	Cancel() error
}

func NewSqliteBackend(db *sql.DB, enableTransactions bool) *sqliteBackend {
	return &sqliteBackend{
		db:           db,
		transactions: enableTransactions,
	}
}

var _ Backend = (*sqliteBackend)(nil)

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

func (b *sqliteBackend) StartUnitOfWork(ctx context.Context) (UnitOfWork, error) {
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
	inner UnitOfWork
}

func (uow *transactionUow) Complete() error {
	return uow.tx.Commit()
}

func (uow *transactionUow) Cancel() error {
	return uow.tx.Rollback()
}

func (uow *transactionUow) ValidateSequence(ctx context.Context, aggregateID uuid.UUID, memorySequence int) error {
	return uow.inner.ValidateSequence(ctx, aggregateID, memorySequence)
}

func (uow *transactionUow) WriteEvents(ctx context.Context, events []EventDescriptor) error {
	return uow.inner.WriteEvents(ctx, events)
}

func (uow *transactionUow) LoadView(ctx context.Context, viewType string, viewID string) ([]byte, error) {
	return uow.inner.LoadView(ctx, viewType, viewID)
}

func (uow *transactionUow) SaveView(ctx context.Context, viewType string, viewID string, view []byte) error {
	return uow.inner.SaveView(ctx, viewType, viewID, view)
}

type bareUow struct {
	db Queryable
}

func (uow bareUow) Complete() error { return nil }
func (uow bareUow) Cancel() error   { return nil }

func (uow bareUow) ValidateSequence(ctx context.Context, aggregateID uuid.UUID, memorySequence int) error {
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

func (uow bareUow) WriteEvents(ctx context.Context, events []EventDescriptor) error {
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

func (uow bareUow) LoadView(ctx context.Context, viewType string, viewID string) ([]byte, error) {
	stmt := `select view from views where view_type = @type and view_id = @id`
	row := uow.db.QueryRowContext(ctx, stmt, sql.Named("type", viewType), sql.Named("id", viewID))

	var viewJson []byte
	err := row.Scan(&viewJson)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	return viewJson, nil
}

func (uow bareUow) SaveView(ctx context.Context, viewType string, viewID string, view []byte) error {
	stmt := strings.TrimSpace(`
	insert into
		views(view_type, view_id, view)
		values(@type, @id, @view)
		on conflict(view_type, view_id) do update set view = excluded.view
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

var _ projection = (*AutoProjector)(nil)

type CustomAutoProjection interface {
	AsView() ([]byte, error)
}

type AutoProjector struct{}

func (p *AutoProjector) ViewID(aggregate Aggregate) string {
	return nameOf(aggregate) + ":" + AggregateID(aggregate).String()
}

func (p *AutoProjector) Name() string {
	return "auto_projection"
}

func (p *AutoProjector) Project(ctx context.Context, view []byte, aggregate Aggregate, events []EventDescriptor) ([]byte, error) {
	if custom, ok := aggregate.(CustomAutoProjection); ok {
		return custom.AsView()
	}

	return json.Marshal(aggregate)
}
