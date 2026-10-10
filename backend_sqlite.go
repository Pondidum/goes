package goes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"uuid"
)

var _ Backend = (*sqliteBackend)(nil)

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

func (uow *transactionUow) ReadEvents(ctx context.Context, aggregateID uuid.UUID) ([]EventDescriptor, error) {
	return uow.inner.ReadEvents(ctx, aggregateID)
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

func (uow bareUow) ReadEvents(ctx context.Context, aggregateID uuid.UUID) ([]EventDescriptor, error) {
	stmt := strings.TrimSpace(`
			select aggregate_id, aggregate_type, sequence, timestamp, event_type, event_data
			from events
			where aggregate_id = @aggregate_id
			order by sequence asc
		`)
	rows, err := uow.db.QueryContext(ctx, stmt, sql.Named("aggregate_id", aggregateID.String()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := []EventDescriptor{}
	for rows.Next() {

		e := EventDescriptor{}
		if err := rows.Scan(&e.AggregateID, &e.AggregateType, &e.Sequence, &e.Timestamp, &e.EventType, &e.json); err != nil {
			return nil, err
		}
		events = append(events, e)
	}

	return events, nil
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
