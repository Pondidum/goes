package goes

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIdea(t *testing.T) {

	store := &eventstore{
		backend: &sqliteBackend{transactions: true},
	}

	require.NoError(t, store.Write())
}

type eventstore struct {
	backend backend
}

func (s *eventstore) Write() error {

	unit, err := s.backend.startUnitOfWork()
	if err != nil {
		return err
	}
	defer unit.cancel()

	// unit.writeEvents(...)
	// unit.projectEvents(...)

	if err := unit.complete(); err != nil {
		return err
	}
	return nil
}

type backend interface {
	startUnitOfWork() (unitOfWork, error)
}

type unitOfWork interface {
	writeEvents( /**/ )
	projectEvents( /**/ )

	complete() error
	cancel() error
}

type sqliteBackend struct {
	transactions bool
	db           *sql.DB
}

func (b *sqliteBackend) startUnitOfWork() (unitOfWork, error) {
	if !b.transactions {
		return &bareUow{db: b.db}, nil
	}

	tx, err := b.db.BeginTx(nil, &sql.TxOptions{})
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

func (uow *transactionUow) writeEvents() {
	uow.inner.writeEvents()
}
func (uow *transactionUow) projectEvents() {
	uow.inner.projectEvents()
}

type bareUow struct {
	db Queryable
}

func (uow bareUow) complete() error { return nil }
func (uow bareUow) cancel() error   { return nil }

func (uow bareUow) writeEvents()   {}
func (uow bareUow) projectEvents() {}

type Queryable interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}
