package goes

import (
	"context"
	"uuid"
)

type Backend interface {
	StartUnitOfWork(ctx context.Context) (UnitOfWork, error)
}

type UnitOfWork interface {
	ValidateSequence(ctx context.Context, aggregateID uuid.UUID, memorySequence int) error
	WriteEvents(ctx context.Context, events []EventDescriptor) error
	ReadEvents(ctx context.Context, aggregateID uuid.UUID) ([]EventDescriptor, error)

	ReadView(ctx context.Context, spec ViewReadSpec) ([]byte, error)
	WriteView(ctx context.Context, spec ViewWriteSpec) error
	DeleteView(ctx context.Context, spec ViewDeleteSpec) error

	AllAggregateIDs(ctx context.Context) ([]uuid.UUID, error)

	Complete() error
	Cancel() error
}

type ViewReadSpec struct {
	ProjectionType string
	ViewID         string
}

type ViewWriteSpec struct {
	ProjectionType string
	ViewID         string
	Contents       []byte
}

type ViewDeleteSpec struct {
	ProjectionType string
	All            bool
	ViewID         string
}
