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

	ReadView(ctx context.Context, viewType string, viewID string) ([]byte, error)
	WriteView(ctx context.Context, viewType string, viewID string, view []byte) error
	DeleteView(ctx context.Context, viewType string, spec ViewDeleteSpec) error

	AllAggregateIDs(ctx context.Context) ([]uuid.UUID, error)

	Complete() error
	Cancel() error
}

type ViewDeleteSpec struct {
	All    bool
	ViewID string
}
