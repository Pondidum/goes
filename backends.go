package goes

import (
	"context"
	"encoding/json"
	"uuid"
)

type Backend interface {
	StartUnitOfWork(ctx context.Context) (UnitOfWork, error)
}

type UnitOfWork interface {
	ValidateSequence(ctx context.Context, aggregateID uuid.UUID, memorySequence int) error
	WriteEvents(ctx context.Context, events []EventDescriptor) error
	ReadEvents(ctx context.Context, aggregateID uuid.UUID) ([]EventDescriptor, error)

	LoadView(ctx context.Context, viewType string, viewID string) ([]byte, error)
	SaveView(ctx context.Context, viewType string, viewID string, view []byte) error

	Complete() error
	Cancel() error
}

type projection interface {
	ViewID(aggregate Aggregate) string
	Name() string
	Project(ctx context.Context, view []byte, aggregate Aggregate, events []EventDescriptor) ([]byte, error)
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
