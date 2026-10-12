package goes

import (
	"context"
	"encoding/json"
	"fmt"
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
	Project(ctx context.Context, unit UnitOfWork, pendingEvents []EventDescriptor) error
}

var _ projection = (*AutoProjector)(nil)

type CustomAutoProjection interface {
	AsView() ([]byte, error)
}

type AutoProjector struct {
	AggregateFactory map[string]func() Aggregate
}

func (p *AutoProjector) Project(ctx context.Context, unit UnitOfWork, pendingEvents []EventDescriptor) error {

	aggregateType := pendingEvents[0].AggregateType
	aggregateID := pendingEvents[0].AggregateID

	factory, found := p.AggregateFactory[aggregateType]
	if !found {
		return fmt.Errorf("no factory for %s found", aggregateType)
	}

	aggregate := factory()
	state := aggregate.state()

	writtenEvents, err := unit.ReadEvents(ctx, aggregateID)
	if err != nil {
		return err
	}

	// no need to replay pendingEvents, as unit has already had WriteEvents(pendingEvents) called.
	for _, event := range writtenEvents {
		if err := state.replayEvent(event); err != nil {
			return err
		}
	}

	var view []byte

	if custom, ok := aggregate.(CustomAutoProjection); ok {
		view, err = custom.AsView()
	} else {
		view, err = json.Marshal(aggregate)
	}
	if err != nil {
		return err
	}

	viewType := "auto_projection"
	viewID := fmt.Sprintf("%s:%s", aggregateType, aggregateID)

	if err := unit.SaveView(ctx, viewType, viewID, view); err != nil {
		return err
	}

	return nil

}
