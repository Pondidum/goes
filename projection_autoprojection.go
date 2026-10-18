package goes

import (
	"context"
	"encoding/json"
	"fmt"
)

var _ Projection = (*AutoProjection)(nil)

type AutoProjection struct {
	AggregateFactory map[string]func() Aggregate
}

type CustomView interface {
	AsView() ([]byte, error)
}

func (p *AutoProjection) Reset(ctx context.Context, unit UnitOfWork) error {
	return unit.DeleteView(ctx, "auto_projection", ViewDeleteSpec{
		All: true,
	})
}

func (p *AutoProjection) Project(ctx context.Context, unit UnitOfWork, pendingEvents []EventDescriptor) error {

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

	if custom, ok := aggregate.(CustomView); ok {
		view, err = custom.AsView()
	} else {
		view, err = json.Marshal(aggregate)
	}
	if err != nil {
		return err
	}

	viewType := "auto_projection"
	viewID := fmt.Sprintf("%s:%s", aggregateType, aggregateID)

	if err := unit.WriteView(ctx, viewType, viewID, view); err != nil {
		return err
	}

	return nil

}
