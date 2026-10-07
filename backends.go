package goes

import (
	"context"
	"encoding/json"
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
