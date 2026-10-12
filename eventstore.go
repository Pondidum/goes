package goes

import (
	"context"
	"uuid"
)

func NewEventStore(backend Backend) *eventstore {
	return &eventstore{backend: backend}
}

type eventstore struct {
	backend     Backend
	projections []projection
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

	if err := s.runProjections(ctx, unit, state.pendingEvents); err != nil {
		return err
	}

	if err := unit.Complete(); err != nil {
		return err
	}

	state.sequence = state.pendingEvents[pendingCount-1].Sequence
	state.pendingEvents = nil

	return nil
}

func (s *eventstore) Load(ctx context.Context, id uuid.UUID, aggregate Aggregate) error {

	unit, err := s.backend.StartUnitOfWork(ctx)
	if err != nil {
		return err
	}
	defer unit.Cancel()

	events, err := unit.ReadEvents(ctx, id)
	if err != nil {
		return err
	}

	state := aggregate.state()
	for _, event := range events {
		if err := state.replayEvent(event); err != nil {
			return err
		}
	}

	if err := unit.Complete(); err != nil {
		return err
	}

	return nil
}

// currently reads all events for a single aggregate at a time, rather than all events in order
// i.e. local (aggregate) ordering vs global (eventstream) ordering
func (s *eventstore) RebuildProjection(ctx context.Context, projection projection) error {
	// optimisation for later: check if the projection is the AutoProjector, and if it is don't read
	// events as it will do it itself, only load all aggregateIDs

	unit, err := s.backend.StartUnitOfWork(ctx)
	if err != nil {
		return err
	}
	defer unit.Cancel()

	ids, err := unit.AllAggregateIDs(ctx)
	if err != nil {
		return err
	}

	for _, aggregateID := range ids {
		events, err := unit.ReadEvents(ctx, aggregateID)
		if err != nil {
			return err
		}

		if err := projection.Project(ctx, unit, events); err != nil {
			return err
		}
	}

	if err := unit.Complete(); err != nil {
		return err
	}

	return nil
}

func (s *eventstore) runProjections(ctx context.Context, unit UnitOfWork, pendingEvents []EventDescriptor) error {

	for _, projection := range s.projections {
		if err := projection.Project(ctx, unit, pendingEvents); err != nil {
			return err
		}
	}

	return nil
}
