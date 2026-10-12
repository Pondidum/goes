package goes

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
	"uuid"
)

type Aggregate interface {
	state() *AggregateState
	ID() uuid.UUID
}

func NewAggregateState[TAggregate Aggregate]() (apply func(event any) error, state *AggregateState) {
	state = &AggregateState{
		sequence: -1,
		handlers: map[string]func(event any) error{},
	}

	apply = func(event any) error {
		return applyEvent[TAggregate](state, event)
	}

	return apply, state
}

type AggregateState struct {
	id       uuid.UUID
	sequence int

	handlers map[string]func(event any) error

	pendingEvents []EventDescriptor
}

func (as *AggregateState) state() *AggregateState {
	return as
}

func (as *AggregateState) ID() uuid.UUID {
	return as.id
}

func (as *AggregateState) Register(eventHandler handler) {
	as.handlers[eventHandler.name] = eventHandler.action
}

func typeOfAggregate(aggregate Aggregate) string {
	return reflect.TypeOf(aggregate).Elem().Name()
}

type NewAggregateEvent struct {
	ID uuid.UUID
}

func (e NewAggregateEvent) AggregateID() uuid.UUID {
	return e.ID
}

type setter interface {
	AggregateID() uuid.UUID
}

func applyEvent[TAggregate Aggregate](state *AggregateState, event any) error {
	aggregateType := reflect.TypeFor[TAggregate]()
	if aggregateType.Kind() == reflect.Pointer {
		aggregateType = aggregateType.Elem()
	}
	aggregateName := aggregateType.Name()

	eventType := reflect.TypeOf(event)
	if eventType.Kind() == reflect.Pointer {
		eventType = eventType.Elem()
	}

	eventName := eventType.Name()

	handler, found := state.handlers[eventName]
	if !found {
		return fmt.Errorf("apply: no handler registered for %s", eventName)
	}

	if setter, ok := event.(setter); ok {
		state.id = setter.AggregateID()
	}

	if err := handler(event); err != nil {
		return err
	}

	content, err := json.Marshal(event)
	if err != nil {
		return err
	}

	descriptor := EventDescriptor{
		AggregateID:   state.id,
		AggregateType: aggregateName,
		Sequence:      state.sequence + len(state.pendingEvents) + 1,
		Timestamp:     time.Now().UTC(),
		EventType:     eventName,
		RawEvent:      content,
	}

	state.pendingEvents = append(state.pendingEvents, descriptor)

	return nil
}

func (as *AggregateState) replayEvent(ed EventDescriptor) error {
	if as.sequence >= ed.Sequence {
		return fmt.Errorf("event already applied: aggregate sequence: %v, event sequence %v", as.sequence, ed.Sequence)
	}
	event, err := ed.Event()
	if err != nil {
		return err
	}

	handler, found := as.handlers[ed.EventType]
	if !found {
		return fmt.Errorf("replay: no handler registered for %s", ed.EventType)
	}

	if setter, ok := event.(setter); ok {
		as.id = setter.AggregateID()
	}

	if err := handler(event); err != nil {
		return err
	}

	as.sequence = ed.Sequence
	return nil
}
