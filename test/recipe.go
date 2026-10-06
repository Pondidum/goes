package test

import (
	"go.asamfab.fi/goes"
	"uuid"
)

func BlankRecipe() *RecipeAggregate {
	l := &RecipeAggregate{
		AggregateState: goes.NewAggregateState(),
		Tags:           map[string]string{},
	}

	goes.Register(l.AggregateState, l.onCreated)
	goes.Register(l.AggregateState, l.onTagAdded)
	goes.Register(l.AggregateState, l.onTagRemoved)

	return l
}

func NewRecipe(name string) *RecipeAggregate {
	recipe := BlankRecipe()
	goes.Apply(recipe, RecipeCreated{
		ID:   uuid.New(),
		Name: name,
	})
	return recipe
}

type RecipeAggregate struct {
	*goes.AggregateState

	Name string
	// ingredients []string
	Tags map[string]string
}

type RecipeCreated struct {
	ID   uuid.UUID
	Name string
}

func (ta *RecipeAggregate) onCreated(e RecipeCreated) {
	goes.SetID(ta.AggregateState, e.ID)
	ta.Name = e.Name
}

func (ta *RecipeAggregate) AddTag(name, value string) {
	if existing, found := ta.Tags[name]; found && existing == value {
		return
	}

	goes.Apply(ta, TagAdded{
		Name:  name,
		Value: value,
	})
}

func (ta *RecipeAggregate) onTagAdded(e TagAdded) {
	ta.Tags[e.Name] = e.Value
}

func (ta *RecipeAggregate) RemoveTag(name string) {
	if _, found := ta.Tags[name]; !found {
		return
	}

	goes.Apply(ta, TagRemoved{
		Name: name,
	})
}

func (ta *RecipeAggregate) onTagRemoved(e TagRemoved) {
	delete(ta.Tags, e.Name)
}

type TagAdded struct {
	Name  string
	Value string
}

type TagRemoved struct {
	Name string
}

type RecipeView struct {
	Name string            `json:"name"`
	Tags map[string]string `json:"tags"`
}

func (l *RecipeAggregate) toView() RecipeView {
	return RecipeView{
		Name: l.Name,
		Tags: l.Tags,
	}
}
