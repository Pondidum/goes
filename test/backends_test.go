package test

import (
	"encoding/json"
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.asamfab.fi/goes"
)

func TestIdea(t *testing.T) {
	db := tempSqliteDB(t)
	ap := &goes.AutoProjection{
		AggregateFactory: map[string]func() goes.Aggregate{
			"RecipeAggregate": func() goes.Aggregate {
				return BlankRecipe()
			},
		},
	}
	backend := goes.NewSqliteBackend(db, true)
	store := goes.NewEventStore(backend)
	store.RegisterProjection(ap)

	require.NoError(t, backend.Init(t.Context()))

	recipe := NewRecipe("lihapulla")
	recipe.AddTag("tested", "true")
	recipe.AddTag("mealprep", "true")

	require.NoError(t, store.Write(t.Context(), recipe))

	t.Run("events are written", func(t *testing.T) {
		row := db.QueryRowContext(t.Context(), `select count(*) from events where aggregate_id = ?`, recipe.ID())
		count := 0
		require.NoError(t, row.Scan(&count))
		require.Equal(t, 3, count)
	})

	t.Run("events fields are correct", func(t *testing.T) {
		row := db.QueryRowContext(t.Context(),
			`select aggregate_id, aggregate_type, sequence, timestamp, event_type, event_data  from events where aggregate_id = ? and sequence = 0`,
			recipe.ID())

		ed := goes.EventDescriptor{}
		require.NoError(t, row.Scan(
			&ed.AggregateID,
			&ed.AggregateType,
			&ed.Sequence,
			&ed.Timestamp,
			&ed.EventType,
			&ed.RawEvent,
		))

		assert.Equal(t, recipe.ID(), ed.AggregateID)
		assert.Equal(t, "RecipeAggregate", ed.AggregateType)
		assert.Equal(t, 0, ed.Sequence)
		assert.Equal(t, "RecipeCreated", ed.EventType)

		e, err := ed.Event()
		assert.NoError(t, err)
		assert.Equal(t, &RecipeCreated{
			NewAggregateEvent: goes.NewAggregateEvent{ID: recipe.ID()},
			Name:              recipe.Name,
		}, e)
	})

	t.Run("events are read", func(t *testing.T) {
		unit, err := backend.StartUnitOfWork(t.Context())
		require.NoError(t, err)
		defer unit.Cancel()

		events, err := unit.ReadEvents(t.Context(), recipe.ID())
		require.NoError(t, err)
		require.Len(t, events, 3)
	})

	t.Run("aggregate can be loaded", func(t *testing.T) {
		loaded := BlankRecipe()
		require.NoError(t, store.Load(t.Context(), recipe.ID(), loaded))

		assert.Equal(t, recipe.Name, loaded.Name)
		assert.Equal(t, recipe.Tags, loaded.Tags)
	})

	t.Run("auto projection is written", func(t *testing.T) {
		unit, err := backend.StartUnitOfWork(t.Context())
		require.NoError(t, err)
		defer unit.Cancel()

		viewJson, err := unit.LoadView(t.Context(), "auto_projection", "RecipeAggregate:"+recipe.ID().String())

		view := RecipeView{}
		require.NoError(t, json.Unmarshal(viewJson, &view))

		require.Equal(t, RecipeView{
			Name: "lihapulla",
			Tags: map[string]string{
				"tested":   "true",
				"mealprep": "true",
			},
		}, view)
	})

	t.Run("projections can be rebuilt", func(t *testing.T) {
		require.NoError(t, store.RebuildProjection(t.Context(), ap))

		unit, err := backend.StartUnitOfWork(t.Context())
		require.NoError(t, err)
		defer unit.Cancel()

		viewJson, err := unit.LoadView(t.Context(), "auto_projection", "RecipeAggregate:"+recipe.ID().String())

		view := RecipeView{}
		require.NoError(t, json.Unmarshal(viewJson, &view))

		require.Equal(t, RecipeView{
			Name: "lihapulla",
			Tags: map[string]string{
				"tested":   "true",
				"mealprep": "true",
			},
		}, view)

	})
}
