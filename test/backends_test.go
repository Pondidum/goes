package test

import (
	"database/sql"
	"encoding/json"
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.asamfab.fi/goes"
)

func TestIdea(t *testing.T) {
	db := tempSqliteDB(t)
	ap := &goes.AutoProjector{}
	backend := goes.NewSqliteBackend(db, true)
	store := goes.NewEventStore(backend)
	store.RegisterProjection(ap)

	require.NoError(t, backend.Init(t.Context()))

	recipe := NewRecipe("lihapulla")
	recipe.AddTag("tested", "true")
	recipe.AddTag("mealprep", "true")

	require.NoError(t, store.Write(t.Context(), recipe))

	t.Run("events are written", func(t *testing.T) {
		row := db.QueryRowContext(t.Context(), `select count(*) from events where aggregate_id = ?`, goes.AggregateID(recipe))
		count := 0
		require.NoError(t, row.Scan(&count))
		require.Equal(t, 3, count)
	})

	t.Run("events are read", func(t *testing.T) {
		unit, err := backend.StartUnitOfWork(t.Context())
		require.NoError(t, err)
		defer unit.Cancel()

		events, err := unit.ReadEvents(t.Context(), goes.AggregateID(recipe))
		require.NoError(t, err)
		require.Len(t, events, 3)
	})

	t.Run("aggregate can be loaded", func(t *testing.T) {
		loaded := BlankRecipe()
		require.NoError(t, store.Load(t.Context(), goes.AggregateID(recipe), loaded))

		assert.Equal(t, recipe.Name, loaded.Name)
		assert.Equal(t, recipe.Tags, loaded.Tags)
	})

	t.Run("auto projection is written", func(t *testing.T) {
		var viewJson []byte

		row := db.QueryRowContext(t.Context(),
			`select view from views where view_type = @type and view_id = @id`,
			sql.Named("type", ap.Name()),
			sql.Named("id", ap.ViewID(recipe)),
		)
		require.NoError(t, row.Scan(&viewJson))

		type recipeView struct {
			Name string
			Tags map[string]string
		}

		view := recipeView{}
		require.NoError(t, json.Unmarshal(viewJson, &view))

		require.Equal(t, recipeView{
			Name: "lihapulla",
			Tags: map[string]string{
				"tested":   "true",
				"mealprep": "true",
			},
		}, view)

	})
}
