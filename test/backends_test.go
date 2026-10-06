package test

import (
	"database/sql"
	"encoding/json"
	"testing"
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

	row := db.QueryRowContext(t.Context(), `select count(*) from events where aggregate_id = ?`, goes.AggregateID(recipe))
	count := 0
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 3, count)

	var viewJson []byte

	row = db.QueryRowContext(t.Context(),
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
}
