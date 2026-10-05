package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"go.asamfab.fi/goes"
)

func TestIdea(t *testing.T) {
	db := tempSqliteDB(t)
	backend := goes.NewSqliteBackend(db, true)
	store := goes.NewEventStore(backend)

	require.NoError(t, backend.Init(t.Context()))

	recipe := NewRecipe("lihapulla")
	recipe.AddTag("tested", "true")
	recipe.AddTag("mealprep", "true")

	require.NoError(t, store.Write(t.Context(), recipe))

	row := db.QueryRowContext(t.Context(), `select count(*) from events where aggregate_id = ?`, goes.AggregateID(recipe))
	count := 0
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 3, count)
}
