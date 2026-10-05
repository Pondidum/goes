package test

import (
	"testing"
	"github.com/stretchr/testify/require"
	"go.asamfab.fi/goes"
)

func TestIdea(t *testing.T) {

	backend := goes.NewSqliteBackend(tempSqliteDB(t), true)
	store := goes.NewEventStore(backend)

	require.NoError(t, backend.Init(t.Context()))

	recipe := NewRecipe("lihapulla")
	recipe.AddTag("tested", "true")
	recipe.AddTag("mealprep", "true")

	require.NoError(t, store.Write(t.Context(), recipe))
}
