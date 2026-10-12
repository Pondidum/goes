package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.asamfab.fi/goes"
)

func TestRecipe(t *testing.T) {
	ctx := t.Context()

	backend := goes.NewSqliteBackend(tempSqliteDB(t), true)
	require.NoError(t, backend.Init(ctx))

	store := goes.NewEventStore(backend)

	recipe := NewRecipe("some_test")
	recipe.AddTag("type", "snack")

	require.NoError(t, store.Write(ctx, recipe))

	recipe.RemoveTag("type")
	recipe.AddTag("style", "finnish")

	require.NoError(t, store.Write(ctx, recipe))

	// load

	t.Run("loading aggregate by id", func(t *testing.T) {
		loaded := BlankRecipe()
		assert.NoError(t, store.Load(t.Context(), recipe.ID(), loaded))
		assert.Equal(t, recipe.Name, loaded.Name)
		assert.Equal(t, recipe.Tags, loaded.Tags)

	})

	// t.Run("load auto projection view by id", func(t *testing.T) {
	// 	var view RecipeView
	// 	assert.NoError(t, store.ViewById(recipe.State.ID(), &view))
	// 	assert.Equal(t, recipe.Name, view.Name)
	// 	assert.Equal(t, recipe.tags, view.Tags)
	// })

	// t.Run("load projection view by property", func(t *testing.T) {
	// 	var view RecipeView
	// 	assert.NoError(t, store.ViewByProperty("$.name", recipe.Name, &view))
	// 	assert.Equal(t, recipe.Name, view.Name)
	// 	assert.Equal(t, recipe.tags, view.Tags)
	// })

}
