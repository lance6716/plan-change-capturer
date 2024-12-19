package compare

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnionFind(t *testing.T) {
	u := newUnionFind()
	u.union("a", "b")
	u.union("b", "c")
	u.union("d", "e")
	require.True(t, u.equivalent("a", "b"))
	require.True(t, u.equivalent("a", "c"))
	require.False(t, u.equivalent("a", "d"))
}
