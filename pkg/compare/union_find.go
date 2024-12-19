package compare

type unionFind struct {
	parent map[string]string
}

func newUnionFind() unionFind {
	return unionFind{
		parent: make(map[string]string),
	}
}

func (u unionFind) find(x string) string {
	parent, exists := u.parent[x]
	if !exists {
		u.parent[x] = x
		return x
	}

	// path compression when finding the root
	if parent != x {
		u.parent[x] = u.find(parent)
	}
	return u.parent[x]
}

func (u unionFind) union(x, y string) {
	rootX := u.find(x)
	rootY := u.find(y)

	if rootX != rootY {
		u.parent[rootY] = rootX
	}
}

func (u unionFind) equivalent(x, y string) bool {
	return u.find(x) == u.find(y)
}

func (u unionFind) parentOrSelf(x string) string {
	parent, exists := u.parent[x]
	if !exists {
		return x
	}
	return parent
}
