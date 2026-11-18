package catalog

// Directory exposes immutable authoring descriptions for both current and
// historical drafts. Runtime contracts remain internal to the compiler.
type Directory struct {
	current  *Catalog
	versions map[string]*Catalog
}

func BuiltinDirectory() Directory {
	v1, v2 := BuiltinV1(), BuiltinV2()
	return Directory{current: v2, versions: map[string]*Catalog{string(v1.Revision()): v1, string(v2.Revision()): v2}}
}

func (directory Directory) Descriptions() []NodeDescription { return directory.current.Descriptions() }

func (directory Directory) DescriptionsForRevision(revision string) ([]NodeDescription, bool) {
	value, exists := directory.versions[revision]
	if !exists {
		return nil, false
	}
	return value.Descriptions(), true
}
