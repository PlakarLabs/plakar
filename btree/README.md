# BTree

The BTree package implements a generic B+Tree, a BTree where all the
values are in the leaves, with a "pluggable" storage backend.

The tree itself doesn't manage the storage layout, it just need a
`Storer` interface to get or update existing nodes or put new ones.
It doesn't even manage a cache of blocks or a freelist, these issues
are relegated to the storage layer.

A peculiar thing is that even the "pointer" type is parametrized,
since it could be a disk sector, a checksum, or a key in a leveldb
cache.

There is a way to "convert" one tree from one storage to another via
the `Persist` function that works in a way that's suitable for
content-addressed stores (like a packfile.)
