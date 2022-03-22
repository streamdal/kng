go-rbtree
=========

Package `rbtree` provides an iterative (not recursive) red-black tree with
obvious semantics and powerful, resettable iteration.

This package was born out of a need to modify elements during tree iteration.
Most packages do not provide an obvious way to efficiently "reset" a node or an
iterator. To aid in this need, this package primarily operates on the basis of
nodes rather than node items.

For more information about a red-black tree, and to understand the
implementation, see [Wikipedia](https://en.wikipedia.org/wiki/Red%E2%80%93black_tree).

Documentation
-------------

[![GoDoc](https://godoc.org/github.com/twmb/go-rbtree?status.svg)](https://godoc.org/github.com/twmb/go-rbtree)
