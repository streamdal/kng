// Package rbtree provides an iterative (not recursive) red-black tree with
// obvious semantics and powerful, resettable iteration.
//
// This package provides the easy ability to modify nodes on the fly, but note
// that this comes with the easy footgun of messing up iteration. If you modify
// a tree during iteration, you will likely need to reset your iterator. The
// node under the iterator may no longer be where you expect.
//
// For more information about a red-black tree, and to understand the
// implementation, see Wikipedia:
//
//     https://en.wikipedia.org/wiki/Red%E2%80%93black_tree
package rbtree

type color bool

const red, black color = true, false

// Item is the interface required for implenting to use a tree.
//
// The red-black tree does support duplicate items, but one is not guaranteed
// which duplicate is found when looking to remove one of many duplicates.
// Essentially, there is no stable ordering to duplicate items.
type Item interface {
	// Less returns whether an item is less than another item.
	Less(other Item) bool
}

// Tree is a red-black tree.
type Tree struct {
	root *Node
	size int
}

// Node is an element in a tree containing a user provided item.
type Node struct {
	left   *Node
	right  *Node
	parent *Node
	color  color
	Item   Item
}

// liftRightSideOf is rotateLeft.
//
// Graphically speaking, this takes the node on the right and lifts it above
// ourselves. IMO trying to visualize a "rotation" is confusing.
func (t *Tree) liftRightSideOf(n *Node) {
	r := n.right
	t.relinkParenting(n, r)

	// lift the right
	n.right = r.left
	n.parent = r

	// fix the lifted right's left
	if r.left != nil {
		r.left.parent = n
	}
	r.left = n
}

// liftLeftSideOf is rotateRight, renamed to aid my visualization.
func (t *Tree) liftLeftSideOf(n *Node) {
	l := n.left
	t.relinkParenting(n, l)

	n.left = l.right
	n.parent = l

	if l.right != nil {
		l.right.parent = n
	}
	l.right = n
}

// relinkParenting is called to fix a former child c of node n's parent
// relationship to the parent of n.
//
// After this, the n node can be considered to have no parent.
func (t *Tree) relinkParenting(n, c *Node) {
	p := n.parent
	if c != nil {
		c.parent = p
	}
	if p == nil {
		t.root = c
		return
	}
	if n == p.left {
		p.left = c
	} else {
		p.right = c
	}
}

func (n *Node) sibling() *Node {
	if n.parent == nil {
		return nil
	}
	if n == n.parent.left {
		return n.parent.right
	}
	return n.parent.left
}

func (n *Node) uncle() *Node {
	p := n.parent
	if p.parent == nil {
		return nil
	}
	return p.sibling()
}

func (n *Node) grandparent() *Node {
	return n.parent.parent
}

func (n *Node) isBlack() bool {
	return n == nil || n.color == black
}

// Fix removes a node from the tree and reinserts it. This can be used to "fix"
// a node after the item has been updated, removing some minor garbage.
// This returns the updated node.
//
// This is shorthand for t.Reinsert(t.Delete(n)).
func (t *Tree) Fix(n *Node) *Node {
	r := t.Delete(n)
	t.Reinsert(r)
	return r
}

// Insert inserts an item into the tree, returning the new node.
func (t *Tree) Insert(i Item) *Node {
	r := &Node{Item: i}
	t.Reinsert(r)
	return r
}

// Reinsert inserts a node into the tree.
//
// This function is provided to aid in garbage reduction / ease of use when
// deleting, updating, and reinserting items into a tree. The only value
// required in the node is the item. All other fields are set inside this
// function.
func (t *Tree) Reinsert(n *Node) {
	*n = Node{
		color: red,
		Item:  n.Item,
	}
	t.size++
	if t.root == nil {
		n.color = black
		t.root = n
		return
	}

	on := t.root
	var set **Node
	for {
		if n.Item.Less(on.Item) {
			if on.left == nil {
				set = &on.left
				break
			}
			on = on.left
		} else {
			if on.right == nil {
				set = &on.right
				break
			}
			on = on.right
		}
	}

	n.parent = on
	*set = n

repair:
	// Case 1: we have jumped back to the root. Paint it black.
	if n.parent == nil {
		n.color = black
		return
	}

	// Case 2: if our parent is black, us being red does not add a new black
	// to the chain and cannot increase the maximum number of blacks from
	// root, so we are done.
	if n.parent.color == black {
		return
	}

	// Case 3: if we have an uncle and it is red, then we flip our
	// parent's, uncle's, and grandparent's color.
	//
	// This stops the red-red from parent to us, but may introduce
	// a red-red from grandparent to its parent, so we set ourselves
	// to the grandparent and go back to the repair beginning.
	if uncle := n.uncle(); uncle != nil && uncle.color == red {
		n.parent.color = black
		uncle.color = black
		n = n.grandparent()
		n.color = red
		goto repair
	}

	// Case 4 step 1: our parent is red but uncle is black. Step 2 relies
	// on the node being on the "outside". If we are on the inside, our
	// parent lifts ourselves above itself, thus making the parent the
	// outside, and then we become that parent.
	p := n.parent
	g := p.parent
	if n == p.right && p == g.left {
		t.liftRightSideOf(p)
		n = n.left
	} else if n == p.left && p == g.right {
		t.liftLeftSideOf(p)
		n = n.right
	}

	// Care 4 step 2: we are on the outside, and we and our parent are red.
	// If we are on the left, our grandparent lifts its left and then swaps
	// its and our parent's colors.
	//
	// This fixes the red-red situation while preserving the number of
	// blacks from root to leaf property.
	p = n.parent
	g = p.parent

	if n == p.left {
		t.liftLeftSideOf(g)
	} else {
		t.liftRightSideOf(g)
	}
	p.color = black
	g.color = red
}

// Delete removes a node from the tree, returning the resulting node that was
// removed. Note that you cannot reuse the "deleted" node for anything; you
// must use the returned node. The node for deletion may actually remain in
// the tree with a different item.
func (t *Tree) Delete(n *Node) *Node {
	t.size--

	// We only want to delete nodes with at most one child. If this has
	// two, we find the max node on the left, set this node's item to that
	// node's item, and then delete that max node.
	if n.left != nil && n.right != nil {
		remove := n.left.max()
		n.Item, remove.Item = remove.Item, n.Item
		n = remove
	}

	// Determine which child to elevate into our position now that we know
	// we have at most one child.
	c := n.right
	if n.right == nil {
		c = n.left
	}

	t.doDelete(n, c)
	t.relinkParenting(n, c)

	return n
}

// Since we do not represent leave nodes with objects, we relink the parent
// after deleting. See the Wikipedia note. Most of our deletion operations
// on n (the dubbed "shadow" node) rather than c.
func (t *Tree) doDelete(n, c *Node) {
	// If the node was red, we deleted a red node; the number of black
	// nodes along any path is the same and we can quit.
	if n.color != black {
		return
	}

	// If the node was black, then, if we have a child and it is red,
	// we switch the child to black to preserve the path number.
	if c != nil && c.color == red {
		c.color = black
		return
	}

	// We either do not have a child (nil is black), or we do and it
	// is black. We must preserve the number of blacks.

case1:
	// Case 1: if the child is the new root, then the tree must have only
	// had up to two elements and now has one or zero.  We are done.
	if n.parent == nil {
		return
	}

	// Note that if we are here, we must have a sibling.
	//
	// The first time through, from the deleted node, the deleted node was
	// black and the child was black. This being two blacks meant that the
	// original node's parent required two blacks on the other side.
	//
	// The second time through, through case 3, the sibling was repainted
	// red... so it must still exist.

	// Case 2: if the child's sibling is red, we recolor the parent and
	// sibling and lift the sibling, ensuring we have a black sibling.
	s := n.sibling()
	if s.color == red {
		n.parent.color = red
		s.color = black
		if n == n.parent.left {
			t.liftRightSideOf(n.parent)
		} else {
			t.liftLeftSideOf(n.parent)
		}
		s = n.sibling()
	}

	// Right here, we know the sibling is black. If both sibling children
	// are black or nil leaves (black), we enter cases 3 and 4.
	if s.left.isBlack() && s.right.isBlack() {
		// Case 3: if the parent, sibling, sibling's children are
		// black, we can paint the sibling red to fix the imbalance.
		// However, the same black imbalance can exist on the other
		// side of the parent, so we go back to case 1 on the parent.
		s.color = red
		if n.parent.color == black {
			n = n.parent
			goto case1
		}

		// Case 4: if the sibling and sibling's children are black, but
		// the parent is red, We can swap parent and sibling colors to
		// fix our imbalance. We have no worry of further imbalances up
		// the tree since we deleted a black node, replaced it with a
		// red node, and then painted that red node black.
		n.parent.color = black
		return
	}

	// Now we know the sibling is black and one of its children is red.

	// Case 5: in preparation for 6, if we are on the left, we want our
	// sibling, if it has a right child, for that child's color to be red.
	// We swap the sibling and sibling's left's color (since we know the
	// sibling has a red child and that the right is black) and we lift the
	// left child.
	//
	// This keeps the same number of black nodes and under the sibling.
	if n == n.parent.left && s.right.isBlack() {
		s.color = red
		s.left.color = black
		t.liftLeftSideOf(s)
	} else if n == n.parent.right && s.left.isBlack() {
		s.color = red
		s.right.color = black
		t.liftRightSideOf(s)
	}
	s = n.sibling() // can change from the above case

	// At this point, we know we have a black sibling and, if we are on
	// the left, it has a red child on its right.

	// Case 6: we lift the sibling above the parent, swap the sibling's and
	// parent's color, and change the sibling's right's color from red to
	// black.
	//
	// This brings in a black above our node to replace the one we deleted,
	// while preserves the number of blacks on the other side of the path.
	s.color = n.parent.color
	n.parent.color = black
	if n == n.parent.left {
		s.right.color = black
		t.liftRightSideOf(n.parent)
	} else {
		s.left.color = black
		t.liftLeftSideOf(n.parent)
	}
}

// Find finds a node in the tree equal to the needle, if any.
func (t *Tree) Find(needle Item) *Node {
	on := t.root
	var lastLarger *Node
	for on != nil {
		// If the needle is less than our node, then we have not found
		// the min and should go left. We cannot clear lastLarger since
		// it is still a candidate for equality.
		//
		// If the needle is not less than, it could be equal to our
		// node. We recurse right, saving what could be equal.
		if needle.Less(on.Item) {
			on = on.left
		} else {
			lastLarger = on
			on = on.right
		}
	}

	// If we found a node that did not compare less than our needle,
	// if our needle is not less than the candidate,
	// then the needle and candidate are equal.
	if lastLarger != nil && !lastLarger.Item.Less(needle) {
		return lastLarger
	}
	return nil
}

// FindOrInsert finds a node equal to the needle, if any. If the node does not
// exist, this inserts a new node into the tree with new and returns that node.
func (t *Tree) FindOrInsert(needle Item) *Node {
	found := t.Find(needle)
	if found == nil {
		return t.Insert(needle)
	}
	return found
}

// FindWith calls cmp to find a node in the tree. To continue searching left,
// cmp must return negative. To continue searching right, cmp must return
// positive. To stop at the node containing the current item to cmp, return
// zero.
//
// If cmp never returns zero, this returns nil.
//
// This can be used to find an arbitrary node meeting a condition.
func (t *Tree) FindWith(cmp func(*Node) int) *Node {
	on := t.root
	for on != nil {
		way := cmp(on)
		switch {
		case way < 0:
			on = on.left
		case way == 0:
			return on
		case way > 0:
			on = on.right
		}
	}
	return nil
}

// FindWithOrInsertWith calls cmp to find a node in the tree following the
// semantics described in FindWith. If the cmp function never returns zero,
// this inserts a new node into the tree with new and returns that node.
func (t *Tree) FindWithOrInsertWith(
	find func(*Node) int,
	insert func() Item,
) *Node {
	found := t.FindWith(find)
	if found == nil {
		return t.Insert(insert())
	}
	return found
}

// Len returns the current size of the tree.
func (t *Tree) Len() int { return t.size }

// Into returns a fake node that to both the Left or the Right will be the
// given node. This can be used in combination with iterating to reset
// iteration to the given node.
func Into(n *Node) *Node { return &Node{parent: n} }

// Min returns the minimum node in the tree, or nil if empty.
func (t *Tree) Min() *Node {
	if t.root == nil {
		return nil
	}
	return t.root.min()
}

func (on *Node) min() *Node {
	for on.left != nil {
		on = on.left
	}
	return on
}

// Max returns the maximum node in the tree, or nil if empty.
func (t *Tree) Max() *Node {
	if t.root == nil {
		return nil
	}
	return t.root.max()
}

func (on *Node) max() *Node {
	for on.right != nil {
		on = on.right
	}
	return on
}

// IterAt returns an iterator for a tree starting on the given node.
//
// Note that modifications to the tree during iteration may invalidate the
// iterator and the iterator may need resetting.
func IterAt(on *Node) Iter {
	return Iter{on}
}

// Iter iterates over a tree. This returns nodes in the tree to support easy
// node deletion.
//
// Note that if you modify the tree during iteration, you need to reset the
// iterator or stop iterating.
type Iter struct {
	on *Node
}

// Ok returns whether the iterator is on a node.
func (i Iter) Ok() bool { return i.on != nil }

// Node returns the node the iterator is currently on, if any.
func (i Iter) Node() *Node { return i.on }

// Item returns the item in the node the iterator is currently on. This will
// panic if the iterator is not on a node.
//
// This is shorthand for i.Node().Item.
func (i Iter) Item() Item { return i.Node().Item }

// PeekLeft peeks at the next left node the iterator can move onto without
// moving the iterator. This will panic if not on a node.
//
// For maximal efficiency, to move left after a left peek, use Reset with the
// peeked node.
func (i Iter) PeekLeft() *Node { return (&Iter{i.on}).Left() }

// PeekRight peeks at the next right node the iterator can move onto without
// moving the iterator. This will panic if not on a node.
//
// For maximal efficiency, to move right after a right peek, use Reset with the
// peeked node.
func (i Iter) PeekRight() *Node { return (&Iter{i.on}).Right() }

// Reset resets the iterator to be on the given node.
func (i *Iter) Reset(on *Node) { i.on = on }

// Left moves the iterator to the left (more towards the min) and returns the
// resulting node it lands on, or nil if this moves past the left side of the
// tree. This will panic if not on a node.
func (i *Iter) Left() *Node {
	if i.on.left != nil {
		i.on = i.on.left
		for i.on.right != nil {
			i.on = i.on.right
		}
	} else {
		p := i.on.parent
		for p != nil && p.left == i.on {
			i.on = p
			p = i.on.parent
		}
		i.on = p
	}
	return i.on
}

// Right moves the iterator to the right (more towards the max) and returns the
// resulting node it lands on, or nil if this moves past the right side of the
// tree. This will panic if not on a node.
func (i *Iter) Right() *Node {
	if i.on.right != nil {
		i.on = i.on.right
		for i.on.left != nil {
			i.on = i.on.left
		}
	} else {
		p := i.on.parent
		for p != nil && p.right == i.on {
			i.on = p
			p = i.on.parent
		}
		i.on = p
	}
	return i.on
}
