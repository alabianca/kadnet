package kadnet

import (
	"bytes"
	"fmt"
	"github.com/alabianca/gokad"
)

type compareFunc func(a gokad.Distance, b gokad.Distance) int //GENERIC
type traverseFunc func(k gokad.Distance, v *pendingNode) bool // GENERIC

// treeMap implementation

type treeMap struct {
	root *node
	comp compareFunc
}

func newMap(compFn compareFunc) *treeMap {
	return &treeMap{
		comp: compFn,
	}
}

func (t *treeMap) Insert(key gokad.Distance, data *pendingNode) bool { // GENERIC
	if t.root == nil {
		t.root = newNode(nil, t.comp, false, key, data)
		t.root.insertFixup(t.root)
		return true
	}

	newRoot := t.root.insert(key, data)
	if newRoot != nil {
		t.root = newRoot
		return true
	}

	return false
}

func (t *treeMap) Get(key gokad.Distance) (*pendingNode, bool) { // GENERIC
	var d *pendingNode
	var ok bool

	t.Traverse(func(k gokad.Distance, v *pendingNode) bool { // GENERIC
		if t.comp(k, key) == 0 {
			ok = true
			d = v
			return false
		}

		return true
	})

	return d, ok
}

func (t treeMap) String() string {
	buf := new(bytes.Buffer)
	t.Traverse(func(k gokad.Distance, v *pendingNode) bool { // GENERIC
		buf.WriteString(fmt.Sprintf("%v(%v)->", k, v))
		return true
	})

	return buf.String()
}

func (t *treeMap) Traverse(proj traverseFunc) {
	t.traverse(t.root, proj)
}

func (t *treeMap) TraverseBF(proj traverseFunc) {
	t.traverseBF(t.root, proj)
}

func (t *treeMap) traverseBF(n *node, proj traverseFunc) {
	if n == nil {
		return
	}

	nodes := make([]*node, 0)
	nodes = append(nodes, n)

	for len(nodes) > 0 {
		x := nodes[0]
		cont := proj(x.data.key, x.data.data)
		if !cont {
			break
		}

		nodes = nodes[1:]
		if x.left != nil {
			nodes = append(nodes, x.left)
		}
		if x.right != nil {
			nodes = append(nodes, x.right)
		}
	}
}

func (t *treeMap) traverse(n *node, proj traverseFunc) {
	if n == nil {
		return
	}

	t.traverse(n.left, proj)
	cont := proj(n.data.key, n.data.data)
	if !cont {
		return
	}

	t.traverse(n.right, proj)
}

// Node Implementation. Everything must be private at this point
type nodeData struct { // GENERIC
	key  gokad.Distance
	data *pendingNode
}

type node struct {
	black       bool
	isLeftChild bool
	right       *node
	left        *node
	parent      *node
	data        nodeData
	comp        compareFunc
}

// GENERIC
func newNode(parent *node, c compareFunc, isLeftChild bool, key gokad.Distance, data *pendingNode) *node {
	return &node{
		black:       false, // a node is always inserted red
		isLeftChild: isLeftChild,
		right:       nil,
		left:        nil,
		parent:      parent,
		data: nodeData{
			data: data,
			key:  key,
		},
		comp: c,
	}
}

// GENERIC
func (n *node) insert(key gokad.Distance, data *pendingNode) *node {
	addedNode := n.insertSimple(key, data)
	if addedNode != nil {
		return n.insertFixup(addedNode)
	}

	return addedNode
}

// GENERIC
func (n *node) insertSimple(key gokad.Distance, data *pendingNode) *node {
	res := n.comp(n.data.key, key)
	if res < 0 {
		if n.left != nil {
			return n.left.insertSimple(key, data)
		}
		n.left = newNode(n, n.comp, true, key, data)
		return n.left
	}

	if res > 0 {
		if n.right != nil {
			return n.right.insertSimple(key, data)
		}
		n.right = newNode(n, n.comp, false, key, data)
		return n.right
	}

	return nil
}

func (n *node) insertFixup(x *node) *node {
	parent := x.parent
	grandParent := grandParent(x)
	unc := uncle(x)
	v := violation(x)
	if v == noViolation && isRoot(x) {
		return x
	}

	var nextViolatingNode *node
	switch v {
	// toggle root black
	case redRootViolation:
		toggleColor(x)
		nextViolatingNode = x
	// toggle color of parent, grandparent and uncle
	case redUncleViolation:
		toggleColor(parent)
		toggleColor(unc)
		toggleColor(grandParent)
		nextViolatingNode = parent
	// rotate around parent node and turn it into a line violation
	case triangleViolation:
		isLeft := x.isLeftChild
		if isLeft {
			rotateRight(parent)
		} else {
			rotateLeft(parent)
		}
		nextViolatingNode = parent
	// rotate grandparent and toggle color of parent and grandparent
	case lineViolation:
		isLeft := x.isLeftChild
		if isLeft {
			rotateRight(grandParent)
		} else {
			rotateLeft(grandParent)
		}

		toggleColor(parent)
		toggleColor(grandParent)

		nextViolatingNode = parent

	default:
		nextViolatingNode = parent
	}

	return n.insertFixup(nextViolatingNode)

}


// utility functions

func isBlack(n *node) bool {
	return n == nil || n.black
}

func grandParent(n *node) *node {
	if n.parent == nil {
		return nil
	}

	return n.parent.parent
}

func uncle(n *node) *node {
	gp := grandParent(n)
	if gp == nil {
		return nil
	}

	isLeft := n.parent.isLeftChild
	if isLeft {
		return gp.right
	}

	return gp.left
}

// if a node does not have a parent it is the root
func isRoot(n *node) bool {
	return n.parent == nil
}

// Node is a right child of a right child OR left child of a left child
func lineArrangement(n *node) bool {
	p := n.parent
	if p == nil || isRoot(p) {
		return false
	}

	return (n.isLeftChild && p.isLeftChild) || (!n.isLeftChild && !p.isLeftChild)
}

// Node is a right child of a left child OR left child of a right child
func triangleArrangement(n *node) bool {
	p := n.parent
	if p == nil || isRoot(p){
		return false
	}

	return (!n.isLeftChild && p.isLeftChild) || (n.isLeftChild && !p.isLeftChild)
}

func toggleColor(n *node) {
	if n == nil {
		return
	}
	n.black = !n.black
}

func rotateLeft(n *node) {
	if n == nil || n.right == nil {
		return
	}

	oldParent := n.parent
	grandson := n.right.left
	newParent := n.right
	n.parent = newParent
	n.right = grandson
	newParent.parent = oldParent
	if oldParent != nil {
		if n.isLeftChild {
			oldParent.left = newParent
			newParent.isLeftChild = true
		} else {
			oldParent.right = newParent
			newParent.isLeftChild = false
		}
	}
	newParent.left = n
	n.isLeftChild = true

}

func rotateRight(n *node) {
	if n == nil || n.left == nil {
		return
	}

	oldParent := n.parent
	grandson := n.left.right
	newParent := n.left
	n.parent = newParent
	n.left = grandson
	newParent.parent = oldParent
	if oldParent != nil {
		if n.isLeftChild {
			oldParent.left = newParent
			newParent.isLeftChild = true
		} else {
			oldParent.right = newParent
			newParent.isLeftChild = false
		}
	}
	newParent.right = n
	n.isLeftChild = false
}

// INSERT FIXUP
// 4 Cases/Violations we need to account for
/*	0. Root is red
	1. Violating node's uncle is red
		Solution: Toggle color of Parent, Grandparent and Uncle
	2. (Violating node's uncle is black && Violating node is a right child of a left child) OR (Violating node's uncle is black && Violating node is a left child of a right child)
		Solution: Rotate around parent node. Turn it into case 3
	3. Violating node is a left child of a left child OR Violating node is a right child of a right child
		Solution: Rotate around grandparent AND toggle color of parent and grandparent
*/
type insertViolation int
const noViolation = insertViolation(0)
const redRootViolation = insertViolation(1)
const redUncleViolation = insertViolation(2)
const lineViolation = insertViolation(3)
const triangleViolation = insertViolation(4)
func violation(n *node) insertViolation {
	// 0
	if isRoot(n) && !isBlack(n) {
		return redRootViolation
	}

	// 1
	unc := uncle(n)
	if unc != nil && !isBlack(unc) {
		return redUncleViolation
	}

	// 2
	if triangleArrangement(n) && !isBlack(n) && !isBlack(n.parent) {
		return triangleViolation
	}

	// 3
	if lineArrangement(n) && !isBlack(n) && !isBlack(n.parent) {
		return lineViolation
	}

	return noViolation
}


