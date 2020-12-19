package datacloud.scala.tpobject.bintree

import datacloud.scala.tpobject.bintree.EmptyTree
import datacloud.scala.tpobject.bintree.Node
import datacloud.scala.tpobject.bintree.Tree

sealed abstract class IntTree
case object EmptyIntTree extends IntTree
case class NodeInt(elem : Int, left : IntTree, right : IntTree) extends IntTree

// class utilitaire == statique
object BinTrees {

	def contains(tree:IntTree, elem : Int) : Boolean = tree match{
		case e: EmptyIntTree.type => false
		case n: NodeInt => 
			if(n.elem == elem) {
				true
			} else {
				contains(n.left, elem) || contains(n.right, elem)
			}
	}

	def contains[A](tree:Tree[A], elem : A) : Boolean = tree match{
		case e: EmptyTree.type => false
		case n: Node[A] => 
			if(n.elem == elem) {
				true
			} else {
				contains(n.left, elem) || contains(n.right, elem)
			}
	}

	def size(tree: IntTree) : Int = tree match{
		case e: EmptyIntTree.type => 0
		case n: NodeInt => 1 + size(n.left) + size(n.right)
	}

	def size[A](tree: Tree[A]) : Int = tree match{
		case e: EmptyTree.type => 0
		case n: Node[A] => 1 + size(n.left) + size(n.right)
	}

	def insert(tree:IntTree, elem:Int) : IntTree = tree match {
		case e: EmptyIntTree.type => new NodeInt(elem,EmptyIntTree,EmptyIntTree)
		case n: NodeInt => 
			if(size(n.left) < size(n.right)) {
				new NodeInt(n.elem, insert(n.left, elem), n.right)
			} else {
				new NodeInt(n.elem, n.left, insert(n.right, elem)) 
			}
	}

	def insert[A](tree:Tree[A], elem:A) : Tree[A] = tree match {
		case e: EmptyTree.type => new Node(elem,EmptyTree,EmptyTree)
		case n: Node[A] => 
			if(size(n.left) < size(n.right)) {
				new Node[A](n.elem, insert(n.left, elem), n.right)
			} else {
				new Node[A](n.elem, n.left, insert(n.right, elem)) 
			}
	}
}