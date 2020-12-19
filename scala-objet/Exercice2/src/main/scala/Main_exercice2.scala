package runtime

import datacloud.scala.tpobject.bintree.test.IntTreeTest
import datacloud.scala.tpobject.bintree.test.TreeTest

object Main {
    def main(args: Array[String]) {

		println("Test IntTree")
		val test_inttree = new IntTreeTest()
		test_inttree.test
		
		println("Test TreeString")
	    val test_tree = new TreeTest()
		test_tree.testString

		println("Test TreeDouble")
		test_tree.testDouble

		println("Test TreeList")
		test_tree.testList
    }
}