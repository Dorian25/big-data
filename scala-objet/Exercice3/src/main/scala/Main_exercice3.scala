package runtime

import datacloud.scala.tpobject.vector.test.VectorIntTest

object Main {
    def main(args: Array[String]) {
		println("Test VectorInt")
		val test_vector = new VectorIntTest()
		test_vector.test

		println("Test VectorInt Implicit")
		test_vector.testImplicit
    }
}