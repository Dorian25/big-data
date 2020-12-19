package runtime

import datacloud.scala.tpobject.catalogue.test.CatalogueWithNonMutableTest
import datacloud.scala.tpobject.catalogue.test.CatalogueWithMutableTest

object Main {
    def main(args: Array[String]) {

		println("Test Mutable")
		val test_mutable = new CatalogueWithMutableTest()
		test_mutable.test

		println("Test Non Mutable")
	    val test_nonmutable = new CatalogueWithNonMutableTest()
		test_nonmutable.test
    }
}