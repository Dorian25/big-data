package datacloud.scala.tpfonctionnel.catalogue

import datacloud.scala.tpobject.catalogue.Catalogue

trait CatalogueSolde extends Catalogue {
  def solde(pc: Int): Unit
}