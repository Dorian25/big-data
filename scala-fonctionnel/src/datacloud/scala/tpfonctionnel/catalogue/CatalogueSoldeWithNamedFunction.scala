package datacloud.scala.tpfonctionnel.catalogue

import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithNamedFunction extends CatalogueWithNonMutable with CatalogueSolde {
  def solde(pc:Int):Unit= {
    def diminution(a:Double,percent:Int):Double = a * ((100.0-percent)/100.0)
    this.map = this.map.mapValues(x => diminution(x,pc))
  }
}