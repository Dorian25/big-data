package datacloud.scala.tpfonctionnel.catalogue

import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithFor extends CatalogueWithNonMutable with CatalogueSolde{
  def solde(pc:Int):Unit= {
    for(k <- this.map.keys){
      this.storeProduct(k, this.getPrice(k)*(1-(pc/100.0)))
    }
  }
}