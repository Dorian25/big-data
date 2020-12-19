package datacloud.spark.core.stereoprix

import org.apache.spark._

object Stats extends App {
  
  
  def chiffreAffaire(url: String, annee: Int): Int = {
    val conf = new SparkConf().setAppName("chiffreAffaire").setMaster("local[*]")
    val spark = new SparkContext(conf)
    
    val chiffre_affaire = spark.textFile(url)
                         .map(line => line.split(" "))
                         .map(arr => (arr(0).split("_")(2).toInt,arr(2).toInt))
                         .filter(_._1.toInt == annee)
                         .reduce((a,b) => (annee, a._2+b._2))
    spark.stop()
    chiffre_affaire._2
  }
  
  def chiffreAffaireParCategorie(url_in: String, url_out: String)= {
    val conf = new SparkConf().setAppName("chiffreAffaireParCat").setMaster("local[*]")
    val spark = new SparkContext(conf)
    
    val chiffre_affaire_cat = spark.textFile(url_in)
                           .map(line => line.split(" "))
                           .map(arr => (arr(4),arr(2).toInt))
                           .reduceByKey(_+_)
                           .map(t => t._1 + ':' + t._2.toString())
                           
    chiffre_affaire_cat.saveAsTextFile(url_out)
    spark.stop()
  }
  
  def produitLePlusVenduParCategorie(url_in: String, url_out: String)= {
    val conf = new SparkConf().setAppName("produitPlusVenduParCat").setMaster("local[*]")
    val spark = new SparkContext(conf)
    
    val produit_plusvendu_cat = spark.textFile(url_in)
                           .map(line => line.split(" "))
                           .map(arr => (arr(4),arr(3)))
                           .groupByKey()
                           .map(t => t._1 + ":" + t._2.groupBy(identity).mapValues(_.size).maxBy(_._2)._1)
                           
    produit_plusvendu_cat.saveAsTextFile(url_out)
    spark.stop()
  }

}