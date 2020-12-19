package datacloud.spark.core.matrix

import org.apache.spark.rdd._
import org.apache.spark._
import datacloud.scala.tpobject.vector.VectorInt

object MatrixIntAsRDD {

  implicit def convert(rdd_vect:RDD[VectorInt]): MatrixIntAsRDD = {
		return new MatrixIntAsRDD(rdd_vect)
	}
  
  def makeFromFile(url: String, nb_partitions:Int, sc: SparkContext): MatrixIntAsRDD = {  
    val rdd_vect = sc.textFile(url, minPartitions=nb_partitions)
                .map(_.split(" ").map(_.toInt))
                .map(l => new VectorInt(l))
                .zipWithIndex()
                .sortBy(s => s._2, true)
                .map(s => s._1)
             
    return rdd_vect
  }
}

class MatrixIntAsRDD(val lines: RDD[VectorInt]) {
  
  def nbLines():Int = {
    return lines.count().toInt
  }
  
  def nbColumns():Int = {
    return lines.first().length
  }
  
  def get(i:Int,j:Int):Int = {
    
    val get_val = lines.zipWithIndex()
                     .filter(v => v._2.toInt == i)
                     .map(t => t._1.get(j))
                     .reduce((a,b)=> a)
    return get_val
  } 
  
  override def equals(a:Any):Boolean = {
    if (a.isInstanceOf[MatrixIntAsRDD]) {
      if(a.asInstanceOf[MatrixIntAsRDD].lines.getNumPartitions == lines.getNumPartitions) {
        if(a.asInstanceOf[MatrixIntAsRDD].nbLines == this.nbLines && 
            a.asInstanceOf[MatrixIntAsRDD].nbColumns == this.nbColumns) {
            val comparaison = lines.zip(a.asInstanceOf[MatrixIntAsRDD].lines)
                                   .map(t => t._1.equals(t._2))
                                   .reduce((a,b) => a && b)
            return comparaison                        
        } else {
          return false
        }
      } else {
        return false
      }
    } else {
      return false
    }
  }
  
  def +(other: MatrixIntAsRDD):MatrixIntAsRDD = {
    return lines.zip(other.lines).map(t => t._1 + t._2)
  }
  
  def transpose(): MatrixIntAsRDD = {
    // on numerote les colonnes de chaque ligne [(val1,col1),(val2,col2)]
    // on applatit ce tableau pour n'avoir qu'1 seul élément
    // on change l'ordre en mettant l'indice de la colonne en 1er
    // on group by par rapport à l'indice de colonne (key)
    // on trie par rapport à l'indice de col par ordre croissant (key)
    // col 1 devient ligne 1
    
    
    val transp = lines.flatMap(vect => vect.elements.zipWithIndex)
                .map(e => (e._2, e._1))
                .groupByKey().sortByKey(true)
                .map(g => new VectorInt(g._2 .toArray))
                
    /*
     * Autre version
     * 
     * val transp = lines.zipWithIndex()
                .flatMap(vect => vect._1.elements.zipWithIndex.map(e => (e._1,e._2,vect._2)))
                .map(e => (e._2, (e._1, e._3)))
                .groupByKey().sortByKey(true)
                .map(g => new VectorInt(g._2.toArray.sortBy(_._2).map(s=>s._1)))
    */

    return transp
           
  }
  
  def *(other: MatrixIntAsRDD):MatrixIntAsRDD = {
    if(nbColumns() == other.nbLines()){

      val r = this.transpose().lines.zip(other.lines)
                .map(vectors => vectors._1.prodDyadique(vectors._2).zipWithIndex)
                .flatMap(p => p)
                .map(v => (v._2,v._1))
                .reduceByKey((v1,v2) => v1+v2)
                //trier les lignes car ça n'arrive pas dans le meme ordre
                .sortByKey(true)
                .map(v => v._2)               
      return r        
    } else {
      return null
    }        
  }

  override def toString={
    val sb = new StringBuilder()
    lines.collect().foreach(line=> sb.append(line+"\n"))
    sb.toString()
  }
}