package edu.uchicago.cs.dbp.linearp

import scala.io.Source
import scala.collection.mutable.HashMap
class MatlabLPGenerator {

  def generate(pnum: Int, edge: String) = {
    
    var vmap = new HashMap[Int,Int]();
    
    Source.fromFile(edge).getLines().foreach(s => {
      var parts = s.split("\\s+")
      var from = parts(0).toInt
      var to = parts(1).toInt
    })
  }
}