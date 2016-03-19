package edu.uchicago.cs.dbp.partition.greedy1.mem

import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

class Graph {

  var objects = scala.collection.mutable.Map[String, ArrayBuffer[String]]();

  var transactions = scala.collection.mutable.Map[String, ArrayBuffer[String]]();

  def add(tran_id: String, obj_id: String) = {
    objects.getOrElseUpdate(obj_id, new ArrayBuffer[String]()) += tran_id;
    transactions.getOrElseUpdate(tran_id, new ArrayBuffer[String]()) += obj_id;
  }
}