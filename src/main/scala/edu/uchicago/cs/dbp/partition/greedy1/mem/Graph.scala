package edu.uchicago.cs.dbp.partition.greedy1.mem

import scala.Ordering
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.LinkedList
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.PriorityQueue

class Graph {

  var objects = scala.collection.mutable.Map[String, ArrayBuffer[String]]();

  var transactions = scala.collection.mutable.Map[String, ArrayBuffer[String]]();

  var labels = scala.collection.mutable.Map[String, Int]();

  var labelCounter = scala.collection.mutable.Map[Int, AtomicInteger]();

  var sizeLimit = 0;

  def add(tran_id: String, obj_id: String) = {
    objects.getOrElseUpdate(obj_id, new ArrayBuffer[String]()) += tran_id;
    transactions.getOrElseUpdate(tran_id, new ArrayBuffer[String]()) += obj_id;
  }

  def partition(partition: Int): Unit = {
    sizeLimit = (objects.size.doubleValue() / partition).ceil.toInt;

    var tops = new PriorityQueue[(Int, String)]()(Ordering[Int].on[(Int, String)](_._1).reverse)

    objects.foreach(f => { tops.+=((f._2.size, f._1)) })

    var curpart = 0
    // Expand the label to unlabeled nodes before they exceed their limit
    while (!tops.isEmpty) {
      var labeled = expand(tops.dequeue()._2, curpart)
      if (labeled) {
        curpart += 1
        curpart %= partition
      }
    }
  }

  private def expand(itemId: String, partition: Int): Boolean = {
    if (labels.contains(itemId)) {
      // Already labeled
      return false
    }
    // Breadth search for adjacent nodes until all labeled or size exceed limit

    var counter = 0
    var buffer = new ListBuffer[String]()
    buffer += itemId

    while (!buffer.isEmpty) {
      var first = buffer.remove(0)
      if (assign(first, partition)) {
        // Stop if exceed limit
        return true
      }
      if (counter <= sizeLimit) {
        objects.get(first).get.foreach { tranId =>
          {
            if (counter <= sizeLimit) {
              transactions.get(tranId).get.foreach { adjobj =>
                {
                  if (counter <= sizeLimit) {
                    if (!labels.contains(adjobj)) {
                      buffer += adjobj
                      counter += 1
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    return true
  }

  private def assign(itemId: String, label: Int): Boolean = {
    labels.put(itemId, label);
    return labelCounter.getOrElseUpdate(label, new AtomicInteger(0)).incrementAndGet() > sizeLimit;
  }
}