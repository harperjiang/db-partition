package edu.uchicago.cs.dbp

import scala.collection.mutable.HashMap
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

object MergerParams {
  /**
   * How much the combination result can exceed the average
   */
  var threshold = 1.3f
}

class PartitionMerger {

  def merge(edge: String, partition: String, fromSize: Int, toSize: Int) = {
    var pmap = new HashMap[Int, Int]();

    var psize = Array.fill(fromSize)(0)

    var ccount = 0d;

    Source.fromFile(partition).getLines.foreach(s => {
      var parts = s.split("\\s+")
      var nid = parts(1).toInt
      var pid = parts(0).toInt
      pmap += ((nid, pid))
      psize(pid) += 1
    })

    var tm = new TriangularMatrix(fromSize - 1)

    Source.fromFile(edge).getLines().filter(!_.startsWith("#")) foreach (s => {
      var parts = s.split("\\s+");
      var v1 = parts(0).toInt;
      var v2 = parts(1).toInt;
      var p1 = pmap.get(v1).get
      var p2 = pmap.get(v2).get
      if (p1 != p2) {
        ccount += 1
        var large = Math.max(p1, p2)
        var small = p1 + p2 - large
        tm.add(small, large - 1, 1)
      }
    })

    var steps = new ArrayBuffer[MergeStep]();

    // Search for max value that add up to less than average

    var average = pmap.size / toSize
    var ceiling = average * MergerParams.threshold

    var currentSize = fromSize

    while (currentSize > toSize) {
      //      var min = Double.MaxValue
      var max = Double.MinValue
      var index = (-1, -1)
      for (i <- 0 until currentSize) {
        for (j <- i + 1 until currentSize) {
          if (psize(i) + psize(j) < ceiling) {
            var value = tm.get(i, j - 1)
            if (value > max) {
              max = value
              index = (i, j)
            }
          }
          //          if (psize(i) + psize(j) < min) {
          //            min = psize(i) + psize(j)
          //            index = (i, j)
          //          }
        }
      }
      // Merge partition i and j 
      var step = new MergeStep(index._1, index._2)
      ccount -= tm.get(index._1, index._2 - 1)
      steps += step
      psize = step.convert(psize)
      tm = step.convert(tm)
      currentSize -= 1

      System.out.println(psize.mkString(","))
      System.out.println(ccount)
    }

    System.out.println(psize.mkString(","))
    System.out.println(ccount)
  }
}

//trait MergeScore {
//  def score(sizea:Int, sizeb:Int
//}

/**
 * <code>MergeStep</code> indicates a merge operation of two partition
 */
class MergeStep(i: Int, j: Int) {
  if (i >= j || i < 0)
    throw new IllegalArgumentException();

  def convert(psizes: Array[Int]): Array[Int] = {
    var converted = Array.fill(psizes.length - 1)(0)

    for (index <- 0 until converted.size) {
      if (index < j) {
        converted(index) = psizes(index)
        if (index == i)
          converted(index) += psizes(j)
      } else {
        converted(index) = psizes(index + 1)
      }
    }

    converted
  }

  def convert(tm: TriangularMatrix): TriangularMatrix = {

    var converted = new TriangularMatrix(tm.size - 1)

    for (iid <- 0 until converted.size) {
      for (jid <- iid until converted.size) {
        var value = -1
        // These are the value corresponding to i and j
        var ri = iid;
        var rj = jid + 1;

        if (rj == i) {
          value = tm.get(iid, i - 1) + tm.get(iid, j - 1)
        } else if (ri == i) {
          if (rj < j) {
            value = tm.get(i, rj - 1) + tm.get(rj, j - 1)
          } else {
            value = tm.get(i, rj) + tm.get(j, rj)
          }
        } else {
          // Not affected
          var oldi = ri
          var oldj = rj
          if (ri >= j) {
            ri += 1
          }
          if (rj >= j) {
            rj += 1
          }
          value = tm.get(ri, rj - 1)
        }

        converted.set(iid, jid, value)
      }
    }

    converted
  }

  def translate(pmap: Map[Int, Int]): Map[Int, Int] = {
    null
  }
}

class TriangularMatrix(num: Int) {

  var result = Array.fill(num * (num + 1) / 2)(0)

  def size = num

  def translate(i: Int, j: Int): Int = {
    if (i > j)
      return translate(j, i)
    return num * (num + 1) / 2 - (num - i) * (num - i + 1) / 2 + j - i
  }

  def set(i: Int, j: Int, value: Int): Unit = {
    result(translate(i, j)) = value
  }

  def add(i: Int, j: Int, value: Int): Unit = {
    result(translate(i, j)) += value
  }

  def get(i: Int, j: Int): Int = {
    return result(translate(i, j))
  }

  def print(): Unit = {
    for (i <- 0 to num - 1) {
      var from = translate(i, i)
      var to = translate(i, num - 1)
      for (t <- 0 until i)
        System.out.print("\t")
      System.out.println(result.slice(from, to + 1).mkString("\t"))
    }
  }

  //  def traverse(f: Int => Boolean): (Int, Int) = {
  //
  //  }
}