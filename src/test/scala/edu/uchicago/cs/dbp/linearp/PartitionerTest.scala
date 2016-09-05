package edu.uchicago.cs.dbp.linearp

import java.util.Random

import org.junit.Assert._
import org.junit.Test

import edu.uchicago.cs.dbp.leopard.model.Vertex

class PartitionerTest {

  @Test
  def testGenlset(): Unit = {
    // First test to make sure there is at least one partition available

    var per = new Partitioner(10);
    for (i <- 0 to 100) {
      var set = per.genlset()
      assertTrue(set.size < 10)
    }

    // In each iteration, increase the size of available partitions, 
    // check whether the result is balanced after multiple iterations
    var all = Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    var random = new Random(System.currentTimeMillis())
    var count = 1;
    for (i <- 0 to 10000) {
      var set = per.genlset();
      var leave = all -- set;
      var choose = random.nextInt(leave.size)
      var to = leave.toList(choose)
      per.partitions(to).addPrimary(new Vertex(count))
      count += 1;

    }
    var psize = per.partitions.map { _.size }
    var avg = psize.sum.toDouble / psize.size
    var esqsize = psize.map(Math.pow(_, 2)).sum / psize.size
    var variance = esqsize - Math.pow(avg, 2)
    assertTrue(variance < 1)
  }

  @Test
  def testAssignVertex(): Unit = {
    // Case 1
    var par = new Partitioner(2);

    var v0 = new Vertex(0);
    var v1 = new Vertex(1);
    var vmore = new Vertex(4);
    var vmore2 = new Vertex(5);
    var v2 = new Vertex(2);
    var v3 = new Vertex(3);

    par.partitions(0).addPrimary(v0)
    par.partitions(0).addPrimary(v1)
    par.partitions(0).addPrimary(vmore)
    par.partitions(0).addPrimary(vmore2)
    par.partitions(1).addPrimary(v2)

    v3.adj += v0
    v3.adj += v2

    assertTrue(!par.assign(v3))

    assertEquals(1, v3.primary)
  }

  def testAssignEdge(): Unit = {

  }

}