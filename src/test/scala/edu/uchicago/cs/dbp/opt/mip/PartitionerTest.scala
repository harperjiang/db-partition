package edu.uchicago.cs.dbp.opt.mip

import java.util.Random

import org.junit.Assert._
import org.junit.Test

import edu.uchicago.cs.dbp.model.Edge
import edu.uchicago.cs.dbp.model.Vertex
import edu.uchicago.cs.dbp.online.opt.mip.MIPPartitioner;

class PartitionerTest {

  @Test
  def testGenlset(): Unit = {
    // First test to make sure there is at least one partition available

    var per = new MIPPartitioner(10);
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
    var max = psize.max
    var min = psize.min
    assertTrue(max - avg < 10)
    assertTrue(avg - min < 10)
  }

  @Test
  def testAssignVertex(): Unit = {
    // Case 1
    var par = new MIPPartitioner(2);

    var v0 = new Vertex(0);
    var v1 = new Vertex(1);
    var v2 = new Vertex(2);
    var v3 = new Vertex(3);

    par.partitions(0).addPrimary(v0)
    par.partitions(0).addPrimary(v1)
    for (i <- 4 to 7)
      par.partitions(0).addPrimary(new Vertex(i))
    par.partitions(1).addPrimary(v2)

    v3.adj += v0
    v3.adj += v2

    assertTrue(!par.assign(v3))

    assertEquals(1, v3.primary)
    // Increase the size of partition 2 and reassign
    for (i <- 8 to 20)
      par.partitions(1).addPrimary(new Vertex(i))
    assertTrue(par.assign(v3))
    assertEquals(0, v3.primary)
  }

  @Test
  def testAssignEdge(): Unit = {
    var par = new MIPPartitioner(3);

    var v0 = new Vertex(0)
    var v1 = new Vertex(1)
    var v2 = new Vertex(2)
    var v3 = new Vertex(3)
    par.partitions(0).addPrimary(v0)
    par.partitions(0).addPrimary(v1)
    par.partitions(0).addPrimary(v2)
    par.partitions(0).addPrimary(v3)

    var v4 = new Vertex(4)
    var v5 = new Vertex(5)

    par.partitions(1).addPrimary(v4)
    par.partitions(1).addPrimary(v5)

    var u = new Vertex(6)
    var v = new Vertex(7)

    u.adj += v0
    u.adj += v1
    u.adj += v2
    u.adj += v3

    v.adj += v4
    v.adj += v5

    for (i <- 8 to 30)
      par.partitions(2).addPrimary(new Vertex(i))

    var res = par.assign(new Edge(Array(u, v)))

    assertEquals(0, u.primary)
    assertEquals(1, v.primary)
  }

  def testAdd(): Unit = {

  }
}