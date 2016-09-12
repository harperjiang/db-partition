package edu.uchicago.cs.dbp.online.opt.qp

import org.junit.Test
import org.junit.Assert._
import edu.uchicago.cs.dbp.model.Vertex

class QPPartitionerTest {
  @Test
  def testAssignVertex(): Unit = {

    Params.alpha = 2000;
    Params.beta = 2f;
    // Case 1
    var par = new QPPartitioner(3);

    var v0 = new Vertex(0);
    var v1 = new Vertex(1);
    var v2 = new Vertex(2);
    var v3 = new Vertex(3);
    var v4 = new Vertex(21);

    par.partitions(0).addPrimary(v0)
    par.partitions(0).addPrimary(v1)
    for (i <- 4 to 7)
      par.partitions(0).addPrimary(new Vertex(i))
    par.partitions(1).addPrimary(v2)

    for (i <- 8 to 20)
      par.partitions(2).addPrimary(new Vertex(i))
    par.partitions(2).addPrimary(v4)

    v3.adj += v0
    v3.adj += v2
    v3.adj += v4

    assertTrue(!par.assign(v3))

    assertEquals(1, v3.primary)
    // Increase the size of partition 2 and reassign
    for (i <- 8 to 20)
      par.partitions(1).addPrimary(new Vertex(i))
    assertTrue(par.assign(v3))
    assertEquals(0, v3.primary)

    assertTrue(par.partitions(0).vertices.contains(v3))
  }
}