package edu.uchicago.cs.dbp.online.leopard.model

import org.junit.Test
import org.junit.Assert._

import edu.uchicago.cs.dbp.model.Partition;
import edu.uchicago.cs.dbp.model.Vertex;

class PartitionTest {

  @Test
  def testAddPrimary(): Unit = {
    var p = new Partition(3);
    var v = new Vertex(1);
    p.addPrimary(v);
    var v2 = new Vertex(2);
    p.addPrimary(v2);

    assertEquals(2, p.size())

    assertEquals(3, v.primary);
    assertEquals(3, v2.primary);
  }

  @Test
  def testAddSecondary(): Unit = {
    var p = new Partition(3);
    var v = new Vertex(1);
    p.addPrimary(v);
    var v2 = new Vertex(2);
    p.addSecondary(v2);

    assertEquals(2, p.size())

    assertEquals(3, v.primary);
    assertNotEquals(3, v2.primary);
    
    assertTrue(v2.replicas.contains(3))
  }
}