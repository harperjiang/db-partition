package edu.uchicago.cs.dbp.online.leopard.model

import org.junit.Test
import org.junit.Assert._

import edu.uchicago.cs.dbp.model.Vertex;

class VertexTest {

  @Test
  def testAssign(): Unit = {
    var v = new Vertex();
    v.assign(5);
    assertEquals(5, v.primary)
  }

  @Test
  def testAddSecondary(): Unit = {
    var v = new Vertex();
    v.addSecondary(3);
    v.addSecondary(4);
    v.addSecondary(3);

    assertEquals(2, v.replicas.size)
  }

  @Test
  def testNumPrimaryNeighbors(): Unit = {
    var v = new Vertex();

    var v1 = new Vertex();
    v1.assign(0);
    v1.addSecondary(1);
    v1.addSecondary(3);

    var v2 = new Vertex();
    v2.assign(1);
    v2.addSecondary(0);
    v2.addSecondary(2);
    v2.addSecondary(3);

    var v3 = new Vertex();
    v3.assign(2);
    v3.addSecondary(1);

    var v4 = new Vertex();
    v4.assign(2);
    v4.addSecondary(0);
    v4.addSecondary(3);

    v.attach(Array(v1, v2, v3, v4));

    var np = v.numPrimaryNeighbors(4);

    assertArrayEquals(Array(3d, 3d, 3d, 3d), np, 0.001);
  }

  @Test
  def testNumSecondaryNeighbors(): Unit = {
    var v = new Vertex();

    var v1 = new Vertex();
    v1.assign(0);
    v1.addSecondary(1);
    v1.addSecondary(3);

    var v2 = new Vertex();
    v2.assign(1);
    v2.addSecondary(0);
    v2.addSecondary(2);
    v2.addSecondary(3);

    var v3 = new Vertex();
    v3.assign(2);
    v3.addSecondary(1);

    var v4 = new Vertex();
    v4.assign(2);
    v4.addSecondary(0);
    v4.addSecondary(3);

    v.attach(Array(v1, v2, v3, v4));

    var np = v.numSecondaryNeighbors(4);

    assertArrayEquals(Array(1d, 1d, 2d, 0d), np, 0.001);
  }
}