package edu.uchicago.cs.dbp.online.mlayer

import org.junit.Assert._
import org.junit.Before
import org.junit.Test

import edu.uchicago.cs.dbp.model.Edge
import edu.uchicago.cs.dbp.model.Vertex
import java.util.HashSet

class HyperVertexTest {

  val mlp = new MLayerPartitioner(10)

  val v1 = new Vertex(1)
  val v2 = new Vertex(2)
  val v3 = new Vertex(3)
  val v4 = new Vertex(4)
  val v5 = new Vertex(5)

  var hv1: mlp.HyperVertex = null
  var hv2: mlp.HyperVertex = null
  var hv3: mlp.HyperVertex = null

  @Before
  def prepare: Unit = {
    new Edge(v1, v2)
    new Edge(v1, v3)
    new Edge(v2, v3)
    new Edge(v3, v4)
    new Edge(v3, v5)

    hv1 = new mlp.HyperVertex
    hv2 = new mlp.HyperVertex
    hv3 = new mlp.HyperVertex

    hv1.add(v1)
    hv1.add(v2)
    hv2.add(v3)
    hv2.add(v4)
    hv3.add(v5)

    hv1.assign(1)
    hv2.assign(2)
    hv3.assign(3)

    hv1.updateNeighbors
    hv2.updateNeighbors
    hv3.updateNeighbors

    hv1.avgNumNeighbors = 2
    hv2.avgNumNeighbors = 2.5
    hv3.avgNumNeighbors = 1
  }

  @Test
  def testNeighbors(): Unit = {
    assertFalse(hv1.neighbors.toSet.contains(hv1))
    assertFalse(hv2.neighbors.toSet.contains(hv2))
    assertFalse(hv3.neighbors.toSet.contains(hv3))
    assertTrue(hv1.neighbors.toSet.contains(hv2))
    assertFalse(hv1.neighbors.toSet.contains(hv3))
    assertTrue(hv2.neighbors.toSet.contains(hv1))
    assertTrue(hv2.neighbors.toSet.contains(hv3))
    assertTrue(hv3.neighbors.toSet.contains(hv2))
    assertFalse(hv3.neighbors.toSet.contains(hv1))
  }

  @Test
  def testUpdateNeighbors: Unit = {
    hv2.remove(v3)
    hv2.updateNeighbors

    assertTrue(hv2.neighbors.isEmpty)
  }

  @Test
  def testAssign(): Unit = {
    hv1.assign(5)
    assertEquals(5, v1.primary)
    assertEquals(5, v2.primary)
    assertEquals(2, mlp.partitions(5).size)
    assertEquals(2, v3.primary)
    assertEquals(2, v4.primary)
    assertEquals(3, v5.primary)

    hv1.assign(-1)
    assertEquals(-1, v1.primary)
    assertEquals(-1, v2.primary)
    assertEquals(0, mlp.partitions(5).size)
  }

  @Test
  def testAdd(): Unit = {
    var v6 = new Vertex(6)
    new Edge(v5, v6)
    hv3.add(v6)

    assertEquals(2, hv3.size)
    assertEquals(3, v6.primary)
    assertEquals(1.5, hv3.avgNumNeighbors, 0.001)
  }

  @Test
  def testRemove(): Unit = {
    hv2.remove(v3)
    assertEquals(-1, v3.primary)
    assertEquals(1, hv2.size)
    assertEquals(1, hv2.avgNumNeighbors, 0.001)
  }

  @Test
  def testMerge(): Unit = {
    hv1.merge(hv2)

    assertEquals(1, v3.primary)
    assertEquals(1, v4.primary)
    assertEquals(4, hv1.size)
    assertEquals(2.25, hv1.avgNumNeighbors, 0.001)
    assertEquals(1, hv1.neighbors.size)
    assertTrue(hv1.neighbors.toSet.contains(hv3))

    mlp.hvs.foreach(f => assertTrue(f._2 != hv2))
    assertTrue(hv1.neighbors.toSet.contains(hv3))
    assertEquals(1, hv3.neighbors.size)
    assertTrue(hv3.neighbors.toSet.contains(hv1))
  }

}

