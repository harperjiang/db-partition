package edu.uchicago.cs.dbp.online.mlayer

import org.junit.Test
import org.junit.Before
import org.junit.Assert._
import edu.uchicago.cs.dbp.model.Vertex
import edu.uchicago.cs.dbp.model.Edge

class MLayerPartitionerTest {

  val mlp = new MLayerPartitioner(10)

  var hv1: mlp.HyperVertex = null
  var hv2: mlp.HyperVertex = null
  var hv3: mlp.HyperVertex = null

  val v1 = new Vertex(1)
  val v2 = new Vertex(2)
  val v3 = new Vertex(3)
  val v4 = new Vertex(4)
  val v5 = new Vertex(5)
  val v6 = new Vertex(6)

  @Before
  def prepare: Unit = {
    // Set params for test
    MLayerParams.instance.neighborThreshold = 5

    hv1 = new mlp.HyperVertex
    hv2 = new mlp.HyperVertex
    hv3 = new mlp.HyperVertex

    new Edge(v1, v2)
    new Edge(v1, v3)
    new Edge(v1, v4)
    new Edge(v1, v5)
    new Edge(v1, v6)

    new Edge(v2, v3)
    new Edge(v4, v5)
    new Edge(v4, v6)

    hv1.add(v1)

    hv2.add(v2)
    hv2.add(v3)

    hv3.add(v4)
    hv3.add(v5)
    hv3.add(v6)

    hv1.updateNeighbors
    hv2.updateNeighbors
    hv3.updateNeighbors

    hv1.assign(0)
    hv2.assign(1)
    hv3.assign(2)

    hv1.avgNumNeighbors = 5
    hv2.avgNumNeighbors = 2
    hv3.avgNumNeighbors = 7d / 3
  }

  @Test
  def testAddEdge1: Unit = {
    val v7 = new Vertex(7)
    mlp.add(new Edge(v3, v7))

    assertEquals(v3.primary, v7.primary)
    assertEquals(hv2, mlp.hvs.get(v7.id).get)

    mlp.add(new Edge(v4, v7))

    assertEquals(v3.primary, v7.primary)
    assertEquals(hv3, mlp.hvs.get(v7.id).get)

    // v4 will be independent
    mlp.add(new Edge(v3, v4))
    mlp.add(new Edge(v4, v6))

    val nhv4 = mlp.hvs.getOrElse(v4.id, null)
    assertEquals(1, nhv4.size)
  }

  @Test
  def testAddEdge2: Unit = {

  }

  @Test
  def testAddEdgeStartFromScratch: Unit = {

  }

  @Test
  def testHyperFetch1: Unit = {
    // Fetch existing vertices
    assertTrue(hv1 == mlp.hyperFetch(v1))
    assertTrue(hv2 == mlp.hyperFetch(v2))
    assertTrue(hv2 == mlp.hyperFetch(v3))
    assertTrue(hv3 == mlp.hyperFetch(v4))
    assertTrue(hv3 == mlp.hyperFetch(v5))
    assertTrue(hv3 == mlp.hyperFetch(v6))

    // New vertex join
    val v7 = new Vertex(7)
    new Edge(v3, v7)
    new Edge(v4, v7)

    var hv7 = mlp.hyperFetch(v7)

    assertEquals(hv2, hv7)
    assertEquals(3, hv7.size)
    assertEquals(2, hv7.neighbors.size)
    assertTrue(hv7.neighbors.toSet.contains(hv1))
    assertTrue(hv7.neighbors.toSet.contains(hv3))
    assertEquals(7d / 3, hv7.avgNumNeighbors, 0.001)

    // Existing vertex has enough edge
    new Edge(v3, v4)
    val v8 = new Vertex(8)
    new Edge(v4, v8)

    val nhv4 = mlp.hyperFetch(v4)

    assertFalse(Array(hv1.id, hv2.id, hv3.id).contains(nhv4.id))
    assertEquals(3, nhv4.neighbors.size)
    assertEquals(3, hv1.neighbors.size)
    assertEquals(3, hv2.neighbors.size)
    assertEquals(2, hv3.neighbors.size)
  }

  @Test
  def testIsSingleOut: Unit = {
    assertTrue(mlp.isSingleOut(hv1))
    assertFalse(mlp.isSingleOut(hv2))
    assertFalse(mlp.isSingleOut(hv3))
  }

  @Test
  def testShouldMerge: Unit = {
    assertFalse(mlp.shouldMerge(hv1, hv2))
    assertFalse(mlp.shouldMerge(hv1, hv3))
    assertTrue(mlp.shouldMerge(hv2, hv3))
  }

  @Test
  def testAssign: Unit = {
    assertTrue(mlp.assign(hv1, true))

    var psize = Array(0, 2, 3, 0, 0, 0, 0, 0, 0, 0)
    var nbsize = Array(0, 2, 3, 0, 0, 0, 0, 0, 0, 0)

    var score = nbsize.zip(psize)
      .zipWithIndex
      .map(e => (mlp.assignScore(e._1._1, e._1._2), e._2))
      .maxBy(_._1)._2
    assertEquals(score, hv1.partition)
  }

  @Test
  def testShouldReassign: Unit = {
    var yes = 0d
    var no = 0d

    for (i <- 0 to 20000) {
      mlp.shouldReassign(hv1) match {
        case true => yes += 1
        case false => no += 1
      }
    }

    assertEquals(1, no / yes, 0.1)
  }

  @Test
  def testHasEnoughNeighbors: Unit = {
    assertTrue(mlp.hasEnoughNeighbors(v1))
    assertFalse(mlp.hasEnoughNeighbors(v2))
    assertFalse(mlp.hasEnoughNeighbors(v3))
    assertFalse(mlp.hasEnoughNeighbors(v4))
    assertFalse(mlp.hasEnoughNeighbors(v5))
    assertFalse(mlp.hasEnoughNeighbors(v6))
  }

  @Test
  def testJoinScore: Unit = {
    val v7 = new Vertex
    new Edge(v3, v7)

    val sa1 = mlp.joinScore(v7, hv1)
    val sa2 = mlp.joinScore(v7, hv2)
    val sa3 = mlp.joinScore(v7, hv3)

    assertEquals(Double.MinValue, sa1, 0.001)
    assertEquals(-2, sa2, 0.001)
    assertEquals(-4, sa3, 0.001)

    new Edge(v4, v7)

    val sb1 = mlp.joinScore(v7, hv1)
    val sb2 = mlp.joinScore(v7, hv2)
    val sb3 = mlp.joinScore(v7, hv3)

    assertEquals(Double.MinValue, sb1, 0.001)
    assertEquals(0, sb2, 0.001)
    assertEquals(-1, sb3, 0.001)

  }
}