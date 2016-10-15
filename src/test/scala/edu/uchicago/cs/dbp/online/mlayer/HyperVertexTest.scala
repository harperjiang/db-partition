package edu.uchicago.cs.dbp.online.mlayer

import org.junit.Test
import org.junit.Assert._
import edu.uchicago.cs.dbp.model.Vertex

class HyperVertexTest {

  @Test
  def testAdd = {
    val mlp = new MLayerPartitioner(10);

    var hv = new mlp.HyperVertex(1)

    hv.assign(6)

    var v0 = new Vertex(0)

    var v1 = new Vertex(1)
    mlp.partitions(0).addPrimary(v1)

    hv.add(v0)
    hv.add(v1)

    assertEquals(6, v0.primary)
    assertEquals(6, v1.primary)
    assertEquals(2, mlp.partitions(6).size)
    assertEquals(0, mlp.partitions(0).size)
  }

  @Test
  def testRemove = {
    val mlp = new MLayerPartitioner(10);

    var hv = new mlp.HyperVertex(1)

    hv.assign(6)

    var v0 = new Vertex(0)

    var v1 = new Vertex(1)
    mlp.partitions(0).addPrimary(v1)

    hv.add(v0)
    hv.add(v1)
    
    hv.remove(v0)
    hv.remove(v1)
    
    assertEquals(6,v0.primary)
    assertEquals(6,v1.primary)
  }

  @Test
  def testMerge = {
    val mlp = new MLayerPartitioner(10)
    var hv1 = new mlp.HyperVertex(1)
    var hv2 = new mlp.HyperVertex(2)

    hv1.merge(hv2)
  }
}