package edu.uchicago.cs.dbp.online.mlayer

import org.junit.Test

class HyperVertexTest {

  @Test
  def testAdd = {

  }

  @Test
  def testRemove = {

  }

  @Test
  def testMerge = {
    val mlp = new MLayerPartitioner(10)
    var hv1 = new mlp.HyperVertex(1)
    var hv2 = new mlp.HyperVertex(2)

    hv1.merge(hv2)
  }
}