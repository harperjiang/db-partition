package edu.uchicago.cs.dbp.linearp

import org.junit.Test
import org.junit.Assert._
class PartitionerTest {

  @Test
  def testGenlset(): Unit = {
    var per = new Partitioner(10);
    var set = per.genlset()
    assertTrue(set.size < 10)
  }
}