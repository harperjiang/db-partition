package edu.uchicago.cs.dbp.online.leopard

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

import edu.uchicago.cs.dbp.model.Edge
import edu.uchicago.cs.dbp.model.Vertex

class LeopardPartitionerTest {

  @Test
  def testAssign(): Unit = {
    var per = new LeopardPartitioner(5);

    var v1 = new Vertex();
    v1.id = 1;

    var v2 = new Vertex();
    v2.id = 2;

    var v3 = new Vertex();
    v3.id = 3;

    var v4 = new Vertex();
    v4.id = 4;

    v1.attach(Array(v2, v3));
    v2.attach(Array(v1, v4));
    v3.attach(Array(v1, v4));
    v4.attach(Array(v2, v3));

    LeopardParams.save();
    LeopardParams.wSize = 5;
    LeopardParams.avgReplica = 1;
    LeopardParams.minReplica = 1;

    assertFalse(per.assign(v1));
    assertFalse(per.assign(v2));
    assertFalse(per.assign(v3));
    assertFalse(per.assign(v4));

    assertEquals(0, v1.primary);
    assertEquals(1, v2.primary);
    assertEquals(2, v3.primary);
    assertEquals(3, v4.primary);

    LeopardParams.load();
  }

  @Test
  def testAdd(): Unit = {
    var per = new LeopardPartitioner(5);

    var v1 = new Vertex();
    v1.id = 1;
    var v2 = new Vertex();
    v2.id = 2;
    var v3 = new Vertex();
    v3.id = 3;
    var v4 = new Vertex();
    v4.id = 4;

    per.add(new Edge(Array(v1, v2)));
    
    assertTrue(v1.neighbors.toList.contains(v2));
    assertTrue(v2.neighbors.toList.contains(v1));
    
    per.add(new Edge(Array(v1, v3)));
    per.add(new Edge(Array(v3, v4)));
  }

}